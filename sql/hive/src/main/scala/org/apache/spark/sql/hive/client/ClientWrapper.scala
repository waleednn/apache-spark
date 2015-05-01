/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.client

import java.io.{BufferedReader, InputStreamReader, File, PrintStream}
import java.net.URI
import java.util.{ArrayList => JArrayList}

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.api.Database
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.Driver

import org.apache.spark.Logging
import org.apache.spark.sql.execution.QueryExecutionException


/**
 * A class that wraps the HiveClient and converts its responses to externally visible classes.
 * Note that this class is typically loaded with an internal classloader for each instantiation,
 * allowing it to interact directly with a specific isolated version of Hive.  Loading this class
 * with the isolated classloader however will result in it only being visible as a ClientInterface,
 * not a ClientWrapper.
 *
 * This class needs to interact with multiple versions of Hive, but will always be compiled with
 * the 'native', execution version of Hive.  Therefore, any places where hive breaks compatibility
 * must use reflection after matching on `version`.
 *
 * @param version the version of hive used when pick function calls that are not compatible.
 * @param config  a collection of configuration options that will be added to the hive conf before
 *                opening the hive client.
 */
class ClientWrapper(
    version: HiveVersion,
    config: Map[String, String])
  extends ClientInterface
  with Logging
  with ReflectionMagic {

  private val conf = new HiveConf(classOf[SessionState])
  config.foreach { case (k, v) =>
    logDebug(s"Hive Config: $k=$v")
    conf.set(k, v)
  }

  private def properties = Seq(
    "javax.jdo.option.ConnectionURL",
    "javax.jdo.option.ConnectionDriverName",
    "javax.jdo.option.ConnectionUserName")

  properties.foreach(p => logInfo(s"Hive Configuration: $p = ${conf.get(p)}"))

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  private val outputBuffer = new java.io.OutputStream {
    var pos: Int = 0
    var buffer = new Array[Int](10240)
    def write(i: Int): Unit = {
      buffer(pos) = i
      pos = (pos + 1) % buffer.size
    }

    override def toString: String = {
      val (end, start) = buffer.splitAt(pos)
      val input = new java.io.InputStream {
        val iterator = (start ++ end).iterator

        def read(): Int = if (iterator.hasNext) iterator.next() else -1
      }
      val reader = new BufferedReader(new InputStreamReader(input))
      val stringBuilder = new StringBuilder
      var line = reader.readLine()
      while(line != null) {
        stringBuilder.append(line)
        stringBuilder.append("\n")
        line = reader.readLine()
      }
      stringBuilder.toString()
    }
  }

  val state = {
    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    val ret = try {
      val newState = new SessionState(conf)
      SessionState.start(newState)
      newState.out = new PrintStream(outputBuffer, true, "UTF-8")
      newState.err = new PrintStream(outputBuffer, true, "UTF-8")
      newState
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  private val client = Hive.get(conf)

  private def withClassLoader[A](f: => A): A = synchronized {
    val original = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    Hive.set(client)
    version match {
      case hive.v12 =>
        classOf[SessionState]
          .callStatic[SessionState, SessionState]("start", state)
      case hive.v13 =>
        classOf[SessionState]
          .callStatic[SessionState, SessionState]("setCurrentSessionState", state)
    }
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  def currentDatabase: String = withClassLoader {
    state.getCurrentDatabase
  }

  def createDatabase(tableName: String): Unit = withClassLoader {
    val table = new Table("default", tableName)
    client.createDatabase(
      new Database("default", "", new File("").toURI.toString, new java.util.HashMap), true)
  }

  def getDatabaseOption(name: String): Option[HiveDatabase] = withClassLoader {
    Option(client.getDatabase(name)).map { d =>
      HiveDatabase(
        name = d.getName,
        location = d.getLocationUri)
    }
  }

  def getTableOption(dbName: String, tableName: String): Option[HiveTable] = withClassLoader {
    logDebug(s"Looking up $dbName.$tableName")

    val hiveTable = Option(client.getTable(dbName, tableName, false))
    val converted = hiveTable.map { h =>

      HiveTable(
        name = h.getTableName,
        specifiedDatabase = Option(h.getDbName),
        schema = h.getCols.map(f => HiveColumn(f.getName, f.getType, f.getComment)),
        partitionColumns = h.getPartCols.map(f => HiveColumn(f.getName, f.getType, f.getComment)),
        properties = h.getParameters.toMap,
        serdeProperties = h.getTTable.getSd.getSerdeInfo.getParameters.toMap,
        tableType = ManagedTable, // TODO
        location = version match {
          case hive.v12 => Option(h.call[URI]("getDataLocation")).map(_.toString)
          case hive.v13 => Option(h.call[Path]("getDataLocation")).map(_.toString)
        },
        inputFormat = Option(h.getInputFormatClass).map(_.getName),
        outputFormat = Option(h.getOutputFormatClass).map(_.getName),
        serde = Option(h.getSerializationLib)).withClient(this)
    }
    converted
  }

  private def toInputFormat(name: String) =
    Class.forName(name).asInstanceOf[Class[_ <: org.apache.hadoop.mapred.InputFormat[_, _]]]

  private def toOutputFormat(name: String) =
    Class.forName(name)
      .asInstanceOf[Class[_ <: org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]]]

  private def toQlTable(table: HiveTable): Table = {
    val qlTable = new Table(table.database, table.name)

    qlTable.setFields(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))
    qlTable.setPartCols(
      table.partitionColumns.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))
    table.properties.foreach { case (k, v) => qlTable.setProperty(k, v) }
    table.serdeProperties.foreach { case (k, v) => qlTable.setSerdeParam(k, v) }
    version match {
      case hive.v12 =>
        table.location.map(new URI(_)).foreach(u => qlTable.call[URI, Unit]("setDataLocation", u))
      case hive.v13 =>
        table.location
          .map(new org.apache.hadoop.fs.Path(_))
          .foreach(qlTable.call[Path, Unit]("setDataLocation", _))
    }
    table.inputFormat.map(toInputFormat).foreach(qlTable.setInputFormatClass)
    table.outputFormat.map(toOutputFormat).foreach(qlTable.setOutputFormatClass)
    table.serde.foreach(qlTable.setSerializationLib)

    qlTable
  }

  def createTable(table: HiveTable): Unit = withClassLoader {
    val qlTable = toQlTable(table)
    client.createTable(qlTable)
  }

  def alterTable(table: HiveTable): Unit = withClassLoader {
    val qlTable = toQlTable(table)
    client.alterTable(table.qualifiedName, qlTable)
  }

  def getTables(dbName: String): Seq[String] = withClassLoader {
    client.getAllTables(dbName).toSeq
  }

  def getAllPartitions(hTable: HiveTable): Seq[HivePartition] = withClassLoader {
    val qlTable = toQlTable(hTable)
    val qlPartitions = version match {
      case hive.v12 => client.call[Table, Set[Partition]]("getAllPartitionsForPruner", qlTable)
      case hive.v13 => client.call[Table, Set[Partition]]("getAllPartitionsOf", qlTable)
    }
    qlPartitions.map(_.getTPartition).map { p =>
      HivePartition(
        values = Option(p.getValues).map(_.toSeq).getOrElse(Seq.empty),
        storage = HiveStorageDescriptor(
          location = p.getSd.getLocation,
          inputFormat = p.getSd.getInputFormat,
          outputFormat = p.getSd.getOutputFormat,
          serde = p.getSd.getSerdeInfo.getSerializationLib))
    }.toSeq
  }

  def listTables(dbName: String): Seq[String] = withClassLoader {
    client.getAllTables
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  def runSqlHive(sql: String): Seq[String] = {
    val maxResults = 100000
    val results = runHive(sql, maxResults)
    // It is very confusing when you only get back some of the results...
    if (results.size == maxResults) sys.error("RESULTS POSSIBLY TRUNCATED")
    results
  }

  /**
   * Execute the command using Hive and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = withClassLoader {
    logDebug(s"Running hiveql '$cmd'")
    if (cmd.toLowerCase.startsWith("set")) { logDebug(s"Changing config: $cmd") }
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = version match {
        case hive.v12 =>
          classOf[CommandProcessorFactory]
            .callStatic[String, HiveConf, CommandProcessor]("get", cmd_1, conf)
        case hive.v13 =>
          classOf[CommandProcessorFactory]
            .callStatic[Array[String], HiveConf, CommandProcessor]("get", Array(tokens(0)), conf)
      }

      proc match {
        case driver: Driver =>
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.close()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)

          val results = version match {
            case hive.v12 =>
              val res = new JArrayList[String]
              driver.call[JArrayList[String], Boolean]("getResults", res)
              res.toSeq
            case hive.v13 =>
              val res = new JArrayList[Object]
              driver.call[JArrayList[Object], Boolean]("getResults", res)
              res.map { r =>
                r match {
                  case s: String => s
                  case a: Array[Object] => a(0).asInstanceOf[String]
                }
              }
          }
          driver.close()
          results

        case _ =>
          if (state.out != null) {
            state.out.println(tokens(0) + " " + cmd_1)
          }
          Seq(proc.run(cmd_1).getResponseCode.toString)
      }
    } catch {
      case e: Exception =>
        logError(
          s"""
            |======================
            |HIVE FAILURE OUTPUT
            |======================
            |${outputBuffer.toString}
            |======================
            |END HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        throw e
    }
  }

  def loadPartition(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      holdDDLTime: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean): Unit = withClassLoader {

    client.loadPartition(
      new Path(loadPath), // TODO: Use URI
      tableName,
      partSpec,
      replace,
      holdDDLTime,
      inheritTableSpecs,
      isSkewedStoreAsSubdir)
  }

  def loadTable(
      loadPath: String, // TODO URI
      tableName: String,
      replace: Boolean,
      holdDDLTime: Boolean): Unit = withClassLoader {
    client.loadTable(
      new Path(loadPath),
      tableName,
      replace,
      holdDDLTime)
  }

  def loadDynamicPartitions(
      loadPath: String,
      tableName: String,
      partSpec: java.util.LinkedHashMap[String, String],
      replace: Boolean,
      numDP: Int,
      holdDDLTime: Boolean,
      listBucketingEnabled: Boolean): Unit = withClassLoader {
    client.loadDynamicPartitions(
      new Path(loadPath),
      tableName,
      partSpec,
      replace,
      numDP,
      holdDDLTime,
      listBucketingEnabled)
  }

  def reset(): Unit = withClassLoader {
    client.getAllTables("default").foreach { t =>
        logDebug(s"Deleting table $t")
        val table = client.getTable("default", t)
        client.getIndexes("default", t, 255).foreach { index =>
          client.dropIndex("default", t, index.getIndexName, true)
        }
        if (!table.isIndexTable) {
          client.dropTable("default", t)
        }
      }
      client.getAllDatabases.filterNot(_ == "default").foreach { db =>
        logDebug(s"Dropping Database: $db")
        client.dropDatabase(db, true, false, true)
      }
  }
}
