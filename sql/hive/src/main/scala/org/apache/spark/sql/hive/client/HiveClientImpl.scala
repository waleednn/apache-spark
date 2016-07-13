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

import java.io.{File, PrintStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.reflectiveCalls

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType}
import org.apache.hadoop.hive.metastore.api.{Database => HiveDatabase, FieldSchema}
import org.apache.hadoop.hive.metastore.api.{SerDeInfo, StorageDescriptor}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.util.{CircularBuffer, Utils}

/**
 * A class that wraps the HiveClient and converts its responses to externally visible classes.
 * Note that this class is typically loaded with an internal classloader for each instantiation,
 * allowing it to interact directly with a specific isolated version of Hive.  Loading this class
 * with the isolated classloader however will result in it only being visible as a [[HiveClient]],
 * not a [[HiveClientImpl]].
 *
 * This class needs to interact with multiple versions of Hive, but will always be compiled with
 * the 'native', execution version of Hive.  Therefore, any places where hive breaks compatibility
 * must use reflection after matching on `version`.
 *
 * Every HiveClientImpl creates an internal HiveConf object. This object is using the given
 * `hadoopConf` as the base. All options set in the `sparkConf` will be applied to the HiveConf
 * object and overrides any exiting options. Then, options in extraConfig will be applied
 * to the HiveConf object and overrides any existing options.
 *
 * @param version the version of hive used when pick function calls that are not compatible.
 * @param sparkConf all configuration options set in SparkConf.
 * @param hadoopConf the base Configuration object used by the HiveConf created inside
 *                   this HiveClientImpl.
 * @param extraConfig a collection of configuration options that will be added to the
 *                hive conf before opening the hive client.
 * @param initClassLoader the classloader used when creating the `state` field of
 *                        this [[HiveClientImpl]].
 */
private[hive] class HiveClientImpl(
    override val version: HiveVersion,
    sparkConf: SparkConf,
    hadoopConf: Configuration,
    extraConfig: Map[String, String],
    initClassLoader: ClassLoader,
    val clientLoader: IsolatedClientLoader)
  extends HiveClient
  with Logging {

  // Circular buffer to hold what hive prints to STDOUT and ERR.  Only printed when failures occur.
  private val outputBuffer = new CircularBuffer()

  private val shim = version match {
    case hive.v12 => new Shim_v0_12()
    case hive.v13 => new Shim_v0_13()
    case hive.v14 => new Shim_v0_14()
    case hive.v1_0 => new Shim_v1_0()
    case hive.v1_1 => new Shim_v1_1()
    case hive.v1_2 => new Shim_v1_2()
  }

  // Create an internal session state for this HiveClientImpl.
  val state = {
    val original = Thread.currentThread().getContextClassLoader
    // Switch to the initClassLoader.
    Thread.currentThread().setContextClassLoader(initClassLoader)

    // Set up kerberos credentials for UserGroupInformation.loginUser within
    // current class loader
    // Instead of using the spark conf of the current spark context, a new
    // instance of SparkConf is needed for the original value of spark.yarn.keytab
    // and spark.yarn.principal set in SparkSubmit, as yarn.Client resets the
    // keytab configuration for the link name in distributed cache
    if (sparkConf.contains("spark.yarn.principal") && sparkConf.contains("spark.yarn.keytab")) {
      val principalName = sparkConf.get("spark.yarn.principal")
      val keytabFileName = sparkConf.get("spark.yarn.keytab")
      if (!new File(keytabFileName).exists()) {
        throw new SparkException(s"Keytab file: ${keytabFileName}" +
          " specified in spark.yarn.keytab does not exist")
      } else {
        logInfo("Attempting to login to Kerberos" +
          s" using principal: ${principalName} and keytab: ${keytabFileName}")
        UserGroupInformation.loginUserFromKeytab(principalName, keytabFileName)
      }
    }

    def isCliSessionState(state: SessionState): Boolean = {
      var temp: Class[_] = if (state != null) state.getClass else null
      var found = false
      while (temp != null && !found) {
        found = temp.getName == "org.apache.hadoop.hive.cli.CliSessionState"
        temp = temp.getSuperclass
      }
      found
    }

    val ret = try {
      // originState will be created if not exists, will never be null
      val originalState = SessionState.get()
      if (isCliSessionState(originalState)) {
        // In `SparkSQLCLIDriver`, we have already started a `CliSessionState`,
        // which contains information like configurations from command line. Later
        // we call `SparkSQLEnv.init()` there, which would run into this part again.
        // so we should keep `conf` and reuse the existing instance of `CliSessionState`.
        originalState
      } else {
        val hiveConf = new HiveConf(hadoopConf, classOf[SessionState])
        // HiveConf is a Hadoop Configuration, which has a field of classLoader and
        // the initial value will be the current thread's context class loader
        // (i.e. initClassLoader at here).
        // We call initialConf.setClassLoader(initClassLoader) at here to make
        // this action explicit.
        hiveConf.setClassLoader(initClassLoader)
        // First, we set all spark confs to this hiveConf.
        sparkConf.getAll.foreach { case (k, v) =>
          if (k.toLowerCase.contains("password")) {
            logDebug(s"Applying Spark config to Hive Conf: $k=xxx")
          } else {
            logDebug(s"Applying Spark config to Hive Conf: $k=$v")
          }
          hiveConf.set(k, v)
        }
        // Second, we set all entries in config to this hiveConf.
        extraConfig.foreach { case (k, v) =>
          if (k.toLowerCase.contains("password")) {
            logDebug(s"Applying extra config to HiveConf: $k=xxx")
          } else {
            logDebug(s"Applying extra config to HiveConf: $k=$v")
          }
          hiveConf.set(k, v)
        }
        val state = new SessionState(hiveConf)
        if (clientLoader.cachedHive != null) {
          Hive.set(clientLoader.cachedHive.asInstanceOf[Hive])
        }
        SessionState.start(state)
        state.out = new PrintStream(outputBuffer, true, "UTF-8")
        state.err = new PrintStream(outputBuffer, true, "UTF-8")
        state
      }
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  // Log the default warehouse location.
  logInfo(
    s"Warehouse location for Hive client " +
      s"(version ${version.fullVersion}) is ${conf.get("hive.metastore.warehouse.dir")}")

  /** Returns the configuration for the current session. */
  def conf: HiveConf = state.getConf

  override def getConf(key: String, defaultValue: String): String = {
    conf.get(key, defaultValue)
  }

  // We use hive's conf for compatibility.
  private val retryLimit = conf.getIntVar(HiveConf.ConfVars.METASTORETHRIFTFAILURERETRIES)
  private val retryDelayMillis = shim.getMetastoreClientConnectRetryDelayMillis(conf)

  /**
   * Runs `f` with multiple retries in case the hive metastore is temporarily unreachable.
   */
  private def retryLocked[A](f: => A): A = clientLoader.synchronized {
    // Hive sometimes retries internally, so set a deadline to avoid compounding delays.
    val deadline = System.nanoTime + (retryLimit * retryDelayMillis * 1e6).toLong
    var numTries = 0
    var caughtException: Exception = null
    do {
      numTries += 1
      try {
        return f
      } catch {
        case e: Exception if causedByThrift(e) =>
          caughtException = e
          logWarning(
            "HiveClient got thrift exception, destroying client and retrying " +
              s"(${retryLimit - numTries} tries remaining)", e)
          clientLoader.cachedHive = null
          Thread.sleep(retryDelayMillis)
      }
    } while (numTries <= retryLimit && System.nanoTime < deadline)
    if (System.nanoTime > deadline) {
      logWarning("Deadline exceeded")
    }
    throw caughtException
  }

  private def causedByThrift(e: Throwable): Boolean = {
    var target = e
    while (target != null) {
      val msg = target.getMessage()
      if (msg != null && msg.matches("(?s).*(TApplication|TProtocol|TTransport)Exception.*")) {
        return true
      }
      target = target.getCause()
    }
    false
  }

  private def client: Hive = {
    if (clientLoader.cachedHive != null) {
      clientLoader.cachedHive.asInstanceOf[Hive]
    } else {
      val c = Hive.get(conf)
      clientLoader.cachedHive = c
      c
    }
  }

  /**
   * Runs `f` with ThreadLocal session state and classloaders configured for this version of hive.
   */
  def withHiveState[A](f: => A): A = retryLocked {
    val original = Thread.currentThread().getContextClassLoader
    // Set the thread local metastore client to the client associated with this HiveClientImpl.
    Hive.set(client)
    // The classloader in clientLoader could be changed after addJar, always use the latest
    // classloader
    state.getConf.setClassLoader(clientLoader.classLoader)
    // setCurrentSessionState will use the classLoader associated
    // with the HiveConf in `state` to override the context class loader of the current
    // thread.
    shim.setCurrentSessionState(state)
    val ret = try f finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    ret
  }

  def setOut(stream: PrintStream): Unit = withHiveState {
    state.out = stream
  }

  def setInfo(stream: PrintStream): Unit = withHiveState {
    state.info = stream
  }

  def setError(stream: PrintStream): Unit = withHiveState {
    state.err = stream
  }

  override def setCurrentDatabase(databaseName: String): Unit = withHiveState {
    if (getDatabaseOption(databaseName).isDefined) {
      state.setCurrentDatabase(databaseName)
    } else {
      throw new NoSuchDatabaseException(databaseName)
    }
  }

  override def createDatabase(
      database: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = withHiveState {
    client.createDatabase(
      new HiveDatabase(
        database.name,
        database.description,
        database.locationUri,
        database.properties.asJava),
        ignoreIfExists)
  }

  override def dropDatabase(
      name: String,
      ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = withHiveState {
    client.dropDatabase(name, true, ignoreIfNotExists, cascade)
  }

  override def alterDatabase(database: CatalogDatabase): Unit = withHiveState {
    client.alterDatabase(
      database.name,
      new HiveDatabase(
        database.name,
        database.description,
        database.locationUri,
        database.properties.asJava))
  }

  override def getDatabaseOption(name: String): Option[CatalogDatabase] = withHiveState {
    Option(client.getDatabase(name)).map { d =>
      CatalogDatabase(
        name = d.getName,
        description = d.getDescription,
        locationUri = d.getLocationUri,
        properties = d.getParameters.asScala.toMap)
    }
  }

  override def listDatabases(pattern: String): Seq[String] = withHiveState {
    client.getDatabasesByPattern(pattern).asScala
  }

  override def getTableOption(
      dbName: String,
      tableName: String): Option[CatalogTable] = withHiveState {
    logDebug(s"Looking up $dbName.$tableName")
    Option(client.getTable(dbName, tableName, false)).map { h =>
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val partCols = h.getPartCols.asScala.map(fromHiveColumn)
      val schema = h.getCols.asScala.map(fromHiveColumn) ++ partCols

      // Skew spec, storage handler, and bucketing info can't be mapped to CatalogTable (yet)
      val unsupportedFeatures = ArrayBuffer.empty[String]

      if (!h.getSkewedColNames.isEmpty) {
        unsupportedFeatures += "skewed columns"
      }

      if (h.getStorageHandler != null) {
        unsupportedFeatures += "storage handler"
      }

      if (!h.getBucketCols.isEmpty) {
        unsupportedFeatures += "bucketing"
      }

      val properties = h.getParameters.asScala.toMap

      val storage = CatalogStorageFormat(
        locationUri = shim.getDataLocation(h).filterNot { _ =>
          // SPARK-15269: Persisted data source tables always store the location URI as a SerDe
          // property named "path" instead of standard Hive `dataLocation`, because Hive only
          // allows directory paths as location URIs while Spark SQL data source tables also
          // allows file paths. So the standard Hive `dataLocation` is meaningless for Spark SQL
          // data source tables.
          DDLUtils.isDatasourceTable(properties) &&
            h.getTableType == HiveTableType.EXTERNAL_TABLE &&
            // Spark SQL may also save external data source in Hive compatible format when
            // possible, so that these tables can be directly accessed by Hive. For these tables,
            // `dataLocation` is still necessary. Here we also check for input format class
            // because only these Hive compatible tables set this field.
            h.getInputFormatClass == null
        },
        properties = h.getTTable.getSd.getSerdeInfo.getParameters.asScala.toMap
      ).withInputFormat(Option(h.getInputFormatClass).map(_.getName))
        .withOutputFormat(Option(h.getOutputFormatClass).map(_.getName))
        .withSerde(Option(h.getSerializationLib))

      CatalogTable(
        identifier = TableIdentifier(h.getTableName, Option(h.getDbName)),
        tableType = h.getTableType match {
          case HiveTableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
          case HiveTableType.MANAGED_TABLE => CatalogTableType.MANAGED
          case HiveTableType.INDEX_TABLE => CatalogTableType.INDEX
          case HiveTableType.VIRTUAL_VIEW => CatalogTableType.VIEW
        },
        schema = schema,
        provider = Some("hive"),
        partitionColumnNames = partCols.map(_.name),
        sortColumnNames = Seq(), // TODO: populate this
        bucketColumnNames = h.getBucketCols.asScala,
        numBuckets = h.getNumBuckets,
        owner = h.getOwner,
        createTime = h.getTTable.getCreateTime.toLong * 1000,
        lastAccessTime = h.getLastAccessTime.toLong * 1000,
        storage = storage,
        properties = properties,
        viewOriginalText = Option(h.getViewOriginalText),
        viewText = Option(h.getViewExpandedText),
        unsupportedFeatures = unsupportedFeatures)
    }
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = withHiveState {
    client.createTable(toHiveTable(table), ignoreIfExists)
  }

  override def dropTable(
      dbName: String,
      tableName: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withHiveState {
    shim.dropTable(client, dbName, tableName, true, ignoreIfNotExists, purge)
  }

  override def alterTable(tableName: String, table: CatalogTable): Unit = withHiveState {
    val hiveTable = toHiveTable(table)
    // Do not use `table.qualifiedName` here because this may be a rename
    val qualifiedTableName = s"${table.database}.$tableName"
    client.alterTable(qualifiedTableName, hiveTable)
  }

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = withHiveState {
    shim.createPartitions(client, db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = withHiveState {
    // TODO: figure out how to drop multiple partitions in one call
    val hiveTable = client.getTable(db, table, true /* throw exception */)
    // do the check at first and collect all the matching partitions
    val matchingParts =
      specs.flatMap { s =>
        // The provided spec here can be a partial spec, i.e. it will match all partitions
        // whose specs are supersets of this partial spec. E.g. If a table has partitions
        // (b='1', c='1') and (b='1', c='2'), a partial spec of (b='1') will match both.
        val parts = client.getPartitions(hiveTable, s.asJava).asScala
        if (parts.isEmpty && !ignoreIfNotExists) {
          throw new AnalysisException(
            s"No partition is dropped. One partition spec '$s' does not exist in table '$table' " +
            s"database '$db'")
        }
        parts.map(_.getValues)
      }.distinct
    var droppedParts = ArrayBuffer.empty[java.util.List[String]]
    matchingParts.foreach { partition =>
      try {
        val deleteData = true
        shim.dropPartition(client, db, table, partition, deleteData, purge)
      } catch {
        case e: Exception =>
          val remainingParts = matchingParts.toBuffer -- droppedParts
          logError(
            s"""
               |======================
               |Attempt to drop the partition specs in table '$table' database '$db':
               |${specs.mkString("\n")}
               |In this attempt, the following partitions have been dropped successfully:
               |${droppedParts.mkString("\n")}
               |The remaining partitions have not been dropped:
               |${remainingParts.mkString("\n")}
               |======================
             """.stripMargin)
          throw e
      }
      droppedParts += partition
    }
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = withHiveState {
    require(specs.size == newSpecs.size, "number of old and new partition specs differ")
    val catalogTable = getTable(db, table)
    val hiveTable = toHiveTable(catalogTable)
    specs.zip(newSpecs).foreach { case (oldSpec, newSpec) =>
      val hivePart = getPartitionOption(catalogTable, oldSpec)
        .map { p => toHivePartition(p.copy(spec = newSpec), hiveTable) }
        .getOrElse { throw new NoSuchPartitionException(db, table, oldSpec) }
      client.renamePartition(hiveTable, oldSpec.asJava, hivePart)
    }
  }

  override def alterPartitions(
      db: String,
      table: String,
      newParts: Seq[CatalogTablePartition]): Unit = withHiveState {
    val hiveTable = toHiveTable(getTable(db, table))
    client.alterPartitions(table, newParts.map { p => toHivePartition(p, hiveTable) }.asJava)
  }

  override def getPartitionOption(
      table: CatalogTable,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    val hivePartition = client.getPartition(hiveTable, spec.asJava, false)
    Option(hivePartition).map(fromHivePartition)
  }

  /**
   * Returns the partitions for the given table that match the supplied partition spec.
   * If no partition spec is specified, all partitions are returned.
   */
  override def getPartitions(
      table: CatalogTable,
      spec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    spec match {
      case None => shim.getAllPartitions(client, hiveTable).map(fromHivePartition)
      case Some(s) => client.getPartitions(hiveTable, s.asJava).asScala.map(fromHivePartition)
    }
  }

  override def getPartitionsByFilter(
      table: CatalogTable,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = withHiveState {
    val hiveTable = toHiveTable(table)
    shim.getPartitionsByFilter(client, hiveTable, predicates).map(fromHivePartition)
  }

  override def listTables(dbName: String): Seq[String] = withHiveState {
    client.getAllTables(dbName).asScala
  }

  override def listTables(dbName: String, pattern: String): Seq[String] = withHiveState {
    client.getTablesByPattern(dbName, pattern).asScala
  }

  /**
   * Runs the specified SQL query using Hive.
   */
  override def runSqlHive(sql: String): Seq[String] = {
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
  protected def runHive(cmd: String, maxRows: Int = 1000): Seq[String] = withHiveState {
    logDebug(s"Running hiveql '$cmd'")
    if (cmd.toLowerCase.startsWith("set")) { logDebug(s"Changing config: $cmd") }
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      // The remainder of the command.
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc = shim.getCommandProcessor(tokens(0), conf)
      proc match {
        case driver: Driver =>
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.close()
            CommandProcessorFactory.clean(conf)
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)

          val results = shim.getDriverResults(driver)
          driver.close()
          CommandProcessorFactory.clean(conf)
          results

        case _ =>
          if (state.out != null) {
            // scalastyle:off println
            state.out.println(tokens(0) + " " + cmd_1)
            // scalastyle:on println
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
      isSkewedStoreAsSubdir: Boolean): Unit = withHiveState {
    shim.loadPartition(
      client,
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
      holdDDLTime: Boolean): Unit = withHiveState {
    shim.loadTable(
      client,
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
      listBucketingEnabled: Boolean): Unit = withHiveState {
    shim.loadDynamicPartitions(
      client,
      new Path(loadPath),
      tableName,
      partSpec,
      replace,
      numDP,
      holdDDLTime,
      listBucketingEnabled)
  }

  override def createFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    shim.createFunction(client, db, func)
  }

  override def dropFunction(db: String, name: String): Unit = withHiveState {
    shim.dropFunction(client, db, name)
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = withHiveState {
    shim.renameFunction(client, db, oldName, newName)
  }

  override def alterFunction(db: String, func: CatalogFunction): Unit = withHiveState {
    shim.alterFunction(client, db, func)
  }

  override def getFunctionOption(
      db: String, name: String): Option[CatalogFunction] = withHiveState {
    shim.getFunctionOption(client, db, name)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = withHiveState {
    shim.listFunctions(client, db, pattern)
  }

  def addJar(path: String): Unit = {
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    clientLoader.addJar(jarURL)
    runSqlHive(s"ADD JAR $path")
  }

  def newSession(): HiveClientImpl = {
    clientLoader.createClient().asInstanceOf[HiveClientImpl]
  }

  def reset(): Unit = withHiveState {
    client.getAllTables("default").asScala.foreach { t =>
        logDebug(s"Deleting table $t")
        val table = client.getTable("default", t)
        client.getIndexes("default", t, 255).asScala.foreach { index =>
          shim.dropIndex(client, "default", t, index.getIndexName)
        }
        if (!table.isIndexTable) {
          client.dropTable("default", t)
        }
      }
      client.getAllDatabases.asScala.filterNot(_ == "default").foreach { db =>
        logDebug(s"Dropping Database: $db")
        client.dropDatabase(db, true, false, true)
      }
  }


  /* -------------------------------------------------------- *
   |  Helper methods for converting to and from Hive classes  |
   * -------------------------------------------------------- */

  private def toInputFormat(name: String) =
    Utils.classForName(name).asInstanceOf[Class[_ <: org.apache.hadoop.mapred.InputFormat[_, _]]]

  private def toOutputFormat(name: String) =
    Utils.classForName(name)
      .asInstanceOf[Class[_ <: org.apache.hadoop.hive.ql.io.HiveOutputFormat[_, _]]]

  private def toHiveColumn(c: CatalogColumn): FieldSchema = {
    new FieldSchema(c.name, c.dataType, c.comment.orNull)
  }

  private def fromHiveColumn(hc: FieldSchema): CatalogColumn = {
    new CatalogColumn(
      name = hc.getName,
      dataType = hc.getType,
      nullable = true,
      comment = Option(hc.getComment))
  }

  private def toHiveTable(table: CatalogTable): HiveTable = {
    val hiveTable = new HiveTable(table.database, table.identifier.table)
    // For EXTERNAL_TABLE, we also need to set EXTERNAL field in the table properties.
    // Otherwise, Hive metastore will change the table to a MANAGED_TABLE.
    // (metastore/src/java/org/apache/hadoop/hive/metastore/ObjectStore.java#L1095-L1105)
    hiveTable.setTableType(table.tableType match {
      case CatalogTableType.EXTERNAL =>
        hiveTable.setProperty("EXTERNAL", "TRUE")
        HiveTableType.EXTERNAL_TABLE
      case CatalogTableType.MANAGED =>
        HiveTableType.MANAGED_TABLE
      case CatalogTableType.INDEX => HiveTableType.INDEX_TABLE
      case CatalogTableType.VIEW => HiveTableType.VIRTUAL_VIEW
    })
    // Note: In Hive the schema and partition columns must be disjoint sets
    val (partCols, schema) = table.schema.map(toHiveColumn).partition { c =>
      table.partitionColumnNames.contains(c.getName)
    }
    if (table.schema.isEmpty) {
      // This is a hack to preserve existing behavior. Before Spark 2.0, we do not
      // set a default serde here (this was done in Hive), and so if the user provides
      // an empty schema Hive would automatically populate the schema with a single
      // field "col". However, after SPARK-14388, we set the default serde to
      // LazySimpleSerde so this implicit behavior no longer happens. Therefore,
      // we need to do it in Spark ourselves.
      hiveTable.setFields(
        Seq(new FieldSchema("col", "array<string>", "from deserializer")).asJava)
    } else {
      hiveTable.setFields(schema.asJava)
    }
    hiveTable.setPartCols(partCols.asJava)
    // TODO: set sort columns here too
    hiveTable.setBucketCols(table.bucketColumnNames.asJava)
    hiveTable.setOwner(conf.getUser)
    hiveTable.setNumBuckets(table.numBuckets)
    hiveTable.setCreateTime((table.createTime / 1000).toInt)
    hiveTable.setLastAccessTime((table.lastAccessTime / 1000).toInt)
    table.storage.locationUri.foreach { loc => shim.setDataLocation(hiveTable, loc) }
    table.storage.getInputFormat.map(toInputFormat).foreach(hiveTable.setInputFormatClass)
    table.storage.getOutputFormat.map(toOutputFormat).foreach(hiveTable.setOutputFormatClass)
    hiveTable.setSerializationLib(
      table.storage.getSerde.getOrElse("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    table.storage.getProperties.foreach { case (k, v) => hiveTable.setSerdeParam(k, v) }
    table.properties.foreach { case (k, v) => hiveTable.setProperty(k, v) }
    table.comment.foreach { c => hiveTable.setProperty("comment", c) }
    table.viewOriginalText.foreach { t => hiveTable.setViewOriginalText(t) }
    table.viewText.foreach { t => hiveTable.setViewExpandedText(t) }
    hiveTable
  }

  private def toHivePartition(
      p: CatalogTablePartition,
      ht: HiveTable): HivePartition = {
    val tpart = new org.apache.hadoop.hive.metastore.api.Partition
    val partValues = ht.getPartCols.asScala.map { hc =>
      p.spec.get(hc.getName).getOrElse {
        throw new IllegalArgumentException(
          s"Partition spec is missing a value for column '${hc.getName}': ${p.spec}")
      }
    }
    val storageDesc = new StorageDescriptor
    val serdeInfo = new SerDeInfo
    p.storage.locationUri.foreach(storageDesc.setLocation)
    p.storage.getInputFormat.foreach(storageDesc.setInputFormat)
    p.storage.getOutputFormat.foreach(storageDesc.setOutputFormat)
    p.storage.getSerde.foreach(serdeInfo.setSerializationLib)
    serdeInfo.setParameters(p.storage.getProperties.asJava)
    storageDesc.setSerdeInfo(serdeInfo)
    tpart.setDbName(ht.getDbName)
    tpart.setTableName(ht.getTableName)
    tpart.setValues(partValues.asJava)
    tpart.setSd(storageDesc)
    new HivePartition(ht, tpart)
  }

  private def fromHivePartition(hp: HivePartition): CatalogTablePartition = {
    val apiPartition = hp.getTPartition

    val storage = CatalogStorageFormat(
      locationUri = Option(apiPartition.getSd.getLocation),
      properties = apiPartition.getSd.getSerdeInfo.getParameters.asScala.toMap
    ).withInputFormat(Option(apiPartition.getSd.getInputFormat))
      .withOutputFormat(Option(apiPartition.getSd.getOutputFormat))
      .withSerde(Option(apiPartition.getSd.getSerdeInfo.getSerializationLib))

    CatalogTablePartition(
      spec = Option(hp.getSpec).map(_.asScala.toMap).getOrElse(Map.empty),
      storage = storage)
  }
}
