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

package org.apache.spark.sql.hive.thriftserver

import java.io.{BufferedReader, InputStreamReader}

import scala.sys.process._
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.HiveResult.hiveResultString
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.util.Utils



private[hive] case class SparkSQLDriver(context: SQLContext,
                                         hadoopConf: Configuration)
  extends Logging {

  type RowResult = Seq[String]
  type TableResult = Seq[Seq[String]]

  /**
   *
   * @param cmd
   * @return
   */
  def processCmd(cmd: String): Int = {
    val cmd_cleaned = cmd.trim
    cmd_cleaned
      .split("\\s+")
      .toList match {
      case ("quit" | "exit") :: _ =>
        sys.exit(0)
        0
      case "source" :: filepath :: _ =>
        processFile(filepath)
      case s :: _ if s startsWith "!" =>
        processShellCmd(cmd_cleaned.tail)
      case _ =>
        processSQLCmd(cmd_cleaned)
    }
  }

  /**
   *
   * @param command
   * @return
   */
  def run(command: String): Try[Option[TableResult]] = {
    Try {
      context.sparkContext.setJobDescription(command)
      val execution = context.sessionState.executePlan(context.sql(command).logicalPlan)
      val results = SQLExecution.withNewExecutionId(context.sparkSession, execution) {
        hiveResultString(execution.executedPlan).map(_.split("\t").toSeq)
      }

      val schemaValues = execution.analyzed.schema
      if (schemaValues.nonEmpty) {
        Some(schemaValues.map(attr => attr.name) +: results)
      } else {
        None
      }
    }
  }

  /**
   *
   * @param file
   * @return
   */
  def processFile(file: String): Int = {
    val auxPath = new Path(file)
    val fs = if (auxPath.toUri.isAbsolute) {
      FileSystem.get(auxPath.toUri, hadoopConf)
    } else {
      FileSystem.getLocal(hadoopConf)
    }
    val path = if (!auxPath.toUri.isAbsolute) {
      fs.makeQualified(auxPath)
    } else {
      auxPath
    }

    lazy val br = new BufferedReader(new InputStreamReader(fs.open(path)))

    @scala.annotation.tailrec
    def readLines(reader: BufferedReader, outputString: List[String]): List[String] = {
      val line = reader.readLine()
      if (line == null) {
        outputString
      } else {
        readLines(reader, outputString :+ line)
      }
    }

    val resultLines = Try(readLines(br, List[String]()))
    IOUtils.closeStream(br)
    resultLines match {
      case Success(result) => processLines(result)
      case Failure(exception) =>
        logError(exception.getMessage)
        1
    }
  }

  /**
   *
   * @param cmd
   * @return
   */
  def processSQLCmd(cmd: String): Int = {

    run(cmd) match {
      case Success(value) =>
        value match {
          case Some(results) =>
            // scalastyle:off println
            println(showQueryResults(results))
            // scalastyle:on println
          case None =>
        }
        0
      case Failure(exception) =>
        // scalastyle:off println
        println(s"Error in query: ${exception.getMessage}")
        // scalastyle:on println
        1
    }
  }


  /**
   *
   * @param cmd
   * @return
   */
  def processShellCmd(cmd: String): Int = {
    Try(cmd.!!) match {
      case Success(value) =>
        // scalastyle:off println
        println(value)
        // scalastyle:on println
        0
      case Failure(exception) =>
        logError(exception.getMessage)
        1
    }
  }

  /**
   *
   * @param cmd
   * @return
   */
  def processLine(cmd: String): Int = processLines(List[String](cmd))

  /**
   *
   * @param cmd
   * @return
   */
  def processLines(cmd: List[String]): Int = {

    // Avoiding lines starting with --.
    val trimmed: String = cmd
      .filterNot(_.startsWith("--"))
      .map(_.trim)
      .map(_.replace("\\\\", " "))
      .mkString
      .trim

    // Using Regex to select complete sections
    // with (simple and double) quotes.
    val replacementTag = "''"
    val regexPattern = """(["'])(.*?[^\\])\1""".r

    // Finding all groups that match quote pattern.
    val replace = regexPattern
      .findAllIn(trimmed)
      .toList

    // Replacing those groups with a tag,
    // and splitting lines using ;.
    val allin = regexPattern
      .replaceAllIn(trimmed, replacementTag)
      .split(";")
      .toList

    // Tail-recursive function to replace back original content from regex groups.
    @scala.annotation.tailrec
    def pushBack(
                  lines: List[String],
                  replacements: List[String],
                  accumulator: List[String]): List[String] = {

      if (lines.isEmpty) {
        accumulator
      } else {
        if (lines.head.contains(replacementTag) && replacements.nonEmpty) {

          // Avoids auto escaping.
          val avoid_escapes = replacements.head
            .replaceAll("\"", "\\\\\"")
            .replaceAll("\'", "\\\\\'")

          val rep = lines.head.replaceFirst(replacementTag, avoid_escapes)

          if (rep.contains(replacementTag)) {
            pushBack(rep +: lines.tail, replacements.tail, accumulator)
          } else {
            pushBack(lines.tail, replacements.tail, accumulator :+ rep)
          }

        } else {
          pushBack(lines.tail, replacements, accumulator :+ lines.head)
        }
      }
    }

    val commands = pushBack(allin, replace, List[String]())

    @scala.annotation.tailrec
    def runCommands(cmd: List[String], prevResult: Int): Int = {
      if (cmd.isEmpty) {
        prevResult
      } else if (prevResult == 1) {
        prevResult
      } else {
        runCommands(cmd.tail, processCmd(cmd.head))
      }
    }
    runCommands(commands, 0)
  }

  /**
   *
   * @param resultRows
   * @return
   */
  def showQueryResults(resultRows: TableResult): String = {

    val sb = new StringBuilder

    val numCols =
      if (resultRows.nonEmpty) {
        resultRows.head.length
      } else {
        0
      }

    // We set a minimum column width at '3'
    val minimumColWidth = 3
    val colWidths = Array.fill(numCols)(minimumColWidth)

    val rows = resultRows.map {
      x => x ++ Seq.fill(numCols - x.length)("")
    }

    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), Utils.stringHalfWidth(cell))
      }
    }

    val paddedRows: Seq[Seq[String]] = rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        StringUtils.leftPad(cell, colWidths(i) - Utils.stringHalfWidth(cell) + cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)

    sb.toString()
  }

}
