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

import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging

/**
 * This class will check arguments used in command line.
 * It mimics command line options from Hive 2.3
 *
 * @param args command line args.
 */
private[hive] case class SparkSQLCLIArguments(args: Array[String]) extends Logging {


  /**
   * No upper/lower case transformations. Exact values are required.
   * Options without arguments.
   */
  val optionsMap = Map(
    'H' -> "help",
    'V' -> "verbose",
    'S' -> "silent"
  )


  /**
   * No upper/lower case transformations. Exact values are required.
   * Options with arguments.
   */
  val optionsArgMap = Map(
    "--conf" -> "conf",
    "--database" -> "database",
    "--define" -> "define", "-d" -> "define",
    "-e" -> "quoted-query-string",
    "--file" -> "file", "-f" -> "file",
    "--hiveconf" -> "hiveconf",
    "--hivevar" -> "hivevar",
    "-i" -> "init-file"
  )

  /**
   * Usage string helper to show options for --help and  illegal arguments.
   */
  lazy val usage: String = {
    s"""
      |Usage:
      |${optionsArgMap.map(pair => s"${pair._1} [ARG]\t${pair._2}").mkString("\n")}
      |
      |Options:
      |${optionsMap.map(pair => s"-${pair._1}|--${pair._2}\t${pair._2}").mkString("\n")}
      |""".stripMargin
  }

  lazy val parsed: Map[String, Seq[String]] = {
    /**
     *
     * @param innerList List of arguments, will be tail recursive.
     * @param accumulator List of resulting Map of arguments found in innerList
     * @param prevKey Previous key for next element in list to be its value.
     *                (--conf(prevKey) "spark.conf=setting"(value))
     * @return accumulator. (Tail-recursive)
     */
    @scala.annotation.tailrec
    def argHelper(
        innerList: List[String],
        accumulator: Map[String, Seq[String]],
        prevKey: Option[String]): Map[String, Seq[String]] = {

      if (innerList.isEmpty) {
        // End of args list.
        accumulator
      } else {
        if (prevKey.isEmpty) {

          // If current arg is not a key and also
          // there is no previous key, should throw exception.
          if (!innerList.head.startsWith("-")) {
            throw new IllegalArgumentException(
              s"Unknown parameter: ${innerList.head} \n" + usage)
          }

          // Matching argument with exact valid keys.
          val optionArg: Option[String] = optionsArgMap.get(innerList.head)
          val optionNonArg: Boolean = optionsMap.values.toSeq
            .contains(innerList.head.filter(_ != '-'))

          if (optionArg.nonEmpty || optionNonArg) {
            // Looking for keys.
            if (optionNonArg) {
              // Matching option. Will add value to the accumulator.
              argHelper(
                innerList.tail,
                accumulator + (innerList.head.filter(_ != '-') -> Nil),
                None)
            } else {
              // Will check with key with argument.
              argHelper(innerList.tail, accumulator, optionArg)
            }

          } else {
            // Checking only if key is option only.
            if (!innerList.head.toSet.subsetOf(optionsMap.keySet + '-')) {
              throw new IllegalArgumentException(
                s"Unknown parameter: ${innerList.head} \n" + usage)
            }

            val newAccumulator = innerList.head.toSet
              .filter(_ != '-').map(optionsMap(_) -> Nil)
            argHelper(innerList.tail, accumulator ++ newAccumulator, None)
          }
        } else {
          // Checking for option with args.
          if (prevKey.get.startsWith("-")) {
            throw new IllegalArgumentException(
              s"Expecting value for: $prevKey, received: ${innerList.head}"
            )
          }

          val newAccumulator = accumulator.getOrElse(prevKey.get, Seq()) ++ Seq(innerList.head)
          argHelper(innerList.tail, accumulator + (prevKey.get -> newAccumulator), None)
        }
      }
    }

    argHelper(args.toList, Map(), None)
  }


  /**
   * Sanity check for used arguments.
   * Will show usage and exit when fails to parse.
   */
  def parse(): Unit = {
    Try(parsed) match {
      case Failure(value) =>
        logError(value.getMessage)
        showHelp()
        sys.exit(1)
      case Success(value) =>
        logDebug(value.mkString)
        showHelp()
    }
  }

  /**
   * Collects hiveconf values.
   */
  lazy val getHiveConfigs: Seq[(String, String)] = {
    parsed
      .getOrElse("hiveconf", Nil)
      .map { x =>
        Try {
          val auxConfigs = x.split("=")
          auxConfigs(0) -> auxConfigs(1)
        }
      }
      .filter(_.isSuccess)
      .map(x => x.get)
  }

  /**
   * Collect a hiveconf value with a key.
   * @param key hiveconf key to collect
   * @return collected value if exists.
   */
  def getHiveConf(key: String): Option[String] = {
    parsed
      .getOrElse("hiveconf", Nil)
      .filter(_.split("=")(0) == key)
      .map(_.split("=")(1))
      .headOption
  }

  /**
   * Collects spark configs values.
   */
  lazy val getSparkConfigs: Seq[(String, String)] = {
    parsed
      .getOrElse("conf", Nil)
      .map { x =>
        Try {
          val auxConfigs = x.split("=")
          auxConfigs(0) -> auxConfigs(1)
        }
      }
      .filter(_.isSuccess)
      .map(x => x.get)
  }

  /**
   * Provides selected database from arguments.
   * @return selected database from arguments if any.
   */
  def getDatabase: Option[String] = {
    parsed.getOrElse("database", Nil).headOption
  }

  /**
   * Provides selected init file from arguments.
   * @return selected init file from arguments if any.
   */
  def getInitFile: Option[String] = {
    parsed.getOrElse("init-file", Nil).headOption
  }

  /**
   * Provides selected file from arguments.
   * @return selected file from arguments if any.
   */
  def getFile: Option[String] = {
    parsed.getOrElse("file", Nil).headOption
  }

  /**
   * Provides quoted query from arguments.
   * @return quoted query from arguments if any.
   */
  def getQueryString: Option[String] = {
    parsed.getOrElse("quoted-query-string", Nil).headOption
  }

  /**
   *
   * @return
   */
  def isSilent: Boolean = {
    parsed.get("silent").nonEmpty
  }

  /**
   *
   * @return
   */
  def isVerbose: Boolean = {
    parsed.get("verbose").nonEmpty
  }

  /**
   * If help option is used, will print usage and exit.
   */
  def showHelp(): Unit = {
    if (parsed.get("help").nonEmpty) {
      // scalastyle:off println
//      println(usage)
      // scalastyle:on println
      SparkSQLEnv.printStream(usage)
      sys.exit(0)
    }
  }

  // Enforces arguments sanity, when this class is created.
  parse()
}
