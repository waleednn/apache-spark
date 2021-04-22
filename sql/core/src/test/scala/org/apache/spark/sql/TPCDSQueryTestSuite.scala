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

package org.apache.spark.sql

import java.io.File
import java.nio.file.{Files, Paths}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.util.{fileToString, resourceToString, stringToFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession

/**
 * End-to-end tests to check TPCDS query results.
 *
 * To run this test suite:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data> build/sbt "sql/testOnly *TPCDSQueryTestSuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *   SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "~sql/testOnly *TPCDSQueryTestSuite -- -z q79"
 * }}}
 *
 * To re-generate golden files for this suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSQueryTestSuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 SPARK_TPCDS_DATA=<path of TPCDS SF=1 data>
 *     build/sbt "sql/testOnly *TPCDSQueryTestSuite -- -z q79"
 * }}}
 */
class TPCDSQueryTestSuite extends QueryTest with TPCDSBase with SQLQueryTestHelper {

  private val tpcdsDataPath = sys.env.get("SPARK_TPCDS_DATA")
  private val regenerateGoldenFiles = sys.env.get("SPARK_GENERATE_GOLDEN_FILES").exists(_ == "1")

  // To make output results deterministic
  protected override def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.SHUFFLE_PARTITIONS.key, "1")

  protected override def createSparkSession: TestSparkSession = {
    new TestSparkSession(new SparkContext("local[1]", this.getClass.getSimpleName, sparkConf))
  }

  // We use SF=1 table data here, so we cannot use SF=100 stats
  protected override val injectStats: Boolean = false

  if (tpcdsDataPath.nonEmpty) {
    val nonExistentTables = tableNames.filterNot { tableName =>
      Files.exists(Paths.get(s"${tpcdsDataPath.get}/$tableName"))
    }
    if (nonExistentTables.nonEmpty) {
      fail(s"Non-existent TPCDS table paths found in ${tpcdsDataPath.get}: " +
        nonExistentTables.mkString(", "))
    }
  }

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    java.nio.file.Paths.get("src", "test", "resources", "tpcds-query-results")
      .toFile.getAbsolutePath
  }

  override def createTable(
      spark: SparkSession,
      tableName: String,
      format: String = "parquet",
      options: Seq[String] = Nil): Unit = {
    spark.sql(
      s"""
         |CREATE TABLE `$tableName` (${tableColumns(tableName)})
         |USING $format
         |LOCATION '${tpcdsDataPath.get}/$tableName'
         |${options.mkString("\n")}
       """.stripMargin)
  }

  private def runQuery(query: String, goldenFile: File): Unit = {
    val (schema, output) = handleExceptions(getNormalizedResult(spark, query))
    val queryString = query.trim
    val outputString = output.mkString("\n").replaceAll("\\s+$", "")
    if (regenerateGoldenFiles) {
      val goldenOutput = {
        s"-- Automatically generated by ${getClass.getSimpleName}\n\n" +
          s"-- !query schema\n" +
          schema + "\n" +
          s"-- !query output\n" +
          outputString +
          "\n"
      }
      val parent = goldenFile.getParentFile
      if (!parent.exists()) {
        assert(parent.mkdirs(), "Could not create directory: " + parent)
      }
      stringToFile(goldenFile, goldenOutput)
    }

    // Read back the golden file.
    val (expectedSchema, expectedOutput) = {
      val goldenOutput = fileToString(goldenFile)
      val segments = goldenOutput.split("-- !query.*\n")

      // query has 3 segments, plus the header
      assert(segments.size == 3,
        s"Expected 3 blocks in result file but got ${segments.size}. " +
          "Try regenerate the result files.")

      (segments(1).trim, segments(2).replaceAll("\\s+$", ""))
    }

    assertResult(expectedSchema, s"Schema did not match\n$queryString") { schema }
    assertResult(expectedOutput, s"Result did not match\n$queryString") { outputString }
  }

  if (tpcdsDataPath.nonEmpty) {
    tpcdsQueries.foreach { name =>
      val queryString = resourceToString(s"tpcds/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(name) {
        val goldenFile = new File(s"$baseResourcePath/v1_4", s"$name.sql.out")
        runQuery(queryString, goldenFile)
      }
    }

    tpcdsQueriesV2_7_0.foreach { name =>
      val queryString = resourceToString(s"tpcds-v2.7.0/$name.sql",
        classLoader = Thread.currentThread().getContextClassLoader)
      test(s"$name-v2.7") {
        val goldenFile = new File(s"$baseResourcePath/v2_7", s"$name.sql.out")
        runQuery(queryString, goldenFile)
      }
    }
  } else {
    ignore("skipped because env `SPARK_TPCDS_DATA` is not set") {}
  }
}
