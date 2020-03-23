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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class ParquetFileFormatSuite extends QueryTest with ParquetTest with SharedSparkSession {

  test("read parquet footers in parallel") {
    def testReadFooters(ignoreCorruptFiles: Boolean): Unit = {
      withTempDir { dir =>
        val fs = FileSystem.get(spark.sessionState.newHadoopConf())
        val basePath = dir.getCanonicalPath

        val path1 = new Path(basePath, "first")
        val path2 = new Path(basePath, "second")
        val path3 = new Path(basePath, "third")

        spark.range(1).toDF("a").coalesce(1).write.parquet(path1.toString)
        spark.range(1, 2).toDF("a").coalesce(1).write.parquet(path2.toString)
        spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)

        val fileStatuses =
          Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten

        val footers = ParquetFileFormat.readParquetFootersInParallel(
          spark.sessionState.newHadoopConf(), fileStatuses, ignoreCorruptFiles)

        assert(footers.size == 2)
      }
    }

    testReadFooters(true)
    val exception = intercept[SparkException] {
      testReadFooters(false)
    }.getCause
    assert(exception.getMessage().contains("Could not read footer for file"))
  }

  test("read parquet map invaild key type") {
    withTempDir { dir =>
      val spark_session = spark
      import spark_session.implicits._
      val df = Seq(Map(Map(1 -> 1) -> 1L)).toDF("map")
      val exception = intercept[AnalysisException] {
        df.write.mode("overwrite").parquet(dir.getPath)
      }
      assert(exception.getMessage().contains("Parquet data source does not support"))

      val df2 = Seq(Map(Array(1) -> 1L)).toDF("map")
      val exception2 = intercept[AnalysisException] {
        df2.write.mode("overwrite").parquet(dir.getPath)
      }
      assert(exception2.getMessage().contains("Parquet data source does not support"))
    }
  }
}
