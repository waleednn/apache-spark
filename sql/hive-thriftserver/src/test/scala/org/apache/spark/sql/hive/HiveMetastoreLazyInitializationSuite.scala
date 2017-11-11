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

package org.apache.spark.sql.hive

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.Utils

class HiveMetastoreLazyInitializationSuite extends SparkFunSuite {

  test("lazily initialize Hive client") {
    val spark = SparkSession.builder()
      .appName("HiveMetastoreLazyInitializationSuite")
      .master("local[2]")
      .enableHiveSupport()
      .config("spark.hadoop.hive.metastore.uris", "thrift://127.0.0.1:11111")
      .getOrCreate()
    val originalLevel = org.apache.log4j.Logger.getRootLogger().getLevel
    try {
      // Avoid outputting a lot of expected warning logs
      spark.sparkContext.setLogLevel("error")

      // We should be able to run Spark jobs without Hive client.
      assert(spark.sparkContext.range(0, 1).count() === 1)

      // Make sure that we are not using the local derby metastore.
      val exceptionString = Utils.exceptionString(intercept[AnalysisException] {
        spark.sql("show tables")
      })
      for (msg <- Seq(
        "show tables",
        "Could not connect to meta store",
        "org.apache.thrift.transport.TTransportException",
        "Connection refused")) {
        exceptionString.contains(msg)
      }
    } finally {
      spark.sparkContext.setLogLevel(originalLevel.toString)
      spark.stop()
    }
  }
}
