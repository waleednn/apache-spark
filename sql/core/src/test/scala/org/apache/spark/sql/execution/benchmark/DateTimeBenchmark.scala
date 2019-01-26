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

import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.datasources.json.JSONBenchmark.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
 * Synthetic benchmark for date and timestamp functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/test:runMain <this class>"
 *      Results will be written to "benchmarks/DateTimeBenchmark-results.txt".
 * }}}
 */
object DateTimeBenchmark extends SqlBasedBenchmark {
  def doBenchmark(cardinality: Int, expr: String): Unit = {
    spark.range(cardinality)
      .selectExpr(expr)
      .write.format("noop").save()
  }

  def run(cardinality: Int, name: String, expr: String): Unit = {
    codegenBenchmark(name, cardinality) {
      doBenchmark(cardinality, expr)
    }
  }

  def run(cardinality: Int, func: String): Unit = {
    codegenBenchmark(s"$func to timestamp", cardinality) {
      doBenchmark(cardinality, s"$func(cast(id as timestamp))")
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 10000000
    runBenchmark("Extract components") {
      run(N, "cast to timestamp", "cast(id as timestamp)")
      run(N, "year")
      run(N, "quarter")
      run(N, "month")
      run(N, "weekofyear")
      run(N, "day")
      run(N, "dayofyear")
      run(N, "dayofmonth")
      run(N, "dayofweek")
      run(N, "weekday")
      run(N, "hour")
      run(N, "minute")
      run(N, "second")
    }
  }
}
