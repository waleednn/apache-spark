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

package org.apache.spark.sql.execution.benchmark

import org.apache.spark.sql.internal.SQLConf

/**
 * Synthetic benchmark for nested schema pruning performance for ORC datasource.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/OrcNestedSchemaPruningBenchmark-results.txt".
 * }}}
 */
object OrcNestedSchemaPruningBenchmark extends NestedSchemaPruningBenchmark {
  override val dataSourceName: String = "orc"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark(s"Nested Schema Pruning Benchmark") {
      withSQLConf(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key -> "true",
          SQLConf.USE_V1_SOURCE_READER_LIST.key -> "orc",
          SQLConf.USE_V1_SOURCE_WRITER_LIST.key -> "orc") {
        selectBenchmark (N, numIters)
        limitBenchmark (N, numIters)
        repartitionBenchmark (N, numIters)
        repartitionByExprBenchmark (N, numIters)
        sortBenchmark (N, numIters)
      }
    }
  }
}
