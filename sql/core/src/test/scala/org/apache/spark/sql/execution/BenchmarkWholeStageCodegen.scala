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

package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf, SparkFunSuite}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Benchmark

/**
  * Benchmark to measure whole stage codegen performance.
  * To run this:
  *  build/sbt "sql/test-only *BenchmarkWholeStageCodegen"
  */
class BenchmarkWholeStageCodegen extends SparkFunSuite {
  lazy val conf = new SparkConf().setMaster("local[1]").setAppName("benchmark")
  lazy val sc = SparkContext.getOrCreate(conf)
  lazy val sqlContext = SQLContext.getOrCreate(sc)

  def testRangeFilterAndAggregation(values: Int): Unit = {

    val benchmark = new Benchmark("Single Int Column Scan", values)

    benchmark.addCase("Without whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    benchmark.addCase("With whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").count()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Single Int Column Scan:            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      Without whole stage codegen             6585.36            31.85         1.00 X
      With whole stage codegen                 343.80           609.99        19.15 X
    */
    benchmark.run()
  }

  def testImperitaveAggregation(values: Int): Unit = {

    val benchmark = new Benchmark("Single Int Column Scan", values)

    benchmark.addCase("ImpAgg w/o whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "false")
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().agg("id" -> "stddev").collect()
    }

    benchmark.addCase("ImpAgg w whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().agg("id" -> "stddev").collect()
    }

    benchmark.addCase("DeclAgg w whole stage codegen") { iter =>
      sqlContext.setConf("spark.sql.codegen.wholeStage", "true")
      //
      sqlContext.range(values).filter("(id & 1) = 1").groupBy().agg("id" -> "stddev1").collect()
    }

    /*
      Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
      Single Int Column Scan:            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
      -------------------------------------------------------------------------------
      ImpAgg w/o whole stage codegen          7076.30            14.82         1.00 X
      ImpAgg w whole stage codegen            4027.90            26.03         1.76 X
      DeclAgg w whole stage codegen            786.13           133.38         9.00 X
    */
    benchmark.run()
  }

  ignore("benchmark") {
    testRangeFilterAndAggregation(1024 * 1024 * 200)
    testImperitaveAggregation(1024 * 1024 * 100)
  }
}
