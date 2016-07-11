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

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Benchmark

/**
 * Benchmark [[PrimitiveArray]] for DataFrame and Dataset program using primitive array
 * To run this:
 *  1. replace ignore(...) with test(...)
<<<<<<< 1daa9216fe2a928d9c0b6153b12b043fac3c4fa0
 *  2. build/sbt "sql/test-only *benchmark.PrimitiveArrayBenchmark"
=======
 *  2. build/sbt "sql/test-only *benchmark.PrimitiveArrayDataBenchmark"
>>>>>>> update
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class PrimitiveArrayBenchmark extends BenchmarkBase {
<<<<<<< 1daa9216fe2a928d9c0b6153b12b043fac3c4fa0

  def writeDatasetArray(iters: Int): Unit = {
    import sparkSession.implicits._

    val count = 1024 * 1024 * 2
=======

  def showArray(iters: Int): Unit = {
    import sparkSession.implicits._

    val count = 1024 * 1024 * 24
>>>>>>> update

    val sc = sparkSession.sparkContext
    val primitiveIntArray = Array.fill[Int](count)(65535)
    val dsInt = sc.parallelize(Seq(primitiveIntArray), 1).toDS
    dsInt.count  // force to build dataset
    val intArray = { i: Int =>
      var n = 0
      var len = 0
      while (n < iters) {
        len += dsInt.map(e => e).queryExecution.toRdd.collect.length
        n += 1
      }
    }
    val primitiveDoubleArray = Array.fill[Double](count)(65535.0)
    val dsDouble = sc.parallelize(Seq(primitiveDoubleArray), 1).toDS
    dsDouble.count  // force to build dataset
    val doubleArray = { i: Int =>
      var n = 0
      var len = 0
      while (n < iters) {
        len += dsDouble.map(e => e).queryExecution.toRdd.collect.length
        n += 1
      }
    }

    val benchmark = new Benchmark("Write an array in Dataset", count * iters)
    benchmark.addCase("Int   ")(intArray)
    benchmark.addCase("Double")(doubleArray)
    benchmark.run
    /*
<<<<<<< 1daa9216fe2a928d9c0b6153b12b043fac3c4fa0
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Write an array in Dataset:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            352 /  401         23.8          42.0       1.0X
    Double                                         821 /  885         10.2          97.9       0.4X
    */
  }

  ignore("Write an array in Dataset") {
    writeDatasetArray(4)
=======
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.0.4-301.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read an array in DataFrame:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Int                                            502 /  530         50.1          20.0       1.0X
    Double                                        1111 / 1170         22.7          44.1       0.5X
    */
  }

  ignore("Read an array in DataFrame") {
    showArray(1)
>>>>>>> update
  }

  def writeArray(iters: Int): Unit = {
    import sparkSession.implicits._

    val iters = 5
    val n = 1024 * 1024
    val rows = 15

    val benchmark = new Benchmark("Read primitive array", n)

    val intDF = sparkSession.sparkContext.parallelize(0 until rows, 1)
      .map(i => Array.tabulate(n)(i => i)).toDF()
    intDF.count() // force to create df

    benchmark.addCase(s"Write int array in DataFrame", numIters = iters)(iter => {
      intDF.selectExpr("value as a").collect
    })

    val doubleDF = sparkSession.sparkContext.parallelize(0 until rows, 1)
      .map(i => Array.tabulate(n)(i => i.toDouble)).toDF()
    doubleDF.count() // force to create df

    benchmark.addCase(s"Write double array in DataFrame", numIters = iters)(iter => {
      doubleDF.selectExpr("value as a").collect
    })

    benchmark.run()
    /*
    OpenJDK 64-Bit Server VM 1.8.0_91-b14 on Linux 4.4.11-200.fc22.x86_64
    Intel Xeon E3-12xx v2 (Ivy Bridge)
    Read primitive array:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    Write int array in DataFrame                  1290 / 1748          0.8        1230.1       1.0X
    Write double array in DataFrame               1761 / 2236          0.6        1679.0       0.7X
    */
  }

  ignore("Write an array in DataFrame") {
    writeArray(1)
  }
}
