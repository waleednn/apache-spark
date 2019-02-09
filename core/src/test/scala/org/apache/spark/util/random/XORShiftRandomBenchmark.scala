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

package org.apache.spark.util.random

import java.util.{Random => JavaRandom}

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.util.Utils.times

/**
 * Benchmarks for pseudo random generators
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>
 *   2. build/sbt "core/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "core/test:runMain <this class>"
 *      Results will be written to "benchmarks/XORShiftRandomBenchmark-results.txt".
 * }}}
 */
object XORShiftRandomBenchmark extends BenchmarkBase {
  val seed = 123456789101112L
  val javaRand = new JavaRandom(seed)
  val xorRand = new XORShiftRandom(seed)

  def nextInt(numIters: Int, valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark("nextInt", valuesPerIteration, output = output)

    benchmark.addCase("java.util.Random", numIters) { _ =>
      times(valuesPerIteration) { javaRand.nextInt() }
    }

    benchmark.addCase("XORShiftRandom", numIters) { _ =>
      times(valuesPerIteration) { xorRand.nextInt() }
    }

    benchmark.run()
  }

  def nextLong(numIters: Int, valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark("nextLong", valuesPerIteration, output = output)

    benchmark.addCase("java.util.Random", numIters) { _ =>
      times(valuesPerIteration) { javaRand.nextLong() }
    }

    benchmark.addCase("XORShiftRandom", numIters) { _ =>
      times(valuesPerIteration) { xorRand.nextLong() }
    }

    benchmark.run()
  }

  def nextDouble(numIters: Int, valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark("nextDouble", valuesPerIteration, output = output)

    benchmark.addCase("java.util.Random", numIters) { _ =>
      times(valuesPerIteration) { javaRand.nextDouble() }
    }

    benchmark.addCase("XORShiftRandom", numIters) { _ =>
      times(valuesPerIteration) { xorRand.nextDouble() }
    }

    benchmark.run()
  }

  def nextGaussian(numIters: Int, valuesPerIteration: Int): Unit = {
    val benchmark = new Benchmark("nextGaussian", valuesPerIteration, output = output)

    benchmark.addCase("java.util.Random", numIters) { _ =>
      times(valuesPerIteration) { javaRand.nextGaussian() }
    }

    benchmark.addCase("XORShiftRandom", numIters) { _ =>
      times(valuesPerIteration) { xorRand.nextGaussian() }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    val valuesPerIteration = 100000000
    runBenchmark("Pseudo random") {
      nextInt(numIters, valuesPerIteration)
      nextLong(numIters, valuesPerIteration)
      nextDouble(numIters, valuesPerIteration)
      nextGaussian(numIters, valuesPerIteration)
    }
  }
}

