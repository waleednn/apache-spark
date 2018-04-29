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
package org.apache.spark.sql.execution.datasources.csv

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure CSV read/write performance.
 * To run this:
 *  spark-submit --class <this class> --jars <spark sql test jar>
 */
object CSVBenchmarks {
  val conf = new SparkConf()

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("benchmark-csv-datasource")
    .config(conf)
    .getOrCreate()
  import spark.implicits._

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }


  def perlineBenchmark(rowsNum: Int): Unit = {
    val benchmark = new Benchmark("CSV parsing in the per-line mode", rowsNum)

    withTempPath { path =>
      // scalastyle:off
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on

      spark.sparkContext.range(0, rowsNum, 1)
        .map(_ => "a")
        .toDF("colA")
        .write.csv(path.getAbsolutePath)

      val schema = new StructType().add("colA", StringType)
      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase("Read CSV file with one column", 3) { _ =>
        ds.count()
      }

      /*
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      CSV parsing in the per-line mode:  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ------------------------------------------------------------------------------------------
      Read CSV file with one column      23352 / 23495          4.3         233.5       1.0X
      */
      benchmark.run()
    }
  }

  def multiColumnsBenchmark(rowsNum: Int): Unit = {
    val colsNum = 1000
    val benchmark = new Benchmark(s"Wide rows with $colsNum columns", rowsNum)

    withTempPath { path =>
      // scalastyle:off println
      benchmark.out.println("Preparing data for benchmarking ...")
      // scalastyle:on println

      val fields = for (i <- 0 until colsNum) yield StructField(s"col$i", DoubleType)
      val schema = StructType(fields)
      val values = (0 until colsNum).map(i => s"$i.$i").mkString(",")
      val columnNames = schema.fieldNames

      val rdd = spark.sparkContext.range(0, rowsNum, 1)
        .map(_ => Row.fromSeq((0 until colsNum).map(_ + 0.1)))
      val df = spark.createDataFrame(rdd, schema)
      df.write.option("header", true).csv(path.getAbsolutePath)

      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"Select all columns", 3) { _ =>
        ds.select("*").count()
      }
      benchmark.addCase(s"Select 10 columns", 3) { _ =>
        ds.select($"col0", $"col10", $"col20", $"col30", $"col40",
          $"col50", $"col60", $"col70", $"col80", $"col90").count()
      }
      benchmark.addCase(s"Select one column", 3) { _ =>
        ds.select($"col0").count()
      }

      /*
      Intel(R) Core(TM) i7-7920HQ CPU @ 3.10GHz

      Wide rows with 1000 columns:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
      ---------------------------------------------------------------------------------------------
      Select all columns                       53338 / 53477          0.0       53337.6       1.0X
      Select 10 columns                        52724 / 53132          0.0       52723.6       1.0X
      Select one column                        52277 / 52494          0.0       52276.7       1.0X
      */
      benchmark.run()
    }
  }

  def main(args: Array[String]): Unit = {
    perlineBenchmark(100 * 1000 * 1000)
    multiColumnsBenchmark(rowsNum = 1000 * 1000)
  }
}
