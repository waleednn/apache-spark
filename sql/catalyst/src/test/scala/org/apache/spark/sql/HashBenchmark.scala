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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Murmur3Hash, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmark for the previous interpreted hash function(InternalRow.hashCode) vs the new codegen
 * hash expression(Murmur3Hash).
 */
object HashBenchmark {

  def test(name: String, schema: StructType, iters: Int): Unit = {
    val numRows = 512

    val generator = RandomDataGenerator.forType(schema, nullable = false).get
    val encoder = RowEncoder(schema)
    val attrs = schema.toAttributes
    val safeProjection = GenerateSafeProjection.generate(attrs, attrs)

    val rows = (1 to numRows).map(_ =>
      // The output of encoder is UnsafeRow, use safeProjection to turn in into safe format.
      safeProjection(encoder.toRow(generator().asInstanceOf[Row])).copy()
    ).toArray

    val benchmark = new Benchmark("Hash For " + name, iters * numRows)
    benchmark.addCase("interpreted version") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += rows(i).hashCode()
          i += 1
        }
      }
    }

    val getHashCode = UnsafeProjection.create(new Murmur3Hash(attrs) :: Nil, attrs)
    benchmark.addCase("codegen version") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += getHashCode(rows(i)).getInt(0)
          i += 1
        }
      }
    }
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val simple = new StructType().add("i", IntegerType)
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For simple:                   Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    interpreted version                      531.53           252.51         1.00 X
    codegen version                         1228.31           109.27         0.43 X
     */
    test("simple", simple, 1024 * 256)

    val normal = new StructType()
      .add("null", NullType)
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("bigDecimal", DecimalType.SYSTEM_DEFAULT)
      .add("smallDecimal", DecimalType.USER_DEFAULT)
      .add("string", StringType)
      .add("binary", BinaryType)
      .add("date", DateType)
      .add("timestamp", TimestampType)
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For normal:                   Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    interpreted version                     2187.63             0.96         1.00 X
    codegen version                         1693.21             1.24         1.29 X
     */
    test("normal", normal, 1024 * 4)

    val arrayOfInt = ArrayType(IntegerType)
    val array = new StructType()
      .add("array", arrayOfInt)
      .add("arrayOfArray", ArrayType(arrayOfInt))
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For array:                    Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    interpreted version                     3290.06             0.08         1.00 X
    codegen version                         6674.07             0.04         0.49 X
     */
    test("array", array, 512)

    val mapOfInt = MapType(IntegerType, IntegerType)
    val map = new StructType()
      .add("map", mapOfInt)
      .add("mapOfMap", MapType(IntegerType, mapOfInt))
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For map:                      Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    -------------------------------------------------------------------------------
    interpreted version                    64709.73             0.00         1.00 X
    codegen version                         8019.04             0.02         8.07 X
     */
    test("map", map, 256)
  }
}
