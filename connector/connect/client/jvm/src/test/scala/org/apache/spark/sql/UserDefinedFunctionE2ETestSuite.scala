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

import java.lang.{Long => JLong}
import java.util.{Iterator => JIterator}
import java.util.Arrays

import scala.collection.JavaConverters._

import org.apache.spark.api.java.function._
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{PrimitiveIntEncoder, PrimitiveLongEncoder}
import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.functions.udf

/**
 * All tests in this class requires client UDF artifacts synced with the server. TODO: It means
 * these tests only works with SBT for now.
 */
class UserDefinedFunctionE2ETestSuite extends RemoteSparkSession {
  test("simple udf") {
    def dummyUdf(x: Int): Int = x + 5
    val myUdf = udf(dummyUdf _)
    val df = spark.range(5).select(myUdf(Column("id")))
    val result = df.collect()
    assert(result.length == 5)
    result.zipWithIndex.foreach { case (v, idx) =>
      assert(v.getInt(0) == idx + 5)
    }
  }

  test("Dataset typed filter") {
    val rows = spark.range(10).filter(n => n % 2 == 0).collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))
  }

  test("Dataset typed filter - java") {
    val rows = spark
      .range(10)
      .filter(new FilterFunction[JLong] {
        override def call(value: JLong): Boolean = value % 2 == 0
      })
      .collectAsList()
    assert(rows == Arrays.asList[Long](0, 2, 4, 6, 8))
  }

  test("Dataset typed map") {
    val rows = spark.range(10).map(n => n / 2)(PrimitiveLongEncoder).collectAsList()
    assert(rows == Arrays.asList[Long](0, 0, 1, 1, 2, 2, 3, 3, 4, 4))
  }

  test("Dataset typed map - java") {
    val rows = spark
      .range(10)
      .map(
        new MapFunction[JLong, Long] {
          def call(value: JLong): Long = value / 2
        },
        PrimitiveLongEncoder)
      .collectAsList()
    assert(rows == Arrays.asList[Long](0, 0, 1, 1, 2, 2, 3, 3, 4, 4))
  }

  test("Dataset typed flat map") {
    val rows = spark
      .range(5)
      .flatMap(n => Iterator(42, 42))(PrimitiveIntEncoder)
      .collectAsList()
    assert(rows.size() == 10)
    rows.forEach(x => assert(x == 42))
  }

  test("Dataset typed flat map - java") {
    val rows = spark
      .range(5)
      .flatMap(
        new FlatMapFunction[JLong, Int] {
          def call(value: JLong): JIterator[Int] = Arrays.asList(42, 42).iterator()
        },
        PrimitiveIntEncoder)
      .collectAsList()
    assert(rows.size() == 10)
    rows.forEach(x => assert(x == 42))
  }

  test("Dataset typed map partition") {
    val df = spark.range(0, 100, 1, 50).repartition(4)
    val result =
      df.mapPartitions(iter => Iterator.single(iter.length))(PrimitiveIntEncoder).collect()
    assert(result.sorted.toSeq === Seq(23, 25, 25, 27))
  }

  test("Dataset typed map partition - java") {
    val df = spark.range(0, 100, 1, 50).repartition(4)
    val result = df
      .mapPartitions(
        new MapPartitionsFunction[JLong, Int] {
          override def call(input: JIterator[JLong]): JIterator[Int] = {
            Arrays.asList(input.asScala.length).iterator()
          }
        },
        PrimitiveIntEncoder)
      .collect()
    assert(result.sorted.toSeq === Seq(23, 25, 25, 27))
  }
}
