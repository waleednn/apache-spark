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

package org.apache.spark.sql.parquet

import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConversions._

import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.apache.parquet.hadoop.thrift.ParquetThriftOutputFormat

import org.apache.spark.sql.parquet.test.thrift.{Nested, ParquetThriftCompat, Suit}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.{Row, SQLContext}

object ParquetThriftCompatibilitySuite {
  def makeParquetThriftCompat(i: Int): ParquetThriftCompat = {
    def makeComplexColumn(i: Int): JMap[Integer, JList[Nested]] = {
      mapAsJavaMap(Seq.tabulate(3) { n =>
        (i + n: Integer) -> seqAsJavaList(Seq.tabulate(3) { m =>
          new Nested(
            seqAsJavaList(Seq.tabulate(3)(j => i + j + m)),
            s"val_${i + m}")
        })
      }.toMap)
    }

    val value = new ParquetThriftCompat(
      i % 2 == 0,
      i.toByte,
      (i + 1).toShort,
      i + 2,
      i.toLong * 10,
      i.toDouble + 0.2d,
      ByteBuffer.wrap(s"val_$i".getBytes),
      s"val_$i",
      Suit.values()(i % 4),

      seqAsJavaList(Seq.tabulate(3)(n => s"arr_${i + n}")),
      setAsJavaSet(Set(i)),
      mapAsJavaMap(Seq.tabulate(3)(n =>(i + n: Integer) -> s"val_${i + n}").toMap),
      makeComplexColumn(i))

    if (i % 3 == 0) {
      value
    } else {
      value
        .setMaybeBoolColumn(i % 2 == 0)
        .setMaybeByteColumn(i.toByte)
        .setMaybeShortColumn((i + 1).toShort)
        .setMaybeIntColumn(i + 2)
        .setMaybeLongColumn(i.toLong * 10)
        .setMaybeDoubleColumn(i.toDouble + 0.2d)
        .setMaybeBinaryColumn(ByteBuffer.wrap(s"val_$i".getBytes))
        .setMaybeStringColumn(s"val_$i")
        .setMaybeEnumColumn(Suit.values()(i % 4))
    }
  }
}

class ParquetThriftCompatibilitySuite extends ParquetCompatibilityTest {
  import ParquetCompatibilityTest.makeNullable
  import ParquetThriftCompatibilitySuite.makeParquetThriftCompat

  override def sqlContext: SQLContext = TestSQLContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val job = new Job()
    ParquetThriftOutputFormat.setThriftClass(job, classOf[ParquetThriftCompat])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetThriftCompat])

    sqlContext
      .sparkContext
      .parallelize(0 until 10)
      .map(i => (null, makeParquetThriftCompat(i)))
      .coalesce(1)
      .saveAsNewAPIHadoopFile(
        parquetStore.getCanonicalPath,
        classOf[Void],
        classOf[ParquetThriftCompat],
        classOf[ParquetThriftOutputFormat[ParquetThriftCompat]],
        job.getConfiguration)
  }

  test("Read Parquet file generated by parquet-thrift") {
    checkAnswer(sqlContext.read.parquet(parquetStore.getCanonicalPath), (0 until 10).map { i =>
      def nullable[T <: AnyRef] = makeNullable[T](i) _

      Row(
        i % 2 == 0,
        i.toByte,
        (i + 1).toShort,
        i + 2,
        i.toLong * 10,
        i.toDouble + 0.2d,
        // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
        // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
        // Thrift `STRING`s are encoded using UTF-8.
        s"val_$i",
        s"val_$i",
        // Thrift ENUM values are converted to Parquet binaries containing UTF-8 strings
        Suit.values()(i % 4).name(),

        nullable(i % 2 == 0: java.lang.Boolean),
        nullable(i.toByte: java.lang.Byte),
        nullable((i + 1).toShort: java.lang.Short),
        nullable(i + 2: Integer),
        nullable((i * 10).toLong: java.lang.Long),
        nullable(i.toDouble + 0.2d: java.lang.Double),
        nullable(s"val_$i"),
        nullable(s"val_$i"),
        nullable(Suit.values()(i % 4).name()),

        Seq.tabulate(3)(n => s"arr_${i + n}"),
        // Thrift `SET`s are converted to Parquet `LIST`s
        Seq(i),
        Seq.tabulate(3)(n =>(i + n: Integer) -> s"val_${i + n}").toMap,
        Seq.tabulate(3) { n =>
          (i + n) -> Seq.tabulate(3) { m =>
            Row(Seq.tabulate(3)(j => i + j + m), s"val_${i + m}")
          }
        }.toMap)
    })
  }
}
