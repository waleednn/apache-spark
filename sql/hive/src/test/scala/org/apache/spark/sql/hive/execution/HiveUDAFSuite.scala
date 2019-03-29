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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.ql.udf.UDAFPercentile
import org.apache.hadoop.hive.ql.udf.generic.{AbstractGenericUDAFResolver, GenericUDAFEvaluator, GenericUDAFMax}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.{AggregationBuffer, Mode}
import org.apache.hadoop.hive.ql.util.JavaDataModel
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import test.org.apache.spark.sql.MyDoubleAvg

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.aggregate.ObjectHashAggregateExec
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

class HiveUDAFSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql(s"CREATE TEMPORARY FUNCTION mock AS '${classOf[MockUDAF].getName}'")
    sql(s"CREATE TEMPORARY FUNCTION hive_max AS '${classOf[GenericUDAFMax].getName}'")
    sql(s"CREATE TEMPORARY FUNCTION mock2 AS '${classOf[MockUDAF2].getName}'")

    Seq(
      (0: Integer) -> "val_0",
      (1: Integer) -> "val_1",
      (2: Integer) -> null,
      (3: Integer) -> null
    ).toDF("key", "value").repartition(2).createOrReplaceTempView("t")
    Seq(
      (0: Integer) -> "val_0",
      (1: Integer) -> "val_1",
      (2: Integer) -> "val_2",
      (3: Integer) -> "val_3",
      (4: Integer) -> "val_4",
      (5: Integer) -> "val_5",
      (6: Integer) -> null,
      (7: Integer) -> null
    ).toDF("key", "value").repartition(2).createOrReplaceTempView("t2")
  }

  protected override def afterAll(): Unit = {
    try {
      sql(s"DROP TEMPORARY FUNCTION IF EXISTS mock")
      sql(s"DROP TEMPORARY FUNCTION IF EXISTS hive_max")
    } finally {
      super.afterAll()
    }
  }

  test("built-in Hive UDAF") {
    val df = sql("SELECT key % 2, hive_max(key) FROM t GROUP BY key % 2")

    val aggs = df.queryExecution.executedPlan.collect {
      case agg: ObjectHashAggregateExec => agg
    }

    // There should be two aggregate operators, one for partial aggregation, and the other for
    // global aggregation.
    assert(aggs.length == 2)

    checkAnswer(df, Seq(
      Row(0, 2),
      Row(1, 3)
    ))
  }

  test("customized Hive UDAF") {
    val df = sql("SELECT key % 2, mock(value) FROM t GROUP BY key % 2")

    val aggs = df.queryExecution.executedPlan.collect {
      case agg: ObjectHashAggregateExec => agg
    }

    // There should be two aggregate operators, one for partial aggregation, and the other for
    // global aggregation.
    assert(aggs.length == 2)

    checkAnswer(df, Seq(
      Row(0, Row(1, 1)),
      Row(1, Row(1, 1))
    ))
  }

  test("customized Hive UDAF with two aggregation buffers") {
    val df = sql("SELECT key % 2, mock2(value) FROM t GROUP BY key % 2")

    val aggs = df.queryExecution.executedPlan.collect {
      case agg: ObjectHashAggregateExec => agg
    }

    // There should be two aggregate operators, one for partial aggregation, and the other for
    // global aggregation.
    assert(aggs.length == 2)

    checkAnswer(df, Seq(
      Row(0, Row(1, 1)),
      Row(1, Row(1, 1))
    ))
  }

  test("SPARK-27207: customized Hive UDAF with two aggregation buffers for Sort" +
      " Based Aggregation") {
    withSQLConf("spark.sql.objectHashAggregate.sortBased.fallbackThreshold" -> "2") {
      val df = sql("SELECT key % 2, mock2(value) FROM t2 GROUP BY key % 2")

      val aggs = df.queryExecution.executedPlan.collect {
        case agg: ObjectHashAggregateExec => agg
      }

      // There should be two aggregate operators, one for partial aggregation, and the other for
      // global aggregation.
      assert(aggs.length == 2)

      checkAnswer(df, Seq(
        Row(0, Row(3, 1)),
        Row(1, Row(3, 1))
      ))
    }
  }

  test("call JAVA UDAF") {
    withTempView("temp") {
      withUserDefinedFunction("myDoubleAvg" -> false) {
        spark.range(1, 10).toDF("value").createOrReplaceTempView("temp")
        sql(s"CREATE FUNCTION myDoubleAvg AS '${classOf[MyDoubleAvg].getName}'")
        checkAnswer(
          spark.sql("SELECT default.myDoubleAvg(value) as my_avg from temp"),
          Row(105.0))
      }
    }
  }

  test("non-deterministic children expressions of UDAF") {
    withTempView("view1") {
      spark.range(1).selectExpr("id as x", "id as y").createTempView("view1")
      withUserDefinedFunction("testUDAFPercentile" -> true) {
        // non-deterministic children of Hive UDAF
        sql(s"CREATE TEMPORARY FUNCTION testUDAFPercentile AS '${classOf[UDAFPercentile].getName}'")
        val e1 = intercept[AnalysisException] {
          sql("SELECT testUDAFPercentile(x, rand()) from view1 group by y")
        }.getMessage
        assert(Seq("nondeterministic expression",
          "should not appear in the arguments of an aggregate function").forall(e1.contains))
      }
    }
  }
}

/**
 * A testing Hive UDAF that computes the counts of both non-null values and nulls of a given column.
 */
class MockUDAF extends AbstractGenericUDAFResolver {
  override def getEvaluator(info: Array[TypeInfo]): GenericUDAFEvaluator = new MockUDAFEvaluator
}

class MockUDAF2 extends AbstractGenericUDAFResolver {
  override def getEvaluator(info: Array[TypeInfo]): GenericUDAFEvaluator = new MockUDAFEvaluator2
}

class MockUDAFBuffer(var nonNullCount: Long, var nullCount: Long)
  extends GenericUDAFEvaluator.AbstractAggregationBuffer {

  override def estimate(): Int = JavaDataModel.PRIMITIVES2 * 2
}

class MockUDAFBuffer2(var nonNullCount: Long, var nullCount: Long)
  extends GenericUDAFEvaluator.AbstractAggregationBuffer {

  override def estimate(): Int = JavaDataModel.PRIMITIVES2 * 2
}

class MockUDAFEvaluator extends GenericUDAFEvaluator {
  private val nonNullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector

  private val nullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector

  private val bufferOI = {
    val fieldNames = Seq("nonNullCount", "nullCount").asJava
    val fieldOIs = Seq(nonNullCountOI: ObjectInspector, nullCountOI: ObjectInspector).asJava
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  private val nonNullCountField = bufferOI.getStructFieldRef("nonNullCount")

  private val nullCountField = bufferOI.getStructFieldRef("nullCount")

  override def getNewAggregationBuffer: AggregationBuffer = new MockUDAFBuffer(0L, 0L)

  override def reset(agg: AggregationBuffer): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    buffer.nonNullCount = 0L
    buffer.nullCount = 0L
  }

  override def init(mode: Mode, parameters: Array[ObjectInspector]): ObjectInspector = bufferOI

  override def iterate(agg: AggregationBuffer, parameters: Array[AnyRef]): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    if (parameters.head eq null) {
      buffer.nullCount += 1L
    } else {
      buffer.nonNullCount += 1L
    }
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial ne null) {
      val nonNullCount = nonNullCountOI.get(bufferOI.getStructFieldData(partial, nonNullCountField))
      val nullCount = nullCountOI.get(bufferOI.getStructFieldData(partial, nullCountField))
      val buffer = agg.asInstanceOf[MockUDAFBuffer]
      buffer.nonNullCount += nonNullCount
      buffer.nullCount += nullCount
    }
  }

  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    Array[Object](buffer.nonNullCount: java.lang.Long, buffer.nullCount: java.lang.Long)
  }

  override def terminate(agg: AggregationBuffer): AnyRef = terminatePartial(agg)
}

// Same as MockUDAFEvaluator but using two aggregation buffers, one for PARTIAL1 and the other
// for PARTIAL2.
class MockUDAFEvaluator2 extends GenericUDAFEvaluator {
  private val nonNullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector

  private val nullCountOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector
  private var aggMode: Mode = null

  private val bufferOI = {
    val fieldNames = Seq("nonNullCount", "nullCount").asJava
    val fieldOIs = Seq(nonNullCountOI: ObjectInspector, nullCountOI: ObjectInspector).asJava
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  private val nonNullCountField = bufferOI.getStructFieldRef("nonNullCount")

  private val nullCountField = bufferOI.getStructFieldRef("nullCount")

  override def getNewAggregationBuffer: AggregationBuffer = {
    // These 2 modes consume original data.
    if (aggMode == Mode.PARTIAL1 || aggMode == Mode.COMPLETE) {
      new MockUDAFBuffer(0L, 0L)
    } else {
      new MockUDAFBuffer2(0L, 0L)
    }
  }

  override def reset(agg: AggregationBuffer): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    buffer.nonNullCount = 0L
    buffer.nullCount = 0L
  }

  override def init(mode: Mode, parameters: Array[ObjectInspector]): ObjectInspector = {
    aggMode = mode
    bufferOI
  }

  override def iterate(agg: AggregationBuffer, parameters: Array[AnyRef]): Unit = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer]
    if (parameters.head eq null) {
      buffer.nullCount += 1L
    } else {
      buffer.nonNullCount += 1L
    }
  }

  override def merge(agg: AggregationBuffer, partial: Object): Unit = {
    if (partial ne null) {
      val nonNullCount = nonNullCountOI.get(bufferOI.getStructFieldData(partial, nonNullCountField))
      val nullCount = nullCountOI.get(bufferOI.getStructFieldData(partial, nullCountField))
      val buffer = agg.asInstanceOf[MockUDAFBuffer2]
      buffer.nonNullCount += nonNullCount
      buffer.nullCount += nullCount
    }
  }

  // As this method is called for both states, Partial1 and Partial2, the hack in the method
  // to check for class of aggregation buffer was necessary.
  override def terminatePartial(agg: AggregationBuffer): AnyRef = {
    var result: AnyRef = null
    if (agg.getClass.toString.contains("MockUDAFBuffer2")) {
      val buffer = agg.asInstanceOf[MockUDAFBuffer2]
      result = Array[Object](buffer.nonNullCount: java.lang.Long, buffer.nullCount: java.lang.Long)
    } else {
      val buffer = agg.asInstanceOf[MockUDAFBuffer]
      result = Array[Object](buffer.nonNullCount: java.lang.Long, buffer.nullCount: java.lang.Long)
    }
    result
  }

  override def terminate(agg: AggregationBuffer): AnyRef = {
    val buffer = agg.asInstanceOf[MockUDAFBuffer2]
    Array[Object](buffer.nonNullCount: java.lang.Long, buffer.nullCount: java.lang.Long)
  }
}
