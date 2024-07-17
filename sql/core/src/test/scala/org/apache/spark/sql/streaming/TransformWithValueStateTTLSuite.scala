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

package org.apache.spark.sql.streaming

import java.time.Duration

// import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.{ListStateImplWithTTL, MapStateImplWithTTL, MemoryStream, ValueStateImpl, ValueStateImplWithTTL}
// import org.apache.spark.sql.execution.streaming.state.{ColumnFamilySchemaV1,
// NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec,
// RocksDBStateStoreProvider, StateSchemaV3File}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
// import org.apache.spark.sql.types._

object TTLInputProcessFunction {
  def processRow(
      row: InputEvent,
      valueState: ValueStateImplWithTTL[Int]): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val key = row.key
    if (row.action == "get") {
      val currState = valueState.getOption()
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_without_enforcing_ttl") {
      val currState = valueState.getWithoutEnforcingTTL()
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "get_ttl_value_from_state") {
      val ttlValue = valueState.getTTLValue()
      if (ttlValue.isDefined) {
        val value = ttlValue.get._1
        val ttlExpiration = ttlValue.get._2
        results = OutputEvent(key, value, isTTLValue = true, ttlExpiration) :: results
      }
    } else if (row.action == "put") {
      valueState.update(row.value)
    } else if (row.action == "get_values_in_ttl_state") {
      val ttlValues = valueState.getValuesInTTLState()
      ttlValues.foreach { v =>
        results = OutputEvent(key, -1, isTTLValue = true, ttlValue = v) :: results
      }
    }

    results.iterator
  }

  def processNonTTLStateRow(
      row: InputEvent,
      valueState: ValueStateImpl[Int]): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()
    val key = row.key
    if (row.action == "get") {
      val currState = valueState.getOption()
      if (currState.isDefined) {
        results = OutputEvent(key, currState.get, isTTLValue = false, -1) :: results
      }
    } else if (row.action == "put") {
      valueState.update(row.value)
    }

    results.iterator
  }
}

class ValueStateTTLProcessor(ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent]
  with Logging {

  @transient private var _valueState: ValueStateImplWithTTL[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueState = getHandle
      .getValueState("valueState", Encoders.scalaInt, ttlConfig)
      .asInstanceOf[ValueStateImplWithTTL[Int]]
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    inputRows.foreach { row =>
      val resultIter = TTLInputProcessFunction.processRow(row, _valueState)
      resultIter.foreach { r =>
        results = r :: results
      }
    }

    results.iterator
  }
}

class MultipleValueStatesTTLProcessor(
    ttlKey: String,
    noTtlKey: String,
    ttlConfig: TTLConfig)
  extends StatefulProcessor[String, InputEvent, OutputEvent]
    with Logging {

  @transient var _valueStateWithTTL: ValueStateImplWithTTL[Int] = _
  @transient var _valueStateWithoutTTL: ValueStateImpl[Int] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    _valueStateWithTTL = getHandle
      .getValueState("valueState", Encoders.scalaInt, ttlConfig)
      .asInstanceOf[ValueStateImplWithTTL[Int]]
    _valueStateWithoutTTL = getHandle
      .getValueState("valueState", Encoders.scalaInt)
      .asInstanceOf[ValueStateImpl[Int]]
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InputEvent],
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputEvent] = {
    var results = List[OutputEvent]()

    if (key == ttlKey) {
      inputRows.foreach { row =>
        val resultIterator = TTLInputProcessFunction.processRow(row, _valueStateWithTTL)
        resultIterator.foreach { r =>
          results = r :: results
        }
      }
    } else {
      inputRows.foreach { row =>
        val resultIterator = TTLInputProcessFunction.processNonTTLStateRow(row,
          _valueStateWithoutTTL)
        resultIterator.foreach { r =>
          results = r :: results
        }
      }
    }

    results.iterator
  }
}

// Class to verify state schema is correctly written for state vars.
class TTLProcessorWithCompositeTypes(
      ttlKey: String,
      noTtlKey: String,
      ttlConfig: TTLConfig)
  extends MultipleValueStatesTTLProcessor(
    ttlKey: String, noTtlKey: String, ttlConfig: TTLConfig) {
  @transient private var _listStateWithTTL: ListStateImplWithTTL[Int] = _
  @transient private var _mapStateWithTTL: MapStateImplWithTTL[Int, String] = _

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {
    super.init(outputMode, timeMode)
    _listStateWithTTL = getHandle
      .getListState("listState", Encoders.scalaInt, ttlConfig)
      .asInstanceOf[ListStateImplWithTTL[Int]]
    _mapStateWithTTL = getHandle
      .getMapState("mapState", Encoders.scalaInt, Encoders.STRING, ttlConfig)
      .asInstanceOf[MapStateImplWithTTL[Int, String]]
  }
}

class TransformWithValueStateTTLSuite extends TransformWithStateTTLTest {

  import testImplicits._

  override def getProcessor(ttlConfig: TTLConfig):
    StatefulProcessor[String, InputEvent, OutputEvent] = {
      new ValueStateTTLProcessor(ttlConfig)
  }

  override def getStateTTLMetricName: String = "numValueStateWithTTLVars"

  test("validate multiple value states") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName) {
      val ttlKey = "k1"
      val noTtlKey = "k2"
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val inputStream = MemoryStream[InputEvent]
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          new MultipleValueStatesTTLProcessor(ttlKey, noTtlKey, ttlConfig),
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent(ttlKey, "put", 1)),
        AddData(inputStream, InputEvent(noTtlKey, "put", 2)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // get both state values, and make sure we get unexpired value
        AddData(inputStream, InputEvent(ttlKey, "get", -1)),
        AddData(inputStream, InputEvent(noTtlKey, "get", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent(ttlKey, 1, isTTLValue = false, -1),
          OutputEvent(noTtlKey, 2, isTTLValue = false, -1)
        ),
        // ensure ttl values were added correctly, and noTtlKey has no ttl values
        AddData(inputStream, InputEvent(ttlKey, "get_ttl_value_from_state", -1)),
        AddData(inputStream, InputEvent(noTtlKey, "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent(ttlKey, 1, isTTLValue = true, 61000)),
        AddData(inputStream, InputEvent(ttlKey, "get_values_in_ttl_state", -1)),
        AddData(inputStream, InputEvent(noTtlKey, "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(OutputEvent(ttlKey, -1, isTTLValue = true, 61000)),
        // advance clock after expiry
        AdvanceManualClock(60 * 1000),
        AddData(inputStream, InputEvent(ttlKey, "get", -1)),
        AddData(inputStream, InputEvent(noTtlKey, "get", -1)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        // validate ttlKey is expired, bot noTtlKey is still present
        CheckNewAnswer(OutputEvent(noTtlKey, 2, isTTLValue = false, -1)),
        // validate ttl value is removed in the value state column family
        AddData(inputStream, InputEvent(ttlKey, "get_ttl_value_from_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer()
      )
    }
  }
/*
  test("verify StateSchemaV3 writes correct SQL schema of key/value and with TTL") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key ->
        TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS.toString) {
      withTempDir { checkpointDir =>
        val metadataPathPostfix = "state/0/default/_metadata"
        val stateSchemaPath = new Path(checkpointDir.toString,
          s"$metadataPathPostfix/schema")
        val hadoopConf = spark.sessionState.newHadoopConf()
        val fm = CheckpointFileManager.create(stateSchemaPath, hadoopConf)

        val schema0 = ColumnFamilySchemaV1(
          "valueState",
          new StructType().add("key",
            new StructType().add("value", StringType)),
          new StructType().add("value",
            new StructType().add("value", LongType, false)
              .add("ttlExpirationMs", LongType)),
          NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA),
          None
        )
        val schema1 = ColumnFamilySchemaV1(
          "listState",
          new StructType().add("key",
            new StructType().add("value", StringType)),
          new StructType().add("value",
            new StructType()
              .add("value", IntegerType, false)
              .add("ttlExpirationMs", LongType)),
          NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA),
          None
        )
        val schema2 = ColumnFamilySchemaV1(
          "mapState",
          new StructType()
            .add("key", new StructType().add("value", StringType))
            .add("userKey", new StructType().add("value", StringType)),
          new StructType().add("value",
            new StructType()
              .add("value", IntegerType, false)
              .add("ttlExpirationMs", LongType)),
          PrefixKeyScanStateEncoderSpec(COMPOSITE_KEY_ROW_SCHEMA, 1),
          Option(new StructType().add("value", StringType))
        )

        val ttlKey = "k1"
        val noTtlKey = "k2"
        val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
        val inputStream = MemoryStream[InputEvent]
        val result = inputStream.toDS()
          .groupByKey(x => x.key)
          .transformWithState(
            new TTLProcessorWithCompositeTypes(ttlKey, noTtlKey, ttlConfig),
            TimeMode.ProcessingTime(),
            OutputMode.Append())

        val clock = new StreamManualClock
        testStream(result)(
          StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock,
            checkpointLocation = checkpointDir.getCanonicalPath),
          AddData(inputStream, InputEvent(ttlKey, "put", 1)),
          AddData(inputStream, InputEvent(noTtlKey, "put", 2)),
          // advance clock to trigger processing
          AdvanceManualClock(1 * 1000),
          CheckNewAnswer(),
          Execute { q =>
            println("last progress:" + q.lastProgress)
            val schemaFilePath = fm.list(stateSchemaPath).toSeq.head.getPath
            val schemaFile = new StateSchemaV3File(hadoopConf, stateSchemaPath.getName)
            val colFamilySeq = schemaFile.deserialize(fm.open(schemaFilePath))

            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics.get("numValueStateVars").toInt)
            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics
                .get("numValueStateWithTTLVars").toInt)
            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics
                .get("numListStateWithTTLVars").toInt)
            assert(TransformWithStateSuiteUtils.NUM_SHUFFLE_PARTITIONS ==
              q.lastProgress.stateOperators.head.customMetrics
                .get("numMapStateWithTTLVars").toInt)

            // TODO when there are two state var with the same name,
            // only one schema file is preserved
            assert(colFamilySeq.length == 3)
            /*
            assert(colFamilySeq.map(_.toString).toSet == Set(
              schema0, schema1, schema2
            ).map(_.toString)) */

            assert(colFamilySeq(1).toString == schema1.toString)
            assert(colFamilySeq(2).toString == schema2.toString)
            // The remaining schema file is the one without ttl
            // assert(colFamilySeq.head.toString == schema0.toString)
          },
          StopStream
        )
      }
    }
  } */
}

class TTLTest extends StreamTest {

  import testImplicits._
  def getStateTTLMetricName: String = "numValueStateWithTTLVars"

  def getProcessor(config: TTLConfig): StatefulProcessor[String,
    InputEvent, OutputEvent] = new MapStateSingleKeyTTLProcessor(config)

  test("validate only expired keys are removed from the state") {
    withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
      classOf[RocksDBStateStoreProvider].getName,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val inputStream = MemoryStream[InputEvent]
      val ttlConfig = TTLConfig(ttlDuration = Duration.ofMinutes(1))
      val result = inputStream.toDS()
        .groupByKey(x => x.key)
        .transformWithState(
          getProcessor(ttlConfig),
          TimeMode.ProcessingTime(),
          OutputMode.Append())

      val clock = new StreamManualClock
      testStream(result)(
        StartStream(Trigger.ProcessingTime("1 second"), triggerClock = clock),
        AddData(inputStream, InputEvent("k1", "put", 1)),
        // advance clock to trigger processing
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock halfway to expiration ttl, and add another key
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k2", "put", 2)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // advance clock so that key k1 is expired
        AdvanceManualClock(30 * 1000),
        AddData(inputStream, InputEvent("k1", "get", 1)),
        AddData(inputStream, InputEvent("k2", "get", -1)),
        AdvanceManualClock(1 * 1000),
        // validate k1 is expired and k2 is not
        CheckNewAnswer(OutputEvent("k2", 2, isTTLValue = false, -1)),
        // validate k1 is deleted from state
        AddData(inputStream, InputEvent("k1", "get_ttl_value_from_state", -1)),
        AddData(inputStream, InputEvent("k1", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(),
        // validate k2 exists in state
        AddData(inputStream, InputEvent("k2", "get_values_in_ttl_state", -1)),
        AdvanceManualClock(1 * 1000),
        CheckNewAnswer(
          OutputEvent("k2", -1, isTTLValue = true, 92000))
      )
    }
  }
/*
  test("encoding") {
    import org.apache.spark.sql.catalyst.InternalRow
    import org.apache.spark.sql.catalyst.encoders.encoderFor
    import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection}
    import org.apache.spark.sql.types._
    import org.apache.spark.unsafe.types.UTF8String

    val strEncoder = Encoders.STRING
    val strenc = encoderFor(strEncoder)
    val timestamp: Long = 1L

    val compositeSchema = new StructType()
      .add("timestamp", LongType)
      .add("key", new StructType(strenc.schema.fields))
      .add("userKey", new StructType(strenc.schema.fields))

    val gk = UTF8String.fromString("gk1")
    val k = UTF8String.fromString("k1")

    val keyRow = new GenericInternalRow(Array[Any](gk))
    val userkeyRow = new GenericInternalRow(Array[Any](k))

    val compositeRow = new GenericInternalRow(Array[Any](
      timestamp, keyRow, userkeyRow))

    val compositeKey = UnsafeProjection.create(compositeSchema).apply(compositeRow)

    var dgk = compositeKey.getStruct(1, 1).getString(0)
    println("After encoding, gk: " + dgk)
    println("After encoding, gk len: " +
      dgk.length)
    println("After encoding, k: " +
      compositeKey.getStruct(2, 1).getString(0))
    println("After encoding, k len: " +
      compositeKey.getStruct(2, 1).getString(0)
        .asInstanceOf[String].length)

    val userKeyRowToObjDeserializer =
      strenc.resolveAndBind().createDeserializer()
    var dk = userKeyRowToObjDeserializer.apply(compositeRow.getStruct(2, 1))
    println("after deserialize, k: " + dk)
    println("after deserialize, k len: " + dk.length)


    val newcompositeSchema = new StructType()
      .add("key", new StructType(strenc.schema.fields))
      .add("userKey", new StructType(strenc.schema.fields))
    val gkr = compositeKey.getStruct(1, 1)
    val kr = compositeKey.getStruct(2, 1)
    val newComRow = new GenericInternalRow(Array[Any](
      gkr.asInstanceOf[InternalRow], kr.asInstanceOf[InternalRow]))
    val newcomrow = UnsafeProjection.create(newcompositeSchema).apply(newComRow)

    dgk = newcomrow.getStruct(0, 1).getString(0)
    println("After encoding, gk: " + dgk)
    println("After encoding, gk len: " +
      dgk.length)
    println("After encoding, k: " +
      newcomrow.getStruct(1, 1).getString(0))
    println("After encoding, k len: " +
      newcomrow.getStruct(1, 1).getString(0)
        .asInstanceOf[String].length)

    dk = userKeyRowToObjDeserializer.apply(newcomrow.getStruct(1, 1))
    println("after deserialize, k: " + dk)
    println("after deserialize, k len: " + dk.length)
  }  */

}


