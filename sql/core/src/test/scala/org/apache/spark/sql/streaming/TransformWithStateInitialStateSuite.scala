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

import org.apache.spark.sql.{Encoders, KeyValueGroupedDataset}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.{RocksDBStateStoreProvider, StateStoreMultipleColumnFamiliesNotSupportedException}
import org.apache.spark.sql.internal.SQLConf

case class InitInputRow(key: String, action: String, value: Double)

class StatefulProcessorWithInitialStateTestClass extends StatefulProcessorWithInitialState[
    String, InitInputRow, (String, String, Double), (String, Double)] {
  @transient var _valState: ValueState[Double] = _

  override def handleInitialState(
      key: String,
      initialState: (String, Double)): Unit = {
    _valState.update(initialState._2)
  }

  override def init(operatorOutputMode: OutputMode): Unit = {
    _valState = getHandle.getValueState[Double]("testInit", Encoders.scalaDouble)
  }

  override def close(): Unit = {}

  override def handleInputRows(
      key: String,
      inputRows: Iterator[InitInputRow],
      timerValues: TimerValues): Iterator[(String, String, Double)] = {
    var output = List[(String, String, Double)]()
    for (row <- inputRows) {
      if (row.action == "getOption") {
        output = (key, row.action, _valState.getOption().getOrElse(-1.0)) :: output
      } else if (row.action == "update") {
        _valState.update(row.value)
      } else if (row.action == "remove") {
        _valState.clear()
      }
    }
    output.iterator
  }
}

class AccumulateStatefulProcessorWithInitState extends StatefulProcessorWithInitialStateTestClass {
  override def handleInputRows(
      key: String,
      inputRows: Iterator[InitInputRow],
      timerValues: TimerValues): Iterator[(String, String, Double)] = {
    var output = List[(String, String, Double)]()
    for (row <- inputRows) {
      if (row.action == "getOption") {
        output = (key, row.action, _valState.getOption().getOrElse(0.0)) :: output
      } else if (row.action == "add") {
        // Update state variable as accumulative sum
        val accumulateSum = _valState.getOption().getOrElse(0.0) + row.value
        _valState.update(accumulateSum)
      } else if (row.action == "remove") {
        _valState.clear()
      }
    }
    output.iterator
  }
}

class TransformWithStateInitialStateSuite extends StreamTest {
  import testImplicits._

  private def createInitialDfForTest: KeyValueGroupedDataset[String, (String, Double)] = {
    Seq(("init_1", 40.0), ("init_2", 100.0)).toDS()
      .groupByKey(x => x._1)
      .mapValues(x => x)
  }

    test ("transformWithStateWithInitialState - streaming with rocksdb should succeed") {
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {
        val initStateDf = createInitialDfForTest
        val inputData = MemoryStream[InitInputRow]
        val query = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new
              StatefulProcessorWithInitialStateTestClass(),
            TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
          )
        testStream(query, OutputMode.Update())(
          // Operations in the base class will work
          AddData(inputData, InitInputRow("k1", "update", 37.0)),
          AddData(inputData, InitInputRow("k2", "update", 40.0)),
          AddData(inputData, InitInputRow("non-exist", "getOption", -1.0)),
          CheckNewAnswer(("non-exist", "getOption", -1.0)),
          AddData(inputData, InitInputRow("k1", "remove", -1.0)),
          AddData(inputData, InitInputRow("k1", "getOption", -1.0)),
          CheckNewAnswer(("k1", "getOption", -1.0)),
          // Every row in initial State is processed
          AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
          CheckNewAnswer(("init_1", "getOption", 40.0)),
          AddData(inputData, InitInputRow("init_2", "getOption", -1.0)),
          CheckNewAnswer(("init_2", "getOption", 100.0)),
          // Update row with key in initial row will work
          AddData(inputData, InitInputRow("init_1", "update", 50.0)),
          AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
          CheckNewAnswer(("init_1", "getOption", 50.0)),
          AddData(inputData, InitInputRow("init_1", "remove", -1.0)),
          AddData(inputData, InitInputRow("init_1", "getOption", -1.0)),
          CheckNewAnswer(("init_1", "getOption", -1.0))
        )
      }
    }
    test("transformWithStateWithInitialState - processInitialState should only run once") {
      withSQLConf(SQLConf.STATE_STORE_PROVIDER_CLASS.key ->
        classOf[RocksDBStateStoreProvider].getName) {
        val initStateDf = createInitialDfForTest
        val inputData = MemoryStream[InitInputRow]
        val query = inputData.toDS()
          .groupByKey(x => x.key)
          .transformWithState(new
              AccumulateStatefulProcessorWithInitState(),
            TimeoutMode.NoTimeouts(), OutputMode.Append(), initStateDf
          )
        testStream(query, OutputMode.Update())(
          AddData(inputData, InitInputRow("init_1", "add", 50.0)),
          AddData(inputData, InitInputRow("init_2", "add", 60.0)),
          AddData(inputData, InitInputRow("init_1", "add", 50.0)),
          // If processInitialState was processed multiple times,
          // following checks will fail
          AddData(inputData,
            InitInputRow("init_1", "getOption", -1.0), InitInputRow("init_2", "getOption", -1.0)),
          CheckNewAnswer(("init_2", "getOption", 160.0), ("init_1", "getOption", 140.0))
        )
      }
    }
    test("transformWithStateWithInitialState - streaming with hdfsStateStoreProvider should fail") {
      val inputData = MemoryStream[InitInputRow]
      val result = inputData.toDS()
        .groupByKey(x => x.key)
        .transformWithState(new
            StatefulProcessorWithInitialStateTestClass(),
          TimeoutMode.NoTimeouts(), OutputMode.Append(), createInitialDfForTest
        )
      testStream(result, OutputMode.Update())(
        AddData(inputData, InitInputRow("a", "update", -1.0)),
        ExpectFailure[StateStoreMultipleColumnFamiliesNotSupportedException] {
          (t: Throwable) => {
            assert(t.getMessage.contains("not supported"))
          }
        }
      )
    }

  test("transformWithStateWithInitialState - batch should succeed") {
    val inputData = Seq(InitInputRow("k1", "update", 37.0), InitInputRow("k1", "getOption", -1.0))
    val result = inputData.toDS()
      .groupByKey(x => x.key)
      .transformWithState(new
          StatefulProcessorWithInitialStateTestClass(),
        TimeoutMode.NoTimeouts(),
        OutputMode.Append(), createInitialDfForTest)

    val df = result.toDF()
    checkAnswer(df, Seq(("k1", "getOption", 37.0)).toDF())
  }
}
