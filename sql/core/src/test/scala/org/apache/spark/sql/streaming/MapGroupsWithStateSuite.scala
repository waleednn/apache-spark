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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state.StateStore

class MapGroupsWithStateSuite extends StreamTest with BeforeAndAfterAll {

  import testImplicits._

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  test("mapGroupWithState") {
    val inputData = MemoryStream[String]

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: State[Int]) => {

      var count = state.getOption().getOrElse(0) + values.size
      if (count == 3) {
        state.remove()
        (key, "-1")
      } else {
        state.update(count)
        (key, count.toString)
      }
    }

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .mapGroupsWithState[Int, (String, String)](stateFunc) // Int => State, (Str, Str) => Out

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(1),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("b", "-1"), ("c", "1")),
      assertNumStateRows(2)
    )
  }

  test("flatMapGroupWithState") {
    val inputData = MemoryStream[String]

    // Function to maintain running count up to 2, and then remove the count
    // Returns the data and the count (-1 if count reached beyond 2 and state was just removed)
    val stateFunc = (key: String, values: Iterator[String], state: State[Int]) => {

      var count = state.getOption().getOrElse(0) + values.size
      if (count == 3) {
        state.remove()
        Iterator((key, "-1"))
      } else {
        state.update(count)
        Iterator((key, count.toString))
      }
    }

    val result =
      inputData.toDS()
        .groupByKey(x => x)
        .flatMapGroupsWithState[Int, (String, String)](stateFunc) // Int => State, (Str, Str) => Out

    testStream(result, Append)(
      AddData(inputData, "a"),
      CheckLastBatch(("a", "1")),
      assertNumStateRows(1),
      AddData(inputData, "a", "b"),
      CheckLastBatch(("a", "2"), ("b", "1")),
      assertNumStateRows(2),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b"), // should remove state for "a" and return count as -1
      CheckLastBatch(("a", "-1"), ("b", "2")),
      assertNumStateRows(1),
      StopStream,
      StartStream(),
      AddData(inputData, "a", "b", "c"), // should recreate state for "a" and return count as 1
      CheckLastBatch(("a", "1"), ("b", "-1"), ("c", "1")),
      assertNumStateRows(2)
    )
  }

  private def assertNumStateRows(numTotalRows: Long): AssertOnQuery = AssertOnQuery { q =>
    val progressWithData = q.recentProgress.filter(_.numInputRows > 0).lastOption.get
    assert(progressWithData.stateOperators(0).numRowsTotal === numTotalRows)
    true
  }
}
