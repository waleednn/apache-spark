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

package org.apache.spark.streaming

import java.io.File

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}
import scala.reflect.ClassTag

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.util.{ManualClock, Utils}
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class TrackStateByKeySuite extends SparkFunSuite with BeforeAndAfterAll with BeforeAndAfter {

  private var sc: SparkContext = null
  private var ssc: StreamingContext = null
  private var checkpointDir: File = null
  private val batchDuration = Seconds(1)

  before {
    StreamingContext.getActive().foreach {
      _.stop(stopSparkContext = false)
    }
    checkpointDir = Utils.createTempDir("checkpoint")

    ssc = new StreamingContext(sc, batchDuration)
    ssc.checkpoint(checkpointDir.toString)
  }

  after {
    StreamingContext.getActive().foreach {
      _.stop(stopSparkContext = false)
    }
  }

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TrackStateByKeySuite")
    conf.set("spark.streaming.clock", classOf[ManualClock].getName())
    sc = new SparkContext(conf)
  }

  test("state - get, exists, update, remove, ") {
    var state: StateImpl[Int] = null

    def testState(
        expectedData: Option[Int],
        shouldBeUpdated: Boolean = false,
        shouldBeRemoved: Boolean = false,
        shouldBeTimingOut: Boolean = false
      ): Unit = {
      if (expectedData.isDefined) {
        assert(state.exists)
        assert(state.get() === expectedData.get)
        assert(state.getOption() === expectedData)
        assert(state.getOption.getOrElse(-1) === expectedData.get) // test implicit Option conversion
      } else {
        assert(!state.exists)
        intercept[NoSuchElementException] {
          state.get()
        }
        assert(state.getOption() === None)
        assert(state.getOption.getOrElse(-1) === -1)  // test implicit Option conversion
      }

      assert(state.isTimingOut() === shouldBeTimingOut)
      if (shouldBeTimingOut) {
        intercept[IllegalArgumentException] {
          state.remove()
        }
        intercept[IllegalArgumentException] {
          state.update(-1)
        }
      }

      assert(state.isUpdated() === shouldBeUpdated)

      assert(state.isRemoved() === shouldBeRemoved)
      if (shouldBeRemoved) {
        intercept[IllegalArgumentException] {
          state.remove()
        }
        intercept[IllegalArgumentException] {
          state.update(-1)
        }
      }
    }

    state = new StateImpl[Int]()
    testState(None)

    state.wrap(None)
    testState(None)

    state.wrap(Some(1))
    testState(Some(1))

    state.update(2)
    testState(Some(2), shouldBeUpdated = true)

    state = new StateImpl[Int]()
    state.update(2)
    testState(Some(2), shouldBeUpdated = true)

    state.remove()
    testState(None, shouldBeRemoved = true)

    state.wrapTiminoutState(3)
    testState(Some(3), shouldBeTimingOut = true)
  }

  test("trackStateByKey - basic operations with simple API") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(),
        Seq(1),
        Seq(2, 1),
        Seq(3, 2, 1),
        Seq(4, 3),
        Seq(5),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    // state maintains running count, and updated count is returned
    val trackStateFunc = (value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
      state.update(sum)
      sum
    }

    testOperation(inputData, StateSpec.function(trackStateFunc), outputData, stateData)
  }

  test("trackStateByKey - basic operations with advanced API") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(),
        Seq("aa"),
        Seq("aa", "bb"),
        Seq("aa", "bb", "cc"),
        Seq("aa", "bb"),
        Seq("aa"),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    // state maintains running count, key string doubled and returned
    val trackStateFunc = (batchTime: Time, key: String, value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
      state.update(sum)
      Some(key * 2)
    }

    testOperation(inputData, StateSpec.function(trackStateFunc), outputData, stateData)
  }

  test("trackStateByKey - states as emitted records") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3)),
        Seq(("a", 5)),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("a", 2), ("b", 1)),
        Seq(("a", 3), ("b", 2), ("c", 1)),
        Seq(("a", 4), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1)),
        Seq(("a", 5), ("b", 3), ("c", 1))
      )

    val trackStateFunc = (time: Time, key: String, value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (key, sum)
      state.update(sum)
      Some(output)
    }

    testOperation(inputData, StateSpec.function(trackStateFunc), outputData, stateData)
  }

  test("trackStateByKey - initial states, with nothing emitted") {

    val initialState = Seq(("a", 5), ("b", 10), ("c", -20), ("d", 0))

    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"),
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq()
      )

    val outputData = Seq.fill(inputData.size)(Seq.empty[Int])

    val stateData =
      Seq(
        Seq(("a", 5), ("b", 10), ("c", -20), ("d", 0)),
        Seq(("a", 6), ("b", 10), ("c", -20), ("d", 0)),
        Seq(("a", 7), ("b", 11), ("c", -20), ("d", 0)),
        Seq(("a", 8), ("b", 12), ("c", -19), ("d", 0)),
        Seq(("a", 9), ("b", 13), ("c", -19), ("d", 0)),
        Seq(("a", 10), ("b", 13), ("c", -19), ("d", 0)),
        Seq(("a", 10), ("b", 13), ("c", -19), ("d", 0))
      )

    val trackStateFunc = (time: Time, key: String, value: Option[Int], state: State[Int]) => {
      val sum = value.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (key, sum)
      state.update(sum)
      None.asInstanceOf[Option[Int]]
    }

    val trackStateSpec = StateSpec.function(trackStateFunc).initialState(sc.makeRDD(initialState))
    testOperation(inputData, trackStateSpec, outputData, stateData)
  }

  test("trackStateByKey - state removing") {
    val inputData =
      Seq(
        Seq(),
        Seq("a"),
        Seq("a", "b"), // a will be removed
        Seq("a", "b", "c"), // b will be removed
        Seq("a", "b", "c"), // a and c will be removed
        Seq("a", "b"), // b will be removed
        Seq("a"), // a will be removed
        Seq()
      )

    // States that were removed
    val outputData =
      Seq(
        Seq(),
        Seq(),
        Seq("a"),
        Seq("b"),
        Seq("a", "c"),
        Seq("b"),
        Seq("a"),
        Seq()
      )

    val stateData =
      Seq(
        Seq(),
        Seq(("a", 1)),
        Seq(("b", 1)),
        Seq(("a", 1), ("c", 1)),
        Seq(("b", 1)),
        Seq(("a", 1)),
        Seq(),
        Seq()
      )

    val trackStateFunc = (time: Time, key: String, value: Option[Int], state: State[Int]) => {
      if (state.exists) {
        state.remove()
        Some(key)
      } else {
        state.update(value.get)
        None
      }
    }

    testOperation(
      inputData, StateSpec.function(trackStateFunc).numPartitions(1), outputData, stateData)
  }

  test("trackStateByKey - state timing out") {
    val inputData =
      Seq(
        Seq("a", "b", "c"),
        Seq("a", "b"),
        Seq("a"),
        Seq(), // c will time out
        Seq(), // b will time out
        Seq("a") // a will not time out
      ) ++ Seq.fill(20)(Seq("a")) // a will continue to stay active

    val trackStateFunc = (time: Time, key: String, value: Option[Int], state: State[Int]) => {
      if (value.isDefined) {
        state.update(1)
      }
      if (state.isTimingOut) {
        Some(key)
      } else {
        None
      }
    }

    val (collectedOutputs, collectedStateSnapshots) = getOperationOutput(
      inputData, StateSpec.function(trackStateFunc).timeout(Seconds(3)), 20)

    // b and c should be emitted once each, when they were marked as expired
    assert(collectedOutputs.flatten.sorted === Seq("b", "c"))

    // States for a, b, c should be defined at one point of time
    assert(collectedStateSnapshots.exists {
      _.toSet == Set(("a", 1), ("b", 1), ("c", 1))
    })

    // Finally state should be defined only for a
    assert(collectedStateSnapshots.last.toSet === Set(("a", 1)))
  }


  private def testOperation[K: ClassTag, S: ClassTag, T: ClassTag](
      input: Seq[Seq[K]],
      trackStateSpec: StateSpec[K, Int, S, T],
      expectedOutputs: Seq[Seq[T]],
      expectedStateSnapshots: Seq[Seq[(K, S)]]
    ): Unit = {
    require(expectedOutputs.size == expectedStateSnapshots.size)

    val (collectedOutputs, collectedStateSnapshots) =
      getOperationOutput(input, trackStateSpec, expectedOutputs.size)
    assert(expectedOutputs, collectedOutputs, "outputs")
    assert(expectedStateSnapshots, collectedStateSnapshots, "state snapshots")
  }

  private def getOperationOutput[K: ClassTag, S: ClassTag, T: ClassTag](
      input: Seq[Seq[K]],
      trackStateSpec: StateSpec[K, Int, S, T],
      numBatches: Int
    ): (Seq[Seq[T]], Seq[Seq[(K, S)]]) = {

    // Setup the stream computation
    val inputStream = new TestInputStream(ssc, input, numPartitions = 2)
    val trackeStateStream = inputStream.map(x => (x, 1)).trackStateByKey(trackStateSpec)
    val collectedOutputs = new ArrayBuffer[Seq[T]] with SynchronizedBuffer[Seq[T]]
    val outputStream = new TestOutputStream(trackeStateStream, collectedOutputs)
    val collectedStateSnapshots = new ArrayBuffer[Seq[(K, S)]] with SynchronizedBuffer[Seq[(K, S)]]
    val stateSnapshotStream = new TestOutputStream(
      trackeStateStream.stateSnapshots(), collectedStateSnapshots)
    outputStream.register()
    stateSnapshotStream.register()

    val batchCounter = new BatchCounter(ssc)
    ssc.start()

    val clock = ssc.scheduler.clock.asInstanceOf[ManualClock]
    clock.advance(batchDuration.milliseconds * numBatches)

    batchCounter.waitUntilBatchesCompleted(numBatches, 10000)
    (collectedOutputs, collectedStateSnapshots)
  }

  private def assert[U](expected: Seq[Seq[U]], collected: Seq[Seq[U]], typ: String) {
    val debugString = "\nExpected:\n" + expected.mkString("\n") +
      "\nCollected:\n" + collected.mkString("\n")
    assert(expected.size === collected.size,
      s"number of collected $typ (${collected.size}) different from expected (${expected.size})" +
        debugString)
    expected.zip(collected).foreach { case (c, e) =>
      assert(c.toSet === e.toSet,
        s"collected $typ is different from expected $debugString"
      )
    }
  }
}

