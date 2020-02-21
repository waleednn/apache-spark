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

package org.apache.spark

import java.util.concurrent.Semaphore
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference
import scala.util.control.NonFatal

import org.scalatest.Matchers
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.scheduler._
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.{AccumulatorContext, AccumulatorMetadata, AccumulatorMode, AccumulatorV2, DoubleAccumulator, LongAccumulator, ReliableDoubleAccumulator, ReliableLongAccumulator}


class AccumulatorSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  import AccumulatorSuite.{createDoubleAccum, createLongAccum, createReliableDoubleAccum, createReliableLongAccum}

  override def afterEach(): Unit = {
    try {
      AccumulatorContext.clear()
    } finally {
      super.afterEach()
    }
  }

  test("accumulator serialization") {
    val ser = new JavaSerializer(new SparkConf).newInstance()
    val acc = createLongAccum("x")
    acc.add(5)
    assert(acc.value == 5)
    assert(acc.isAtDriverSide)

    // serialize and de-serialize it, to simulate sending accumulator to executor.
    val acc2 = ser.deserialize[LongAccumulator](ser.serialize(acc))
    // value is reset on the executors
    assert(acc2.value == 0)
    assert(!acc2.isAtDriverSide)

    acc2.add(10)
    // serialize and de-serialize it again, to simulate sending accumulator back to driver.
    val acc3 = ser.deserialize[LongAccumulator](ser.serialize(acc2))
    // value is not reset on the driver
    assert(acc3.value == 10)
    assert(acc3.isAtDriverSide)
  }

  test("get accum") {
    // Don't register with SparkContext for cleanup
    var acc = createLongAccum("a")
    val accId = acc.id
    val ref = WeakReference(acc)
    assert(ref.get.isDefined)

    // Remove the explicit reference to it and allow weak reference to get garbage collected
    acc = null
    System.gc()
    assert(ref.get.isEmpty)

    // Getting a garbage collected accum should return None.
    assert(AccumulatorContext.get(accId).isEmpty)

    // Getting a normal accumulator. Note: this has to be separate because referencing an
    // accumulator above in an `assert` would keep it from being garbage collected.
    val acc2 = createLongAccum("b")
    assert(AccumulatorContext.get(acc2.id) === Some(acc2))

    // Getting an accumulator that does not exist should return None
    assert(AccumulatorContext.get(100000).isEmpty)
  }

  test("long accumulator fragments all") {
    val acc = createReliableLongAccum("long", mode = AccumulatorMode.All)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.value == 0)

    val acc1 = acc.copyAndReset()
    acc1.add(1L)

    val acc2 = acc.copyAndReset()
    acc2.add(2L)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1)
    assert(acc.value == 1)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 3)
    assert(acc.sum == 4)
    assert(acc.value == 4)

    // second fragment
    acc.mergeFragment(acc1, 2)
    assert(acc.count == 4)
    assert(acc.sum == 5)
    assert(acc.value == 5)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 5)
    assert(acc.sum == 6)
    assert(acc.value == 6)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 6)
    assert(acc.sum == 8)
    assert(acc.value == 8)
  }

  test("long accumulator fragments first") {
    val acc = createReliableLongAccum("long", mode = AccumulatorMode.First)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.value == 0)

    val acc1 = acc.copyAndReset().asInstanceOf[ReliableLongAccumulator]
    acc1.add(1L)

    val acc2 = acc.copyAndReset().asInstanceOf[ReliableLongAccumulator]
    acc2.add(1L)
    acc2.add(1L)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1)
    assert(acc.value == 1)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1)
    assert(acc.value == 1)

    // second fragment
    acc.mergeFragment(acc2, 2)
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.value == 3)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 3)
    assert(acc.sum == 3)
    assert(acc.value == 3)
  }

  test("long accumulator fragments larger") {
    val acc = createReliableLongAccum("long", mode = AccumulatorMode.Larger)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.value == 0)

    val acc1 = acc.copyAndReset()
    acc1.add(2L)

    val acc2 = acc.copyAndReset()
    acc2.add(1L)
    acc2.add(1L)

    val acc3 = acc.copyAndReset()
    acc3.add(3L)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    // second fragment
    acc.mergeFragment(acc3, 2)
    assert(acc.count == 3)
    assert(acc.sum == 5)
    assert(acc.value == 5)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 3)
    assert(acc.sum == 5)
    assert(acc.value == 5)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4)
    assert(acc.value == 4)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4)
    assert(acc.value == 4)

    acc.mergeFragment(acc3, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4)
    assert(acc.value == 4)
  }

  test("long accumulator fragments last") {
    val acc = createReliableLongAccum("long", mode = AccumulatorMode.Last)
    assert(acc.count == 0)
    assert(acc.sum == 0)
    assert(acc.value == 0)

    val acc1 = acc.copyAndReset()
    acc1.add(1L)

    val acc2 = acc.copyAndReset()
    acc2.add(2L)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1)
    assert(acc.value == 1)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1)
    assert(acc.value == 1)

    // second fragment
    acc.mergeFragment(acc1, 2)
    assert(acc.count == 2)
    assert(acc.sum == 2)
    assert(acc.value == 2)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 2)
    assert(acc.sum == 3)
    assert(acc.value == 3)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 2)
    assert(acc.sum == 2)
    assert(acc.value == 2)
  }

  test("double accumulator fragments all") {
    val acc = createDoubleAccum("double", mode = AccumulatorMode.All)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.value == 0.0)

    val acc1 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc1.add(1.0)

    val acc2 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc2.add(2.0)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1.0)
    assert(acc.value == 1.0)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 3)
    assert(acc.sum == 4.0)
    assert(acc.value == 4.0)

    // second fragment
    acc.mergeFragment(acc1, 2)
    assert(acc.count == 4)
    assert(acc.sum == 5.0)
    assert(acc.value == 5.0)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 5)
    assert(acc.sum == 6.0)
    assert(acc.value == 6.0)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 6)
    assert(acc.sum == 8.0)
    assert(acc.value == 8.0)
  }

  test("double accumulator fragments first") {
    val acc = createDoubleAccum("double", mode = AccumulatorMode.First)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.value == 0.0)

    val acc1 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc1.add(1.0)

    val acc2 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc2.add(1.0)
    acc2.add(1.0)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1.0)
    assert(acc.value == 1.0)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1.0)
    assert(acc.value == 1.0)

    // second fragment
    acc.mergeFragment(acc2, 2)
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.value == 3.0)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 3)
    assert(acc.sum == 3.0)
    assert(acc.value == 3.0)
  }

  test("double accumulator fragments larger") {
    val acc = createDoubleAccum("double", mode = AccumulatorMode.Larger)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.value == 0.0)

    val acc1 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc1.add(2.0)

    val acc2 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc2.add(1.0)
    acc2.add(1.0)

    val acc3 = acc.copyAndReset()
    acc3.add(3.0)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 2)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    // second fragment
    acc.mergeFragment(acc3, 2)
    assert(acc.count == 3)
    assert(acc.sum == 5.0)
    assert(acc.value == 5.0)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 3)
    assert(acc.sum == 5.0)
    assert(acc.value == 5.0)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4.0)
    assert(acc.value == 4.0)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4.0)
    assert(acc.value == 4.0)

    acc.mergeFragment(acc3, 2)
    assert(acc.count == 4)
    assert(acc.sum == 4.0)
    assert(acc.value == 4.0)
  }

  test("double accumulator fragments last") {
    val acc = createDoubleAccum("double", mode = AccumulatorMode.Last)
    assert(acc.count == 0)
    assert(acc.sum == 0.0)
    assert(acc.value == 0.0)

    val acc1 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc1.add(1.0)

    val acc2 = acc.copyAndReset().asInstanceOf[ReliableDoubleAccumulator]
    acc2.add(2.0)

    // first fragment
    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1.0)
    assert(acc.value == 1.0)

    acc.mergeFragment(acc2, 1)
    assert(acc.count == 1)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc1, 1)
    assert(acc.count == 1)
    assert(acc.sum == 1.0)
    assert(acc.value == 1.0)

    // second fragment
    acc.mergeFragment(acc1, 2)
    assert(acc.count == 2)
    assert(acc.sum == 2.0)
    assert(acc.value == 2.0)

    acc.mergeFragment(acc2, 2)
    assert(acc.count == 2)
    assert(acc.sum == 3.0)
    assert(acc.value == 3.0)

    acc.mergeFragment(acc1, 2)
    assert(acc.count == 2)
    assert(acc.sum == 2.0)
    assert(acc.value == 2)
  }

}

private[spark] object AccumulatorSuite {
  import InternalAccumulator._

  /**
   * Create a double accumulator and register it to `AccumulatorContext`.
   */
  def createDoubleAccum(
      name: String,
      countFailedValues: Boolean = false,
      initValue: Double = 0,
      id: Long = AccumulatorContext.newId(),
      mode: AccumulatorMode = AccumulatorMode.All): ReliableDoubleAccumulator = {
    val acc = new ReliableDoubleAccumulator
    acc.setValue(initValue)
    acc.metadata = AccumulatorMetadata(id, Some(name), countFailedValues, mode)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * Create a long accumulator and register it to `AccumulatorContext`.
   */
  def createLongAccum(
      name: String,
      countFailedValues: Boolean = false,
      initValue: Long = 0,
      id: Long = AccumulatorContext.newId(),
      mode: AccumulatorMode = AccumulatorMode.All): LongAccumulator = {
    val acc = new LongAccumulator
    acc.setValue(initValue)
    acc.metadata = AccumulatorMetadata(id, Some(name), countFailedValues, mode)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * Create a reliable double accumulator and register it to `AccumulatorContext`.
   */
  def createReliableDoubleAccum(
      name: String,
      countFailedValues: Boolean = false,
      initValue: Double = 0,
      id: Long = AccumulatorContext.newId(),
      mode: AccumulatorMode = AccumulatorMode.All): DoubleAccumulator = {
    val acc = new ReliableDoubleAccumulator
    acc.setValue(initValue)
    acc.metadata = AccumulatorMetadata(id, Some(name), countFailedValues, mode)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * Create a reliable long accumulator and register it to `AccumulatorContext`.
   */
  def createReliableLongAccum(
      name: String,
      countFailedValues: Boolean = false,
      initValue: Long = 0,
      id: Long = AccumulatorContext.newId(),
      mode: AccumulatorMode = AccumulatorMode.All): ReliableLongAccumulator = {
    val acc = new ReliableLongAccumulator
    acc.setValue(initValue)
    acc.metadata = AccumulatorMetadata(id, Some(name), countFailedValues, mode)
    AccumulatorContext.register(acc)
    acc
  }

  /**
   * Make an `AccumulableInfo` out of an `AccumulatorV2` with the intent to use the
   * info as an accumulator update.
   */
  def makeInfo(a: AccumulatorV2[_, _]): AccumulableInfo = a.toInfo(Some(a.value), None)

  /**
   * Run one or more Spark jobs and verify that in at least one job the peak execution memory
   * accumulator is updated afterwards.
   */
  def verifyPeakExecutionMemorySet(
      sc: SparkContext,
      testName: String)(testBody: => Unit): Unit = {
    val listener = new SaveInfoListener
    sc.addSparkListener(listener)
    testBody
    // wait until all events have been processed before proceeding to assert things
    sc.listenerBus.waitUntilEmpty()
    val accums = listener.getCompletedStageInfos.flatMap(_.accumulables.values)
    val isSet = accums.exists { a =>
      a.name == Some(PEAK_EXECUTION_MEMORY) && a.value.exists(_.asInstanceOf[Long] > 0L)
    }
    if (!isSet) {
      throw new TestFailedException(s"peak execution memory accumulator not set in '$testName'", 0)
    }
  }
}

/**
 * A simple listener that keeps track of the TaskInfos and StageInfos of all completed jobs.
 */
private class SaveInfoListener extends SparkListener {
  type StageId = Int
  type StageAttemptId = Int

  private val completedStageInfos = new ArrayBuffer[StageInfo]
  private val completedTaskInfos =
    new mutable.HashMap[(StageId, StageAttemptId), ArrayBuffer[TaskInfo]]

  // Callback to call when a job completes. Parameter is job ID.
  @GuardedBy("this")
  private var jobCompletionCallback: () => Unit = null
  private val jobCompletionSem = new Semaphore(0)
  private var exception: Throwable = null

  def getCompletedStageInfos: Seq[StageInfo] = completedStageInfos.toArray.toSeq
  def getCompletedTaskInfos: Seq[TaskInfo] = completedTaskInfos.values.flatten.toSeq
  def getCompletedTaskInfos(stageId: StageId, stageAttemptId: StageAttemptId): Seq[TaskInfo] =
    completedTaskInfos.getOrElse((stageId, stageAttemptId), Seq.empty[TaskInfo])

  /**
   * If `jobCompletionCallback` is set, block until the next call has finished.
   * If the callback failed with an exception, throw it.
   */
  def awaitNextJobCompletion(): Unit = {
    if (jobCompletionCallback != null) {
      jobCompletionSem.acquire()
      if (exception != null) {
        throw exception
      }
    }
  }

  /**
   * Register a callback to be called on job end.
   * A call to this should be followed by [[awaitNextJobCompletion]].
   */
  def registerJobCompletionCallback(callback: () => Unit): Unit = {
    jobCompletionCallback = callback
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (jobCompletionCallback != null) {
      try {
        jobCompletionCallback()
      } catch {
        // Store any exception thrown here so we can throw them later in the main thread.
        // Otherwise, if `jobCompletionCallback` threw something it wouldn't fail the test.
        case NonFatal(e) => exception = e
      } finally {
        jobCompletionSem.release()
      }
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStageInfos += stageCompleted.stageInfo
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    completedTaskInfos.getOrElseUpdate(
      (taskEnd.stageId, taskEnd.stageAttemptId), new ArrayBuffer[TaskInfo]) += taskEnd.taskInfo
  }
}
