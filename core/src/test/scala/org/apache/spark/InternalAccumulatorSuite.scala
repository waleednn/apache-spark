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

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}


class InternalAccumulatorSuite extends SparkFunSuite with LocalSparkContext {
  import InternalAccumulator._
  import AccumulatorParam._

  test("get param") {
    assert(getParam(EXECUTOR_DESERIALIZE_TIME) === LongAccumulatorParam)
    assert(getParam(EXECUTOR_RUN_TIME) === LongAccumulatorParam)
    assert(getParam(RESULT_SIZE) === LongAccumulatorParam)
    assert(getParam(JVM_GC_TIME) === LongAccumulatorParam)
    assert(getParam(RESULT_SERIALIZATION_TIME) === LongAccumulatorParam)
    assert(getParam(MEMORY_BYTES_SPILLED) === LongAccumulatorParam)
    assert(getParam(DISK_BYTES_SPILLED) === LongAccumulatorParam)
    assert(getParam(PEAK_EXECUTION_MEMORY) === LongAccumulatorParam)
    assert(getParam(UPDATED_BLOCK_STATUSES) === UpdatedBlockStatusesAccumulatorParam)
    assert(getParam(TEST_ACCUM) === LongAccumulatorParam)
    // shuffle read
    assert(getParam(shuffleRead.REMOTE_BLOCKS_FETCHED) === IntAccumulatorParam)
    assert(getParam(shuffleRead.LOCAL_BLOCKS_FETCHED) === IntAccumulatorParam)
    assert(getParam(shuffleRead.REMOTE_BYTES_READ) === LongAccumulatorParam)
    assert(getParam(shuffleRead.LOCAL_BYTES_READ) === LongAccumulatorParam)
    assert(getParam(shuffleRead.FETCH_WAIT_TIME) === LongAccumulatorParam)
    assert(getParam(shuffleRead.RECORDS_READ) === LongAccumulatorParam)
    // shuffle write
    assert(getParam(shuffleWrite.BYTES_WRITTEN) === LongAccumulatorParam)
    assert(getParam(shuffleWrite.RECORDS_WRITTEN) === LongAccumulatorParam)
    assert(getParam(shuffleWrite.WRITE_TIME) === LongAccumulatorParam)
    // input
    assert(getParam(input.READ_METHOD) === StringAccumulatorParam)
    assert(getParam(input.RECORDS_READ) === LongAccumulatorParam)
    assert(getParam(input.BYTES_READ) === LongAccumulatorParam)
    // output
    assert(getParam(output.WRITE_METHOD) === StringAccumulatorParam)
    assert(getParam(output.RECORDS_WRITTEN) === LongAccumulatorParam)
    assert(getParam(output.BYTES_WRITTEN) === LongAccumulatorParam)
    intercept[AssertionError] {
      getParam("something that does not start with the right prefix")
    }
  }

  test("create by name") {
    val executorRunTime = create(EXECUTOR_RUN_TIME)
    val updatedBlockStatuses = create(UPDATED_BLOCK_STATUSES)
    val shuffleRemoteBlocksRead = create(shuffleRead.REMOTE_BLOCKS_FETCHED)
    val inputReadMethod = create(input.READ_METHOD)
    assert(executorRunTime.isInstanceOf[Accumulator[Long]])
    assert(executorRunTime.name === Some(EXECUTOR_RUN_TIME))
    assert(updatedBlockStatuses.isInstanceOf[Accumulator[Seq[(BlockId, BlockStatus)]]])
    assert(updatedBlockStatuses.name === Some(UPDATED_BLOCK_STATUSES))
    assert(shuffleRemoteBlocksRead.isInstanceOf[Accumulator[Int]])
    assert(shuffleRemoteBlocksRead.name === Some(shuffleRead.REMOTE_BLOCKS_FETCHED))
    assert(inputReadMethod.isInstanceOf[Accumulator[String]])
    assert(inputReadMethod.name === Some(input.READ_METHOD))
  }

  test("create") {
    val accums = create()
    val shuffleReadAccums = createShuffleReadAccums()
    val shuffleWriteAccums = createShuffleWriteAccums()
    val inputAccums = createInputAccums()
    val outputAccums = createOutputAccums()
    // assert they're all internal
    assert(accums.forall(_.isInternal))
    assert(shuffleReadAccums.forall(_.isInternal))
    assert(shuffleWriteAccums.forall(_.isInternal))
    assert(inputAccums.forall(_.isInternal))
    assert(outputAccums.forall(_.isInternal))
    // assert they all count on failures
    assert(accums.forall(_.countFailedValues))
    assert(shuffleReadAccums.forall(_.countFailedValues))
    assert(shuffleWriteAccums.forall(_.countFailedValues))
    assert(inputAccums.forall(_.countFailedValues))
    assert(outputAccums.forall(_.countFailedValues))
    // assert they all have names
    assert(accums.forall(_.name.isDefined))
    assert(shuffleReadAccums.forall(_.name.isDefined))
    assert(shuffleWriteAccums.forall(_.name.isDefined))
    assert(inputAccums.forall(_.name.isDefined))
    assert(outputAccums.forall(_.name.isDefined))
    // assert `accums` is a strict superset of the others
    val accumNames = accums.map(_.name.get).toSet
    val shuffleReadAccumNames = shuffleReadAccums.map(_.name.get).toSet
    val shuffleWriteAccumNames = shuffleWriteAccums.map(_.name.get).toSet
    val inputAccumNames = inputAccums.map(_.name.get).toSet
    val outputAccumNames = outputAccums.map(_.name.get).toSet
    assert(shuffleReadAccumNames.subsetOf(accumNames))
    assert(shuffleWriteAccumNames.subsetOf(accumNames))
    assert(inputAccumNames.subsetOf(accumNames))
    assert(outputAccumNames.subsetOf(accumNames))
  }

  test("naming") {
    val accums = create()
    val shuffleReadAccums = createShuffleReadAccums()
    val shuffleWriteAccums = createShuffleWriteAccums()
    val inputAccums = createInputAccums()
    val outputAccums = createOutputAccums()
    // assert that prefixes are properly namespaced
    assert(SHUFFLE_READ_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(SHUFFLE_WRITE_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(INPUT_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(OUTPUT_METRICS_PREFIX.startsWith(METRICS_PREFIX))
    assert(accums.forall(_.name.get.startsWith(METRICS_PREFIX)))
    // assert they all start with the expected prefixes
    assert(shuffleReadAccums.forall(_.name.get.startsWith(SHUFFLE_READ_METRICS_PREFIX)))
    assert(shuffleWriteAccums.forall(_.name.get.startsWith(SHUFFLE_WRITE_METRICS_PREFIX)))
    assert(inputAccums.forall(_.name.get.startsWith(INPUT_METRICS_PREFIX)))
    assert(outputAccums.forall(_.name.get.startsWith(OUTPUT_METRICS_PREFIX)))
  }

  test("internal accumulators in TaskContext") {
    sc = new SparkContext("local", "test")
    val taskContext = new TaskContextImpl(0, 0, 0, 0, null, null)
    val accumUpdates = taskContext.taskMetrics.accumulatorUpdates()
    assert(accumUpdates.size > 0)
    assert(accumUpdates.forall(_.internal))
    val testAccum = taskContext.taskMetrics.getAccum(TEST_ACCUM)
    assert(accumUpdates.exists(_.id == testAccum.id))
  }

  test("internal accumulators in a stage") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Have each task add 1 to the internal accumulator
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitions { iter =>
      TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 1
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions)
      // The accumulator values should be merged in the stage
      val stageAccum = findTestAccum(stageInfos.head.accumulables.values)
      assert(stageAccum.value.get.toString.toLong === numPartitions)
      // The accumulator should be updated locally on each task
      val taskAccumValues = taskInfos.map { taskInfo =>
        val taskAccum = findTestAccum(taskInfo.accumulables)
        assert(taskAccum.update.isDefined)
        assert(taskAccum.update.get.toString.toLong === 1L)
        taskAccum.value.get.toString.toLong
      }
      // Each task should keep track of the partial value on the way, i.e. 1, 2, ... numPartitions
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
  }

  test("internal accumulators in multiple stages") {
    val listener = new SaveInfoListener
    val numPartitions = 10
    sc = new SparkContext("local", "test")
    sc.addSparkListener(listener)
    // Each stage creates its own set of internal accumulators so the
    // values for the same metric should not be mixed up across stages
    val rdd = sc.parallelize(1 to 100, numPartitions)
      .map { i => (i, i) }
      .mapPartitions { iter =>
      TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 1
      iter
    }
      .reduceByKey { case (x, y) => x + y }
      .mapPartitions { iter =>
      TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 10
      iter
    }
      .repartition(numPartitions * 2)
      .mapPartitions { iter =>
      TaskContext.get().taskMetrics().getAccum(TEST_ACCUM) += 100
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
    // We ran 3 stages, and the accumulator values should be distinct
      val stageInfos = listener.getCompletedStageInfos
      assert(stageInfos.size === 3)
      val (firstStageAccum, secondStageAccum, thirdStageAccum) =
        (findTestAccum(stageInfos(0).accumulables.values),
          findTestAccum(stageInfos(1).accumulables.values),
          findTestAccum(stageInfos(2).accumulables.values))
      assert(firstStageAccum.value.get.toString.toLong === numPartitions)
      assert(secondStageAccum.value.get.toString.toLong === numPartitions * 10)
      assert(thirdStageAccum.value.get.toString.toLong === numPartitions * 2 * 100)
    }
    rdd.count()
  }

  test("internal accumulators in fully resubmitted stages") {
    testInternalAccumulatorsWithFailedTasks((i: Int) => true) // fail all tasks
  }

  test("internal accumulators in partially resubmitted stages") {
    testInternalAccumulatorsWithFailedTasks((i: Int) => i % 2 == 0) // fail a subset
  }

  /**
   * Return the accumulable info that matches the specified name.
   */
  private def findTestAccum(accums: Iterable[AccumulableInfo]): AccumulableInfo = {
    accums.find { a => a.name == TEST_ACCUM }.getOrElse {
      fail(s"unable to find internal accumulator called $TEST_ACCUM")
    }
  }

  /**
   * Test whether internal accumulators are merged properly if some tasks fail.
   */
  private def testInternalAccumulatorsWithFailedTasks(failCondition: (Int => Boolean)): Unit = {
    val listener = new SaveInfoListener
    val numPartitions = 10
    val numFailedPartitions = (0 until numPartitions).count(failCondition)
    // This says use 1 core and retry tasks up to 2 times
    sc = new SparkContext("local[1, 2]", "test")
    sc.addSparkListener(listener)
    val rdd = sc.parallelize(1 to 100, numPartitions).mapPartitionsWithIndex { case (i, iter) =>
      val taskContext = TaskContext.get()
      taskContext.taskMetrics.getAccum(TEST_ACCUM) += 1
      // Fail the first attempts of a subset of the tasks
      if (failCondition(i) && taskContext.attemptNumber() == 0) {
        throw new Exception("Failing a task intentionally.")
      }
      iter
    }
    // Register asserts in job completion callback to avoid flakiness
    listener.registerJobCompletionCallback { _ =>
      val stageInfos = listener.getCompletedStageInfos
      val taskInfos = listener.getCompletedTaskInfos
      assert(stageInfos.size === 1)
      assert(taskInfos.size === numPartitions + numFailedPartitions)
      val stageAccum = findTestAccum(stageInfos.head.accumulables.values)
      // We should not double count values in the merged accumulator
      assert(stageAccum.value.get.toString.toLong === numPartitions)
      val taskAccumValues = taskInfos.flatMap { taskInfo =>
        if (!taskInfo.failed) {
          // If a task succeeded, its update value should always be 1
          val taskAccum = findTestAccum(taskInfo.accumulables)
          assert(taskAccum.update.isDefined)
          assert(taskAccum.update.get.toString.toLong === 1L)
          Some(taskAccum.value.get.toString.toLong)
        } else {
          // If a task failed, we should not get its accumulator values
          assert(taskInfo.accumulables.isEmpty)
          None
        }
      }
      assert(taskAccumValues.sorted === (1L to numPartitions).toSeq)
    }
    rdd.count()
  }

}
