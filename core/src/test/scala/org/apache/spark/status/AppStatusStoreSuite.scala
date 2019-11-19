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

package org.apache.spark.status

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.status.LiveEntityHelpers.makeNegative
import org.apache.spark.status.api.v1
import org.apache.spark.status.api.v1.{InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics}
import org.apache.spark.util.{Distribution, Utils}
import org.apache.spark.util.kvstore._

class AppStatusStoreSuite extends SparkFunSuite {

  private val uiQuantiles = Array(0.0, 0.25, 0.5, 0.75, 1.0)
  private val stageId = 1
  private val attemptId = 1

  test("quantile calculation: 1 task") {
    compareQuantiles(1, uiQuantiles)
  }

  test("quantile calculation: few tasks") {
    compareQuantiles(4, uiQuantiles)
  }

  test("quantile calculation: more tasks") {
    compareQuantiles(100, uiQuantiles)
  }

  test("quantile calculation: lots of tasks") {
    compareQuantiles(4096, uiQuantiles)
  }

  test("quantile calculation: custom quantiles") {
    compareQuantiles(4096, Array(0.01, 0.33, 0.5, 0.42, 0.69, 0.99))
  }

  test("quantile cache") {
    val store = new InMemoryStore()
    (0 until 4096).foreach { i => store.write(newTaskData(i)) }

    val appStore = new AppStatusStore(store)

    appStore.taskSummary(stageId, attemptId, Array(0.13d))
    intercept[NoSuchElementException] {
      store.read(classOf[CachedQuantile], Array(stageId, attemptId, "13"))
    }

    appStore.taskSummary(stageId, attemptId, Array(0.25d))
    val d1 = store.read(classOf[CachedQuantile], Array(stageId, attemptId, "25"))

    // Add a new task to force the cached quantile to be evicted, and make sure it's updated.
    store.write(newTaskData(4096))
    appStore.taskSummary(stageId, attemptId, Array(0.25d, 0.50d, 0.73d))

    val d2 = store.read(classOf[CachedQuantile], Array(stageId, attemptId, "25"))
    assert(d1.taskCount != d2.taskCount)

    store.read(classOf[CachedQuantile], Array(stageId, attemptId, "50"))
    intercept[NoSuchElementException] {
      store.read(classOf[CachedQuantile], Array(stageId, attemptId, "73"))
    }

    assert(store.count(classOf[CachedQuantile]) === 2)
  }

  private def createAppStore(store: KVStore, live: Boolean = false): AppStatusStore = {
    val conf = new SparkConf()
    if (live) {
      AppStatusStore.createLiveStore(conf)
    } else {
      new AppStatusStore(store)
    }
  }

  test("SPARK-26260: task summary should contain only successful tasks' metrics") {
    val testDir = Utils.createTempDir()
    val diskStore = KVUtils.open(testDir, getClass.getName)
    val inMemoryStore = new InMemoryStore

    val historyDiskAppStore = createAppStore(diskStore)
    val historyInMemoryAppStore = createAppStore(inMemoryStore)
    val liveAppStore = createAppStore(inMemoryStore, live = true)

    Seq(historyDiskAppStore, historyInMemoryAppStore, liveAppStore).foreach { appStore =>
      val store = appStore.store
      // Success and failed tasks metrics
      for (i <- 0 to 5) {
        if (i % 2 == 1) {
          store.write(newTaskData(i, status = "FAILED"))
        } else {
          store.write(newTaskData(i, status = "SUCCESS"))
        }
      }
      // Running tasks metrics (default metrics, positive metrics)
      Seq(-1, 6).foreach { metric =>
        store.write(newTaskData(metric, status = "RUNNING"))
      }

      /**
       * Following are the tasks metrics,
       * 0, 2, 4 => Success
       * 1, 3, 5 => Failed
       * -1, 6 => Running
       *
       * Task summary will consider (0, 2, 4) only
       */
      val summary = appStore.taskSummary(stageId, attemptId, uiQuantiles).get

      val values = Array(0.0, 2.0, 4.0)

      val dist = new Distribution(values, 0, values.length).getQuantiles(uiQuantiles.sorted)
      dist.zip(summary.executorRunTime).foreach { case (expected, actual) =>
        assert(expected === actual)
      }
      appStore.close()
    }
    Utils.deleteRecursively(testDir)
  }

  test("SPARK-26260: task summary should be empty for non-successful tasks") {
    // This test will check for 0 metric value for failed task
    val testDir = Utils.createTempDir()
    val diskStore = KVUtils.open(testDir, getClass.getName)
    val inMemoryStore = new InMemoryStore

    val historyDiskAppStore = createAppStore(diskStore)
    val historyInMemoryAppStore = createAppStore(inMemoryStore)
    val liveAppStore = createAppStore(inMemoryStore, live = true)

    Seq(historyDiskAppStore, historyInMemoryAppStore, liveAppStore).foreach { appStore =>
      val store = appStore.store
      (0 until 5).foreach { i => store.write(newTaskData(i, status = "FAILED")) }
      val summary = appStore.taskSummary(stageId, attemptId, uiQuantiles)
      assert(summary.size === 0)
      appStore.close()
    }
    Utils.deleteRecursively(testDir)
  }

  private def compareQuantiles(count: Int, quantiles: Array[Double]): Unit = {
    val store = new InMemoryStore()
    val values = (0 until count).map { i =>
      val task = newTaskData(i)
      store.write(task)
      i.toDouble
    }.toArray

    val summary = new AppStatusStore(store).taskSummary(stageId, attemptId, quantiles).get
    val dist = new Distribution(values, 0, values.length).getQuantiles(quantiles.sorted)

    dist.zip(summary.executorRunTime).foreach { case (expected, actual) =>
      assert(expected === actual)
    }
  }

  private def newTaskData(i: Int, status: String = "SUCCESS"): TaskDataWrapper = {

    val metrics = new v1.TaskMetrics(
      i, i, i, i, i, i, i, i, i, i,
      new InputMetrics(i, i),
      new OutputMetrics(i, i),
      new ShuffleReadMetrics(i, i, i, i, i, i, i),
      new ShuffleWriteMetrics(i, i, i))

    val hasMetrics = i >= 0

    val taskMetrics: v1.TaskMetrics = if (hasMetrics && status != "SUCCESS") {
      makeNegative(metrics)
    } else {
      metrics
    }

    new TaskDataWrapper(
      i.toLong, i, i, i, i, i, i.toString, i.toString, status, i.toString, false, Nil, None,
      hasMetrics,
      taskMetrics.executorDeserializeTime,
      taskMetrics.executorDeserializeCpuTime,
      taskMetrics.executorRunTime,
      taskMetrics.executorCpuTime,
      taskMetrics.resultSize,
      taskMetrics.jvmGcTime,
      taskMetrics.resultSerializationTime,
      taskMetrics.memoryBytesSpilled,
      taskMetrics.diskBytesSpilled,
      taskMetrics.peakExecutionMemory,
      taskMetrics.inputMetrics.bytesRead,
      taskMetrics.inputMetrics.recordsRead,
      taskMetrics.outputMetrics.bytesWritten,
      taskMetrics.outputMetrics.recordsWritten,
      taskMetrics.shuffleReadMetrics.remoteBlocksFetched,
      taskMetrics.shuffleReadMetrics.localBlocksFetched,
      taskMetrics.shuffleReadMetrics.fetchWaitTime,
      taskMetrics.shuffleReadMetrics.remoteBytesRead,
      taskMetrics.shuffleReadMetrics.remoteBytesReadToDisk,
      taskMetrics.shuffleReadMetrics.localBytesRead,
      taskMetrics.shuffleReadMetrics.recordsRead,
      taskMetrics.shuffleWriteMetrics.bytesWritten,
      taskMetrics.shuffleWriteMetrics.writeTime,
      taskMetrics.shuffleWriteMetrics.recordsWritten,
      stageId, attemptId)
  }
}
