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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.atomic.AtomicLong

object EpochTracker {
  // The current epoch. Note that this is a shared reference; ContinuousWriteRDD.compute() will
  // update the underlying AtomicLong as it finishes epochs. Other code should only read the value.
  val currentEpoch: ThreadLocal[AtomicLong] = new ThreadLocal[AtomicLong] {
    override def initialValue() = new AtomicLong(-1)
  }

  /**
   * Get the current epoch for this task thread.
   */
  def getCurrentEpoch: Long = {
    currentEpoch.get().get()
  }

  /**
   * Increment the current epoch for this task thread. Should be called by [[ContinuousWriteRDD]]
   * between epochs.
   */
  def incrementCurrentEpoch(): Unit = {
    currentEpoch.get().incrementAndGet()
  }

  /**
   * Initialize the current epoch for this task thread. Should be called by [[ContinuousWriteRDD]]
   * at the beginning of a task.
   */
  def initializeCurrentEpoch(startEpoch: Long): Unit = {
    currentEpoch.get().set(startEpoch)
  }
}
