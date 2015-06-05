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

package org.apache.spark.shuffle.hash

import java.io.File

import org.apache.spark._
import org.apache.spark.shuffle._
import org.apache.spark.storage.ShuffleBlockId

/**
 * A ShuffleManager using hashing, that creates one output file per reduce partition on each
 * mapper (possibly reusing these across waves of tasks).
 */
private[spark] class HashShuffleManager(conf: SparkConf) extends ShuffleManager {

  private val fileShuffleBlockResolver = new FileShuffleBlockResolver(conf)

  /* Register a shuffle with the manager and obtain a handle for it to pass to tasks. */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle(shuffleId, numMaps, dependency)
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new HashShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      stageAttemptId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    addShuffleAttempt(handle.shuffleId, stageAttemptId)
    new HashShuffleWriter(shuffleBlockResolver, handle.asInstanceOf[BaseShuffleHandle[K, V, _]],
      mapId, stageAttemptId, context)
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    stageAttemptsForShuffle(shuffleId).forall { stageAttemptId =>
      shuffleBlockResolver.removeShuffle(ShuffleIdAndAttempt(shuffleId, stageAttemptId))
    }
  }

  private[shuffle] override def getShuffleFiles(
      handle: ShuffleHandle,
      mapId: Int,
      reduceId: Int,
      stageAttemptId: Int): Seq[File] = {
    val blockId = ShuffleBlockId(handle.shuffleId, mapId, reduceId, stageAttemptId)
    fileShuffleBlockResolver.getShuffleFiles(blockId)
  }


  override def shuffleBlockResolver: FileShuffleBlockResolver = {
    fileShuffleBlockResolver
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}
