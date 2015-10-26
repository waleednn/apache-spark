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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.rdd.{ReliableRDDCheckpointData, ReliableCheckpointRDD, RDD}
import org.apache.spark.storage._

class CheckpointManager extends Logging {

  /** Keys of RDD partitions that are being checkpointed. */
  private val checkpointingRDDPartitions = new mutable.HashSet[RDDBlockId]

  /** Gets or computes an RDD partition. Used by RDD.iterator() when an RDD is about to be
    * checkpointed. */
  def getOrCompute[T: ClassTag](
      rdd: RDD[T],
      checkpointData: ReliableRDDCheckpointData[T],
      partition: Partition,
      context: TaskContext): Iterator[T] = {
    val conf = checkpointData.broadcastedConf.value.value
    val path =
      new Path(checkpointData.cpDir, ReliableCheckpointRDD.checkpointFileName(partition.index))
    val key = RDDBlockId(rdd.id, partition.index)
    logDebug(s"Looking for partition $key")
    if (checkpointData.isCheckpointed) {
      // TODO how to know we should checkpoint
      return new InterruptibleIterator[T](context,
        ReliableCheckpointRDD.readCheckpointFile(path, conf, context))
    } else {
      // Acquire a lock for loading this partition
      // If another thread already holds the lock, wait for it to finish return its results
      val checkpoint = acquireLockForPartition[T](rdd, partition, key, context)
      if (checkpoint.isDefined) {
        return new InterruptibleIterator[T](context, checkpoint.get)
      }
    }

    // Otherwise, we have to load the partition ourselves
    try {
      logInfo(s"Partition $key not found, computing it")
      val computedValues = rdd.computeOrReadCache(partition, context)
      ReliableCheckpointRDD.writeCheckpointFile(
        context, computedValues, checkpointData.cpDir, conf, partition.index)
      rdd.computeOrReadCache(partition, context)
    } finally {
      checkpointingRDDPartitions.synchronized {
        checkpointingRDDPartitions.remove(key)
        checkpointingRDDPartitions.notifyAll()
      }
    }
  }

  /**
   * Acquire a loading lock for the partition identified by the given block ID.
   *
   * If the lock is free, just acquire it and return None. Otherwise, another thread is already
   * checkpointing the partition, so we wait for it to finish and return the values loaded by the
   * thread.
   */
  private def acquireLockForPartition[T](
      rdd: RDD[T],
      partition: Partition,
      id: RDDBlockId,
      context: TaskContext): Option[Iterator[T]] = {
    checkpointingRDDPartitions.synchronized {
      if (!checkpointingRDDPartitions.contains(id)) {
        // If the partition is free, acquire its lock to compute its value
        checkpointingRDDPartitions.add(id)
       return None
      } else {
        // Otherwise, wait for another thread to finish and return its result
        logInfo(s"Another thread is checkpointing $id, waiting for it to finish...")
        while (checkpointingRDDPartitions.contains(id)) {
          checkpointingRDDPartitions.wait()
        }
        logInfo(s"Finished waiting for $id")
      }
    }
    Some(rdd.computeOrReadCache(partition, context))
  }

}
