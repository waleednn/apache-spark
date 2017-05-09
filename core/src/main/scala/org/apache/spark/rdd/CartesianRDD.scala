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

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.{CompletionIterator, Utils}

private[spark]
class CartesianPartition(
    idx: Int,
    @transient private val rdd1: RDD[_],
    @transient private val rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

private[spark]
class CartesianRDD[T: ClassTag, U: ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var rdd2 : RDD[U])
  extends RDD[(T, U)](sc, Nil)
  with Serializable {

  val numPartitionsInRdd2 = rdd2.partitions.length

  override def getPartitions: Array[Partition] = {
    // create the cross product split
    val array = new Array[Partition](rdd1.partitions.length * rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new CartesianPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CartesianPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd2.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(T, U)] = {
    val blockManager = SparkEnv.get.blockManager
    val currSplit = split.asInstanceOf[CartesianPartition]
    val blockId2 = RDDBlockId(rdd2.id, currSplit.s2.index)
    // Whether the block persisted by the user with valid StorageLevel.
    val persistedInLocal = blockManager.getStatus(blockId2) match {
      case Some(result) =>
        // This meaning if the block is cached by the user? If it's valid, it shouldn't be
        // removed by other task.
        result.storageLevel.isValid
      case None => false
    }
    var cachedInLocal = false

    // Try to get data from the local, otherwise it will be cached to the local.
    def getOrElseCache(
        rdd: RDD[U],
        partition: Partition,
        context: TaskContext,
        level: StorageLevel): Iterator[U] = {
      // Because the getLocalValues return a CompletionIterator, and it will release the read
      // block after the iterator finish using. So there should update the flag.
      cachedInLocal = blockManager.getStatus(blockId2) match {
        case Some(_) => true
        case None => false
      }

      if (persistedInLocal || cachedInLocal) {
        blockManager.getLocalValues(blockId2) match {
          case Some(result) =>
            val existingMetrics = context.taskMetrics().inputMetrics
            existingMetrics.incBytesRead(result.bytes)
            return new InterruptibleIterator[U](context, result.data.asInstanceOf[Iterator[U]]) {
              override def next(): U = {
                existingMetrics.incRecordsRead(1)
                delegate.next()
              }
            }
          case None =>
            if (persistedInLocal) {
              throw new SparkException(s"Block $blockId2 was not found even though it's persisted")
            }
        }
      }

      val iterator = rdd.iterator(partition, context)
      val cachedResult = blockManager.putIterator[U](blockId2, iterator, level, false) match {
        case true =>
          cachedInLocal = true
          "successful"
        case false => "failed"
      }

      logInfo(s"Cache the block $blockId2 to local $cachedResult.")
      iterator
    }

    def removeCachedBlock(): Unit = {
      val blockManager = SparkEnv.get.blockManager
      if (!persistedInLocal || cachedInLocal || blockManager.isRemovable(blockId2)) {
        blockManager.removeOrMarkAsRemovable(blockId2, false)
      }
    }

    val resultIter =
      for (x <- rdd1.iterator(currSplit.s1, context);
           y <- getOrElseCache(rdd2, currSplit.s2, context, StorageLevel.MEMORY_AND_DISK))
        yield (x, y)

    CompletionIterator[(T, U), Iterator[(T, U)]](resultIter, removeCachedBlock())
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / numPartitionsInRdd2)
    },
    new NarrowDependency(rdd2) {
      def getParents(id: Int): Seq[Int] = List(id % numPartitionsInRdd2)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
  }
}
