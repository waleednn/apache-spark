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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.{TaskContext, Partition}

private[spark]
class SlidedRDDPartition[T](val idx: Int, val prev: Partition, val tail: Array[T])
  extends Partition with Serializable {
  override val index: Int = idx
}

/**
 * Represents a RDD from grouping items of its parent RDD in fixed size blocks by passing a sliding
 * window over them. The ordering is first based on the partition index and then the ordering of
 * items within each partition. This is similar to sliding in Scala collections, except that it
 * becomes an empty RDD if the window size is greater than the total number of items. It needs to
 * trigger a Spark job if the parent RDD has more than one partitions.
 *
 * @param parent the parent RDD
 * @param windowSize the window size, must be greater than 1
 *
 * @see [[org.apache.spark.rdd.RDD#sliding]]
 */
private[spark]
class SlidedRDD[T: ClassTag](@transient val parent: RDD[T], val windowSize: Int)
  extends RDD[Array[T]](parent) {

  require(windowSize > 1, "Window size must be greater than 1.")

  override def compute(split: Partition, context: TaskContext): Iterator[Array[T]] = {
    val part = split.asInstanceOf[SlidedRDDPartition[T]]
    (firstParent[T].iterator(part.prev, context) ++ part.tail)
      .sliding(windowSize)
      .map(_.toArray)
      .filter(_.size == windowSize)
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[SlidedRDDPartition[T]].prev)

  override def getPartitions: Array[Partition] = {
    val parentPartitions = parent.partitions
    val n = parentPartitions.size
    if (n == 0) {
      Array.empty
    } else if (n == 1) {
      Array(new SlidedRDDPartition[T](0, parentPartitions(0), Array.empty))
    } else {
      val n1 = n - 1
      val w1 = windowSize - 1
      // Get the first w1 items of each partition, starting from the second partition.
      val nextHeads =
        parent.context.runJob(parent, (iter: Iterator[T]) => iter.take(w1).toArray, 1 until n, true)
      val partitions = mutable.ArrayBuffer[SlidedRDDPartition[T]]()
      var i = 0
      var partitionIndex = 0
      while (i < n1) {
        var j = i
        val tail = mutable.ArrayBuffer[T]()
        // Keep appending to the current tail until appended a head of size w1.
        while (j < n1 && nextHeads(j).size < w1) {
          tail ++= nextHeads(j)
          j += 1
        }
        if (j < n1) {
          tail ++= nextHeads(j)
          j += 1
        }
        partitions += new SlidedRDDPartition[T](partitionIndex, parentPartitions(i), tail.toArray)
        partitionIndex += 1
        // Skip appended heads.
        i = j
      }
      // If the head of last partition has size w1, we also need to add this partition.
      if (nextHeads(n1 - 1).size == w1) {
        partitions += new SlidedRDDPartition[T](partitionIndex, parentPartitions(n1), Array.empty)
      }
      partitions.toArray
    }
  }

  // TODO: Override methods such as aggregate, which only requires one Spark job.
}
