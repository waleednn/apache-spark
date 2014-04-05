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

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer

/**
 * <span class="badge badge-red">DEVELOPER API - UNSTABLE</span>
 *
 * Base class for dependencies.
 */
abstract class Dependency[T](val rdd: RDD[T]) extends Serializable


/**
 * <span class="badge badge-red">DEVELOPER API - UNSTABLE</span>
 *
 * Base class for dependencies where each partition of the parent RDD is used by at most one
 * partition of the child RDD.  Narrow dependencies allow for pipelined execution.
 */
abstract class NarrowDependency[T](rdd: RDD[T]) extends Dependency(rdd) {
  /**
   * Get the parent partitions for a child partition.
   * @param partitionId a partition of the child RDD
   * @return the partitions of the parent RDD that the child partition depends upon
   */
  def getParents(partitionId: Int): Seq[Int]
}


/**
 * <span class="badge badge-red">DEVELOPER API - UNSTABLE</span>
 *
 * Represents a dependency on the output of a shuffle stage.
 * @param rdd the parent RDD
 * @param partitioner partitioner used to partition the shuffle output
 * @param serializer [[org.apache.spark.serializer.Serializer Serializer]] to use. If set to null,
 *                   the default serializer, as specified by `spark.serializer` config option, will
 *                   be used.
 */
class ShuffleDependency[K, V](
    @transient rdd: RDD[_ <: Product2[K, V]],
    val partitioner: Partitioner,
    val serializer: Serializer = null)
  extends Dependency(rdd.asInstanceOf[RDD[Product2[K, V]]]) {

  val shuffleId: Int = rdd.context.newShuffleId()
}


/**
 * <span class="badge badge-red">DEVELOPER API - UNSTABLE</span>
 *
 * Represents a one-to-one dependency between partitions of the parent and child RDDs.
 */
class OneToOneDependency[T](rdd: RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int) = List(partitionId)
}


/**
 * <span class="badge badge-red">DEVELOPER API - UNSTABLE</span>
 *
 * Represents a one-to-one dependency between ranges of partitions in the parent and child RDDs.
 * @param rdd the parent RDD
 * @param inStart the start of the range in the parent RDD
 * @param outStart the start of the range in the child RDD
 * @param length the length of the range
 */
class RangeDependency[T](rdd: RDD[T], inStart: Int, outStart: Int, length: Int)
  extends NarrowDependency[T](rdd) {

  override def getParents(partitionId: Int) = {
    if (partitionId >= outStart && partitionId < outStart + length) {
      List(partitionId - outStart + inStart)
    } else {
      Nil
    }
  }
}
