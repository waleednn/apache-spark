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

package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.types.DataType

private class ShuffledRowRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
  override def hashCode(): Int = idx
}

private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 */
class ShuffledRowRDD[InternalRow: ClassTag](
    rowSchema: Array[DataType],
    @transient var prev: RDD[Product2[Int, InternalRow]],
    serializer: Serializer,
    numPartitions: Int)
  extends RDD[InternalRow](prev.context, Nil) {

  private val part: Partitioner = new PartitionIdPassthrough(numPartitions)

  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency[Int, InternalRow, InternalRow](prev, part, Some(serializer)))
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRowRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[Int, InternalRow, InternalRow]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[Product2[Int, InternalRow]]]
      .map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
