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

package org.apache.spark.sql.execution.streaming.state

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * An RDD that allows computations to be executed against [[StateStore]]s. It
 * uses the [[StateStoreCoordinator]] to use the locations of loaded state stores as
 * preferred locations.
 */
class StateStoreRDD[T: ClassTag, U: ClassTag](
    dataRDD: RDD[T],
    storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U],
    operatorId: Long,
    storeVersion: Long,
    storeDirectory: String,
    @transient private val storeCoordinator: Option[StateStoreCoordinator])
  extends RDD[U](dataRDD) {

  override protected def getPartitions: Array[Partition] = dataRDD.partitions

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val storeId = StateStoreId(operatorId, partition.index)
    storeCoordinator.flatMap(_.getLocation(storeId)).toSeq
  }

  override def compute(partition: Partition, ctxt: TaskContext): Iterator[U] = {
    var store: StateStore = null

    Utils.tryWithSafeFinally {
      val storeId = StateStoreId(operatorId, partition.index)
      store = StateStore.get(storeId, storeDirectory, storeVersion)
      val inputIter = dataRDD.compute(partition, ctxt)
      val outputIter = storeUpdateFunction(store, inputIter)
      assert(store.hasCommitted)
      outputIter
    } {
      if (store != null) store.cancel()
    }
  }
}
