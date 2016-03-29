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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.SparkPlan

case class StateIdentifier(
    checkpointLocation: String,
    operatorId: Long,
    batchId: Long)

trait StatefulOperation extends SparkPlan {
  def stateId: Option[StateIdentifier]

  protected def getStateId: StateIdentifier = attachTree(this) {
    stateId.getOrElse {
      throw new IllegalStateException("State location not present for execution")
    }
  }
}

case class StateStoreRestore(
    keyExpressions: Seq[Attribute],
    stateId: Option[StateIdentifier],
    child: SparkPlan) extends execution.UnaryNode with StatefulOperation {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithStateStore(
      getStateId.checkpointLocation,
      operatorId = getStateId.operatorId,
      storeVersion = getStateId.batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      new StateStoreConf(sqlContext.conf),
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)
        iter.flatMap { row =>
          val key = getKey(row)
          val savedState = store.get(key)
          row +: savedState.toSeq
        }
    }
  }
  override def output: Seq[Attribute] = child.output
}

case class StateStoreSave(
    keyExpressions: Seq[Attribute],
    stateId: Option[StateIdentifier],
    child: SparkPlan) extends execution.UnaryNode with StatefulOperation {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsWithStateStore(
      getStateId.checkpointLocation,
      operatorId = getStateId.operatorId,
      storeVersion = getStateId.batchId,
      keyExpressions.toStructType,
      child.output.toStructType,
      new StateStoreConf(sqlContext.conf),
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        new Iterator[InternalRow] {
          private[this] val baseIterator = iter
          private[this] val getKey = GenerateUnsafeProjection.generate(keyExpressions, child.output)

          override def hasNext: Boolean = {
            if (!baseIterator.hasNext) {
              store.commit()
              false
            } else {
              true
            }
          }

          override def next(): InternalRow = {
            val row = baseIterator.next().asInstanceOf[UnsafeRow]
            val key = getKey(row)
            store.put(key.copy(), row.copy())
            row
          }
        }
    }
  }

  override def output: Seq[Attribute] = child.output
}