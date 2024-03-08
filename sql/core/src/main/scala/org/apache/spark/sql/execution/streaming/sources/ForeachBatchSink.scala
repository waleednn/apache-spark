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

package org.apache.spark.sql.execution.streaming.sources

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Deduplicate, DeduplicateWithinWatermark, Distinct, FlatMapGroupsInPandasWithState, FlatMapGroupsWithState, GlobalLimit, Join, LogicalPlan, TransformWithState}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.DataStreamWriter

class ForeachBatchSink[T](batchWriter: (Dataset[T], Long) => Unit, encoder: ExpressionEncoder[T])
  extends Sink with Logging {

  private def isQueryStateful(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan.collect {
      case node @ (_: Aggregate | _: Distinct | _: FlatMapGroupsWithState
                   | _: FlatMapGroupsInPandasWithState | _: TransformWithState | _: Deduplicate
                   | _: DeduplicateWithinWatermark | _: GlobalLimit) if node.isStreaming => node
      case node @ Join(left, right, _, _, _) if left.isStreaming && right.isStreaming => node
    }.nonEmpty
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val node = LogicalRDD.fromDataset(rdd = data.queryExecution.toRdd, originDataset = data,
      isStreaming = false)
    implicit val enc = encoder
    val ds = Dataset.ofRows(data.sparkSession, node).as[T]
    val isStateful = isQueryStateful(data.logicalPlan)
    if (isStateful) {
      ds.persist()
      callBatchWriter(ds, batchId)
      ds.unpersist()
    } else {
      callBatchWriter(ds, batchId)
    }
  }

  override def toString(): String = "ForeachBatchSink"

  private def callBatchWriter(ds: Dataset[T], batchId: Long): Unit = {
    try {
      batchWriter(ds, batchId)
    } catch {
      // The user code can throw any type of exception.
      case NonFatal(e) if !e.isInstanceOf[SparkThrowable] =>
        throw new SparkException(
          errorClass = "FOREACH_BATCH_USER_FUNCTION_ERROR",
          messageParameters = Map.empty,
          cause = e)
    }
  }
}

/**
 * Interface that is meant to be extended by Python classes via Py4J.
 * Py4J allows Python classes to implement Java interfaces so that the JVM can call back
 * Python objects. In this case, this allows the user-defined Python `foreachBatch` function
 * to be called from JVM when the query is active.
 * */
trait PythonForeachBatchFunction {
  /** Call the Python implementation of this function */
  def call(batchDF: DataFrame, batchId: Long): Unit
}

object PythonForeachBatchHelper {
  def callForeachBatch(dsw: DataStreamWriter[Row], pythonFunc: PythonForeachBatchFunction): Unit = {
    dsw.foreachBatch(pythonFunc.call _)
  }
}

