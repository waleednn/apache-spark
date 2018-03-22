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

import scala.util.control.NonFatal

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{DataWritingSparkTask, InternalRowDataWriterFactory}
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, SupportsWriteInternalRow, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter

case class ContinuousWriteExec(writer: StreamWriter, query: SparkPlan) extends SparkPlan {
  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val writerFactory = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new InternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val rdd = new ContinuousWriteRDD(query.execute(), writerFactory)
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")
    EpochCoordinatorRef.get(
      sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      sparkContext.env)
      .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

    try {
      // Force the RDD to run so continuous processing starts; no data is actually being collected
      // to the driver, as ContinuousWriteRDD outputs nothing.
      rdd.collect()
    } catch {
      case _: InterruptedException =>
      // Interruption is how continuous queries are ended, so accept and ignore the exception.
      case cause: Throwable =>
        logError(s"Data source writer $writer is aborting.")
        try {
          writer.abort(0, messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source writer $writer failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source writer $writer aborted.")
        cause match {
          // Do not wrap interruption exceptions that will be handled by streaming specially.
          case _ if StreamExecution.isInterruptionException(cause) => throw cause
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}
