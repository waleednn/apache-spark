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

package org.apache.spark.sql.execution.datasources.v2

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.executor.CommitDeniedException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.sources.{AlwaysTrue, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, SupportsBatchWrite}
import org.apache.spark.sql.sources.v2.writer.{BatchWrite, DataWriterFactory, SupportsDynamicOverwrite, SupportsOverwrite, SupportsSaveMode, SupportsTruncate, WriteBuilder, WriterCommitMessage}
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * Deprecated logical plan for writing data into data source v2. This is being replaced by more
 * specific logical plans, like [[org.apache.spark.sql.catalyst.plans.logical.AppendData]].
 */
@deprecated("Use specific logical plans like AppendData instead", "2.4.0")
case class WriteToDataSourceV2(batchWrite: BatchWrite, query: LogicalPlan)
  extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
}

case class AppendDataExec(
    table: SupportsBatchWrite,
    writeOptions: DataSourceOptions,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder() match {
      case builder: SupportsSaveMode => // TODO: Remove this
        builder.mode(SaveMode.Append).buildForBatch()

      case builder =>
        builder.buildForBatch()
    }
    doWrite(batchWrite)
  }
}

case class OverwriteByExpressionExec(
    table: SupportsBatchWrite,
    filters: Array[Filter],
    writeOptions: DataSourceOptions,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder() match {
      case builder: SupportsTruncate if isTruncate(filters) =>
        builder.truncate().buildForBatch()

      case builder: SupportsOverwrite =>
        builder.overwrite(filters).buildForBatch()

      case builder: SupportsSaveMode => // TODO: Remove this
        builder.mode(SaveMode.Overwrite).buildForBatch()

      case _ =>
        throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
    }

    doWrite(batchWrite)
  }
}

case class OverwritePartitionsDynamicExec(
    table: SupportsBatchWrite,
    writeOptions: DataSourceOptions,
    query: SparkPlan) extends V2TableWriteExec with BatchWriteHelper {

  override protected def doExecute(): RDD[InternalRow] = {
    val batchWrite = newWriteBuilder() match {
      case builder: SupportsDynamicOverwrite =>
        builder.overwriteDynamicPartitions().buildForBatch()

      case builder: SupportsSaveMode => // TODO: Remove this
        builder.mode(SaveMode.Overwrite).buildForBatch()

      case _ =>
        throw new SparkException(s"Table does not support dynamic partition overwrite: $table")
    }

    doWrite(batchWrite)
  }
}

case class WriteToDataSourceV2Exec(
    batchWrite: BatchWrite,
    query: SparkPlan
  ) extends V2TableWriteExec {

  import DataSourceV2Implicits._

  def writeOptions: DataSourceOptions = Map.empty[String, String].toDataSourceOptions

  override protected def doExecute(): RDD[InternalRow] = {
    doWrite(batchWrite)
  }
}

/**
 * Helper for physical plans that build batch writes.
 */
private[sql] trait BatchWriteHelper {
  def table: SupportsBatchWrite
  def query: SparkPlan
  def writeOptions: DataSourceOptions

  def newWriteBuilder(): WriteBuilder = {
    table.newWriteBuilder(writeOptions)
        .withInputDataSchema(query.schema)
        .withQueryId(UUID.randomUUID().toString)
  }
}

/**
 * The base physical plan for writing data into data source v2.
 */
private[sql] trait V2TableWriteExec extends UnaryExecNode {
  def query: SparkPlan

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  protected def doWrite(batchWrite: BatchWrite): RDD[InternalRow] = {
    val writerFactory = batchWrite.createBatchWriterFactory()
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val rdd = query.execute()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      rdd
    }
    val messages = new Array[WriterCommitMessage](rddWithNonEmptyPartitions.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    try {
      sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writerFactory, context, iter, useCommitCoordinator),
        rddWithNonEmptyPartitions.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId)

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        // Count is here.
        count += 1
        dataWriter.write(iter.next())
      }

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId" +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val message = s"Commit denied for partition $partId (task $taskId, attempt $attemptId" +
            s"stage $stageId.$stageAttempt)"
          logInfo(message)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw new CommitDeniedException(message, stageId, partId, attemptId)
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId" +
        s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId" +
            s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId" +
            s"stage $stageId.$stageAttempt)")
    })
  }
}

private[v2] case class DataWritingSparkTaskResult(
                                                   numRows: Long,
                                                   writerCommitMessage: WriterCommitMessage)

/**
 * Sink progress information collected after commit.
 */
private[sql] case class StreamWriterCommitProgress(numOutputRows: Long)
