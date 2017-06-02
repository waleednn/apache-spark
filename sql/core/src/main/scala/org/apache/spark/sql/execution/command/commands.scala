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

package org.apache.spark.sql.execution.command

import scala.collection.mutable

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.ExecutedWriteSummary
import org.apache.spark.sql.execution.debug._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.{IncrementalExecution, OffsetSeqMetadata}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

/**
 * A logical command specialized for writing data out. `WriteOutFileCommand`s are
 * wrapped in `WrittenFileCommandExec` during execution.
 */
trait WriteOutFileCommand extends logical.Command {

  /**
   * Those metrics will be updated once the command finishes writing data out. Those metrics will
   * be taken by `WrittenFileCommandExe` as its metrics when showing in UI.
   */
  def metrics(sqlContext: SQLContext): Map[String, SQLMetric] = {
    val sparkContext = sqlContext.sparkContext

    Map(
      // General metrics.
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      "numDynamicParts" -> SQLMetrics.createMetric(sparkContext, "number of dynamic part"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      "numOutputBytes" -> SQLMetrics.createMetric(sparkContext, "bytes of written output"),
      "writingTime" -> SQLMetrics.createMetric(sparkContext, "average writing time (ms)"),
      // Detailed metrics per file.
      "minOutputRowsPerFile" ->
        SQLMetrics.createMetric(sparkContext, "minimum number of output rows per file"),
      "maxOutputRowsPerFile" ->
        SQLMetrics.createMetric(sparkContext, "maximum number of output rows per file"),
      "medOutputRowsPerFile" ->
        SQLMetrics.createMetric(sparkContext, "median number of output rows per file"),
      "minOutputBytesPerFile" ->
        SQLMetrics.createMetric(sparkContext, "minimum written bytes per file"),
      "maxOutputBytesPerFile" ->
        SQLMetrics.createMetric(sparkContext, "maximum written bytes per file"),
      "medOutputBytesPerFile" ->
        SQLMetrics.createMetric(sparkContext, "median written bytes per file"),
      // Detailed metrics per partition.
      "minOutputRowsPerPart" ->
        SQLMetrics.createMetric(sparkContext, "minimum number of output rows per partition"),
      "maxOutputRowsPerPart" ->
        SQLMetrics.createMetric(sparkContext, "maximum number of output rows per partition"),
      "medOutputRowsPerPart" ->
        SQLMetrics.createMetric(sparkContext, "median number of output rows per partition"),
      "minOutputBytesPerPart" ->
        SQLMetrics.createMetric(sparkContext, "minimum written bytes per partition"),
      "maxOutputBytesPerPart" ->
        SQLMetrics.createMetric(sparkContext, "maximum written bytes per partition"),
      "medOutputBytesPerPart" ->
        SQLMetrics.createMetric(sparkContext, "median written bytes per partition")
    )
  }

  def run(
      sparkSession: SparkSession,
      children: Seq[SparkPlan],
      metricsCallback: (Seq[ExecutedWriteSummary]) => Unit): Seq[Row] = {
    throw new NotImplementedError
  }
}

/**
 * A logical command that is executed for its side-effects.  `RunnableCommand`s are
 * wrapped in `ExecutedCommand` during execution.
 */
trait RunnableCommand extends logical.Command {
  def run(sparkSession: SparkSession): Seq[Row] = {
    throw new NotImplementedError
  }
}

/**
 * A physical operator that executes the run method of a `logical.Command` and
 * saves the result to prevent multiple executions.
 */
trait CommandExec extends SparkPlan {
  val cmd: logical.Command

  /**
   * A concrete command should override this lazy field to wrap up any side effects caused by the
   * command or any other computation that should be evaluated exactly once. The value of this field
   * can be used as the contents of the corresponding RDD generated from the physical plan of this
   * command.
   *
   * The `execute()` method of all the physical command classes should reference `sideEffectResult`
   * so that the command can be executed eagerly right after the command query is created.
   */
  protected[sql] val sideEffectResult: Seq[InternalRow]

  override def innerChildren: Seq[QueryPlan[_]] = cmd.innerChildren

  override def output: Seq[Attribute] = cmd.output

  override def nodeName: String = cmd.nodeName

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  override def executeToIterator: Iterator[InternalRow] = sideEffectResult.toIterator

  override def executeTake(limit: Int): Array[InternalRow] = sideEffectResult.take(limit).toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

/**
 * A physical operator specialized to execute the run method of a `WriteOutFileCommand`,
 * save the result to prevent multiple executions, and record necessary metrics for UI.
 */
case class WrittenFileCommandExec(
    cmd: WriteOutFileCommand,
    children: Seq[SparkPlan]) extends CommandExec {

  override lazy val metrics = cmd.metrics(sqlContext)

  /**
   * A helper function used to compute median/minimum/maximum values and update metrics based
   * on the given metric name.
   */
  private def setMetrics(values: Seq[Long], metricName: String): Seq[SQLMetric] = {
    if (values.nonEmpty) {
      val sorted = values.sorted
      val metricValues = Seq(sorted(0), sorted(values.length / 2), sorted(values.length - 1))
      Seq("min", "med", "max").zip(metricValues).map { case (prefix, metricValue) =>
        val metric = metrics(prefix + metricName)
        metric.add(metricValue)
        metric
      }
    } else {
      Seq.empty
    }
  }

  /**
   * The callback function used to update metrics returned from the operation of writing data out.
   */
  private def updateDriverMetrics(writeSummaries: Seq[ExecutedWriteSummary]): Unit = {
    var numPartitions = 0
    var numFiles = 0

    val (numBytesPerFilePart, numOutputRowsPerFilePart) = writeSummaries.map { summary =>
      numPartitions += summary.updatedPartitions.size
      numFiles += summary.writtenFileNum

      (summary.writtenBytesPerPart.flatten, summary.numOutputRowsPerPart.flatten)
    }.unzip

    val numBytesPerFile = numBytesPerFilePart.flatten
    val numOutputRowsPerFile = numOutputRowsPerFilePart.flatten

    val totalNumBytes: Long = numBytesPerFile.sum
    val totalNumOutput: Long = numOutputRowsPerFile.sum

    // Computes number of bytes/rows per file.
    val numBytesPerFileMetrics = setMetrics(numBytesPerFile, "OutputBytesPerFile")
    val numOutputsPerFileMetrics = setMetrics(numOutputRowsPerFile, "OutputRowsPerFile")

    // Computes number of bytes/rows per partition.
    val numBytesPerPart = numBytesPerFilePart.map(_.sum)
    val numOutputRowsPerPart = numOutputRowsPerFilePart.map(_.sum)

    val numBytesPerPartMetrics = setMetrics(numBytesPerPart, "OutputBytesPerPart")
    val numOutputsPerPartMetrics = setMetrics(numOutputRowsPerPart, "OutputRowsPerPart")

    // Metrics of writing time in ms.
    // We only count for the non-zero writing time when computing average.
    val writingTimePerFile: Seq[Long] = writeSummaries.flatMap(_.writingTimePerPart.flatten)
    val nonZeroCount = writingTimePerFile.filter(_ > 0).size
    val avgWritingTime = if (nonZeroCount == 0) {
      0
    } else {
      writingTimePerFile.sum / nonZeroCount
    }

    // Updates metrics.
    val numDynamicPartsMetric = metrics("numDynamicParts")
    val fileNumMetric = metrics("numFiles")
    val numBytesMetric = metrics("numOutputBytes")
    val numOutputRowsMetric = metrics("numOutputRows")
    val writingTimeMetric = metrics("writingTime")

    numDynamicPartsMetric.add(numPartitions)
    fileNumMetric.add(numFiles)
    numBytesMetric.add(totalNumBytes)
    numOutputRowsMetric.add(totalNumOutput)
    writingTimeMetric.add(avgWritingTime)

    val generalMetrics = Seq(numDynamicPartsMetric, fileNumMetric, numBytesMetric,
      numOutputRowsMetric, writingTimeMetric)
    val metricsPerFile = numBytesPerFileMetrics ++ numOutputsPerFileMetrics
    val metricsPerPart = numBytesPerPartMetrics ++ numOutputsPerPartMetrics

    val finalMetrics = if (numPartitions == 0) {
      // For non-dynamic partition, we don't need to update the metrics per partition.
      generalMetrics ++ metricsPerFile
    } else {
      generalMetrics ++ metricsPerFile ++ metricsPerPart
    }

    val executionId = sqlContext.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sqlContext.sparkContext, executionId, finalMetrics)
  }

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(sqlContext.sparkSession, children, updateDriverMetrics)
    rows.map(converter(_).asInstanceOf[InternalRow])
  }
}

/**
 * A physical operator specialized to execute the run method of a `RunnableCommand` and
 * save the result to prevent multiple executions.
 */
case class ExecutedCommandExec(cmd: RunnableCommand) extends CommandExec {
  override protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val rows = cmd.run(sqlContext.sparkSession)
    rows.map(converter(_).asInstanceOf[InternalRow])
  }

  override def children: Seq[SparkPlan] = Nil
}

/**
 * An explain command for users to see how a command will be executed.
 *
 * Note that this command takes in a logical plan, runs the optimizer on the logical plan
 * (but do NOT actually execute it).
 *
 * {{{
 *   EXPLAIN (EXTENDED | CODEGEN) SELECT * FROM ...
 * }}}
 *
 * @param logicalPlan plan to explain
 * @param extended whether to do extended explain or not
 * @param codegen whether to output generated code from whole-stage codegen or not
 * @param cost whether to show cost information for operators.
 */
case class ExplainCommand(
    logicalPlan: LogicalPlan,
    extended: Boolean = false,
    codegen: Boolean = false,
    cost: Boolean = false)
  extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val queryExecution =
      if (logicalPlan.isStreaming) {
        // This is used only by explaining `Dataset/DataFrame` created by `spark.readStream`, so the
        // output mode does not matter since there is no `Sink`.
        new IncrementalExecution(
          sparkSession, logicalPlan, OutputMode.Append(), "<unknown>", 0, OffsetSeqMetadata(0, 0))
      } else {
        sparkSession.sessionState.executePlan(logicalPlan)
      }
    val outputString =
      if (codegen) {
        codegenString(queryExecution.executedPlan)
      } else if (extended) {
        queryExecution.toString
      } else if (cost) {
        queryExecution.toStringWithStats
      } else {
        queryExecution.simpleString
      }
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}

/** An explain command for users to see how a streaming batch is executed. */
case class StreamingExplainCommand(
    queryExecution: IncrementalExecution,
    extended: Boolean) extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("plan", StringType, nullable = true)())

  // Run through the optimizer to generate the physical plan.
  override def run(sparkSession: SparkSession): Seq[Row] = try {
    val outputString =
      if (extended) {
        queryExecution.toString
      } else {
        queryExecution.simpleString
      }
    Seq(Row(outputString))
  } catch { case cause: TreeNodeException[_] =>
    ("Error occurred during query planning: \n" + cause.getMessage).split("\n").map(Row(_))
  }
}
