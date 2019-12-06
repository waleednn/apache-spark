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

package org.apache.spark.streaming.ui

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, Unparsed}

import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils => SparkUIUtils}

private[ui] abstract class BatchTableBase(tableId: String, batchInterval: Long) {

  protected def columns: Seq[Node] = {
    <th>Batch Time</th>
      <th>Records</th>
      <th>Scheduling Delay
        {SparkUIUtils.tooltip("Time taken by Streaming scheduler to submit jobs of a batch", "top")}
      </th>
      <th>Processing Time
        {SparkUIUtils.tooltip("Time taken to process all jobs of a batch", "top")}</th>
  }

  /**
   * Return the first failure reason if finding in the batches.
   */
  protected def getFirstFailureReason(batches: Seq[BatchUIData]): Option[String] = {
    batches.flatMap(_.outputOperations.flatMap(_._2.failureReason)).headOption
  }

  protected def getFirstFailureTableCell(batch: BatchUIData): Seq[Node] = {
    val firstFailureReason = batch.outputOperations.flatMap(_._2.failureReason).headOption
    firstFailureReason.map { failureReason =>
      val failureReasonForUI = UIUtils.createOutputOperationFailureForUI(failureReason)
      UIUtils.failureReasonCell(
        failureReasonForUI, rowspan = 1, includeFirstLineInExpandDetails = false)
    }.getOrElse(<td>-</td>)
  }

  protected def baseRow(batch: BatchUIData): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = UIUtils.formatBatchTime(batchTime, batchInterval)
    val numRecords = batch.numRecords
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(SparkUIUtils.formatDuration).getOrElse("-")
    val batchTimeId = s"batch-$batchTime"

    <td id={batchTimeId} sorttable_customkey={batchTime.toString}
        isFailed={batch.isFailed.toString}>
      <a href={s"batch?id=$batchTime"}>
        {formattedBatchTime}
      </a>
    </td>
      <td sorttable_customkey={numRecords.toString}>{numRecords.toString} records</td>
      <td sorttable_customkey={schedulingDelay.getOrElse(Long.MaxValue).toString}>
        {formattedSchedulingDelay}
      </td>
      <td sorttable_customkey={processingTime.getOrElse(Long.MaxValue).toString}>
        {formattedProcessingTime}
      </td>
  }

  private def batchTable: Seq[Node] = {
    <table id={tableId} class="table table-bordered table-striped table-condensed sortable">
      <thead>
        {columns}
      </thead>
      <tbody>
        {renderRows}
      </tbody>
    </table>
  }

  def toNodeSeq: Seq[Node] = {
    batchTable
  }

  protected def createOutputOperationProgressBar(batch: BatchUIData): Seq[Node] = {
    <td class="progress-cell">
      {
      SparkUIUtils.makeProgressBar(
        started = batch.numActiveOutputOp,
        completed = batch.numCompletedOutputOp,
        failed = batch.numFailedOutputOp,
        skipped = 0,
        reasonToNumKilled = Map.empty,
        total = batch.outputOperations.size)
      }
    </td>
  }

  /**
   * Return HTML for all rows of this table.
   */
  protected def renderRows: Seq[Node]
}

private[ui] class ActiveBatchTable(
    runningBatches: Seq[BatchUIData],
    waitingBatches: Seq[BatchUIData],
    batchInterval: Long) extends BatchTableBase("active-batches-table", batchInterval) {

  private val firstFailureReason = getFirstFailureReason(runningBatches)

  override protected def columns: Seq[Node] = super.columns ++ {
    <th>Output Ops: Succeeded/Total</th>
      <th>Status</th> ++ {
      if (firstFailureReason.nonEmpty) {
        <th>Error</th>
      } else {
        Nil
      }
    }
  }

  override protected def renderRows: Seq[Node] = {
    // The "batchTime"s of "waitingBatches" must be greater than "runningBatches"'s, so display
    // waiting batches before running batches
    waitingBatches.flatMap(batch => <tr>{waitingBatchRow(batch)}</tr>) ++
      runningBatches.flatMap(batch => <tr>{runningBatchRow(batch)}</tr>)
  }

  private def runningBatchRow(batch: BatchUIData): Seq[Node] = {
    baseRow(batch) ++ createOutputOperationProgressBar(batch) ++ <td>processing</td> ++ {
      if (firstFailureReason.nonEmpty) {
        getFirstFailureTableCell(batch)
      } else {
        Nil
      }
    }
  }

  private def waitingBatchRow(batch: BatchUIData): Seq[Node] = {
    baseRow(batch) ++ createOutputOperationProgressBar(batch) ++ <td>queued</td>++ {
      if (firstFailureReason.nonEmpty) {
        // Waiting batches have not run yet, so must have no failure reasons.
        <td>-</td>
      } else {
        Nil
      }
    }
  }
}

private[ui] class CompletedBatchPagedTable(
    request: HttpServletRequest,
    parent: StreamingTab,
    batchInterval: Long,
    data: Seq[BatchUIData],
    completedBatchTag: String,
    basePath: String,
    subPath: String,
    parameterOtherTable: Iterable[String],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedTable[BatchUIData] {

  override val dataSource = new CompletedBatchTableDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath = s"$basePath/$subPath/?${parameterOtherTable.mkString("&")}"

  private val firstFailureReason = getFirstFailureReason(data)

  override def tableId: String = completedBatchTag

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped " +
      "table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$completedBatchTag.sort=$encodedSortColumn" +
      s"&$completedBatchTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def pageSizeFormField: String = s"$completedBatchTag.pageSize"

  override def pageNumberFormField: String = s"$completedBatchTag.page"

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())
    s"$parameterPath&$completedBatchTag.sort=$encodedSortColumn&$completedBatchTag.desc=$desc"
  }

  override def headers: Seq[Node] = {
    val completedBatchTableHeaders = Seq("Batch Time", "Records", "Scheduling Delay",
      "Processing Delay", "Total Delay", "Output Ops: Succeeded/Total")

    val tooltips = Seq(None, None, Some("Time taken by Streaming scheduler to" +
      " submit jobs of a batch"), Some("Time taken to process all jobs of a batch"),
      Some("Total time taken to handle a batch"), None)

    assert(completedBatchTableHeaders.length == tooltips.length)

    val headerRow: Seq[Node] = {
      completedBatchTableHeaders.zip(tooltips).map { case (header, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$completedBatchTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$completedBatchTag.desc=${!desc}" +
              s"&$completedBatchTag.pageSize=$pageSize" +
              s"#$completedBatchTag")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          if (tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}&nbsp;{Unparsed(arrow)}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}&nbsp;{Unparsed(arrow)}
              </a>
            </th>
          }
        } else {
          val headerLink = Unparsed(
            parameterPath +
              s"&$completedBatchTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$completedBatchTag.pageSize=$pageSize" +
              s"#$completedBatchTag")

          if(tooltip.nonEmpty) {
            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" title={tooltip.get}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <a href={headerLink}>
                {header}
              </a>
            </th>
          }
        }
      }
    }
    <thead>
      {headerRow}
    </thead>
  }

  override def row(batch: BatchUIData): Seq[Node] = {
    val batchTime = batch.batchTime.milliseconds
    val formattedBatchTime = UIUtils.formatBatchTime(batchTime, batchInterval)
    val numRecords = batch.numRecords
    val schedulingDelay = batch.schedulingDelay
    val formattedSchedulingDelay = schedulingDelay.map(SparkUIUtils.formatDuration).getOrElse("-")
    val processingTime = batch.processingDelay
    val formattedProcessingTime = processingTime.map(SparkUIUtils.formatDuration).getOrElse("-")
    val batchTimeId = s"batch-$batchTime"
    val totalDelay = batch.totalDelay
    val formattedTotalDelay = totalDelay.map(SparkUIUtils.formatDuration).getOrElse("-")

    <tr>
      <td id={batchTimeId}>
        <a href={s"batch?id=$batchTime"}>
          {formattedBatchTime}
        </a>
      </td>
      <td>
        {numRecords.toString} records
      </td>
      <td>
        {formattedSchedulingDelay}
      </td>
      <td>
        {formattedProcessingTime}
      </td>
      <td>
        {formattedTotalDelay}
      </td>
      <td class="progress-cell">
        {SparkUIUtils.makeProgressBar(started = batch.numActiveOutputOp,
        completed = batch.numCompletedOutputOp, failed = batch.numFailedOutputOp, skipped = 0,
        reasonToNumKilled = Map.empty, total = batch.outputOperations.size)}
      </td>
      {
        if (firstFailureReason.nonEmpty) {
          getFirstFailureTableCell(batch)
        } else {
          Nil
        }
      }
    </tr>
  }

  protected def getFirstFailureReason(batches: Seq[BatchUIData]): Option[String] = {
    batches.flatMap(_.outputOperations.flatMap(_._2.failureReason)).headOption
  }

  protected def getFirstFailureTableCell(batch: BatchUIData): Seq[Node] = {
    val firstFailureReason = batch.outputOperations.flatMap(_._2.failureReason).headOption
    firstFailureReason.map { failureReason =>
      val failureReasonForUI = UIUtils.createOutputOperationFailureForUI(failureReason)
      UIUtils.failureReasonCell(
        failureReasonForUI, rowspan = 1, includeFirstLineInExpandDetails = false)
    }.getOrElse(<td>-</td>)
  }
}

private[ui] class CompletedBatchTableDataSource(
    info: Seq[BatchUIData],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[BatchUIData](pageSize) {

  private val data = info.sorted(ordering(sortColumn, desc))

  private var _slicedStartTime: Set[Long] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[BatchUIData] = {
    val r = data.slice(from, to)
    _slicedStartTime = r.map(_.batchTime.milliseconds).toSet
    r
  }

  /**
   * Return Ordering according to sortColumn and desc.
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[BatchUIData] = {
    val ordering: Ordering[BatchUIData] = sortColumn match {
      case "Batch Time" => Ordering.by(_.batchTime)
      case "Records" => Ordering.by(_.numRecords)
      case "Scheduling Delay" => Ordering.by(_.schedulingDelay)
      case "Processing Delay" => Ordering.by(_.processingDelay)
      case "Total Delay" => Ordering.by(_.totalDelay)
      case "Output Ops: Succeeded/Total" => Ordering.by(_.batchTime)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
