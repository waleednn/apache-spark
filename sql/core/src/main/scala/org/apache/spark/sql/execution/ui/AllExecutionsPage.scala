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

package org.apache.spark.sql.execution.ui

import javax.servlet.http.HttpServletRequest

import scala.collection.mutable
import scala.xml.{NodeSeq, Node}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class AllExecutionsPage(parent: SQLTab) extends WebUIPage("") with Logging {

  private val listener = parent.listener

  override def render(request: HttpServletRequest): Seq[Node] = {
    val currentTime = System.currentTimeMillis()
    val content = listener.synchronized {
      val _content = mutable.ListBuffer[Node]()
      if (listener.getRunningExecutions.nonEmpty) {
        _content ++=
          new RunningExecutionTable(
            parent, "Running Queries (" + listener.getRunningExecutions.size + ")", currentTime,
            listener.getRunningExecutions.sortBy(_.submissionTime).reverse).toNodeSeq
      }
      if (listener.getCompletedExecutions.nonEmpty) {
        _content ++=
          new CompletedExecutionTable(
            parent, "Completed Queries (" + listener.getCompletedExecutions.size + ")", currentTime,
            listener.getCompletedExecutions.sortBy(_.submissionTime).reverse).toNodeSeq
      }
      if (listener.getFailedExecutions.nonEmpty) {
        _content ++=
          new FailedExecutionTable(
            parent, "Failed Queries (" + listener.getFailedExecutions.size + ")", currentTime,
            listener.getFailedExecutions.sortBy(_.submissionTime).reverse).toNodeSeq
      }
      _content
    }
    content ++=
      <script>
        function clickDetail(details) {{
          details.parentNode.querySelector('.stage-details').classList.toggle('collapsed')
        }}
      </script>

    val summary: NodeSeq =
      <div>
        <ul class="unstyled">
          {
          if (listener.getRunningExecutions.nonEmpty) {
            <li>
              <a href="#running-execution-table"><strong>Running Queries:</strong></a>
              {listener.getRunningExecutions.size}
            </li>
          }
          }
          {
          if (listener.getCompletedExecutions.nonEmpty) {
            <li id="completed-summary">
              <a href="#completed-execution-table"><strong>Completed Queries:</strong></a>
              {listener.getCompletedExecutions.size}
            </li>
          }
          }
          {
          if (listener.getFailedExecutions.nonEmpty) {
            <li>
              <a href="#failed-execution-table"><strong>Failed Queries:</strong></a>
              {listener.getFailedExecutions.size}
            </li>
          }
          }
        </ul>
      </div>
    
    UIUtils.headerSparkPage("SQL", summary ++ content, parent, Some(5000))
  }
}

private[ui] abstract class ExecutionTable(
    parent: SQLTab,
    tableId: String,
    tableName: String,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData],
    showRunningJobs: Boolean,
    showSucceededJobs: Boolean,
    showFailedJobs: Boolean) {

  protected def baseHeader: Seq[String] = Seq(
    "ID",
    "Description",
    "Submitted",
    "Duration")

  protected def header: Seq[String]

  protected def row(currentTime: Long, executionUIData: SQLExecutionUIData): Seq[Node] = {
    val submissionTime = executionUIData.submissionTime
    val duration = executionUIData.completionTime.getOrElse(currentTime) - submissionTime

    val runningJobs = executionUIData.runningJobs.map { jobId =>
      <a href={jobURL(jobId)}>{jobId.toString}</a><br/>
    }
    val succeededJobs = executionUIData.succeededJobs.sorted.map { jobId =>
      <a href={jobURL(jobId)}>{jobId.toString}</a><br/>
    }
    val failedJobs = executionUIData.failedJobs.sorted.map { jobId =>
      <a href={jobURL(jobId)}>{jobId.toString}</a><br/>
    }
    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(executionUIData)}
      </td>
      <td sorttable_customkey={submissionTime.toString}>
        {UIUtils.formatDate(submissionTime)}
      </td>
      <td sorttable_customkey={duration.toString}>
        {UIUtils.formatDuration(duration)}
      </td>
      {if (showRunningJobs) {
        <td>
          {runningJobs}
        </td>
      }}
      {if (showSucceededJobs) {
        <td>
          {succeededJobs}
        </td>
      }}
      {if (showFailedJobs) {
        <td>
          {failedJobs}
        </td>
      }}
    </tr>
  }

  private def descriptionCell(execution: SQLExecutionUIData): Seq[Node] = {
    val details = if (execution.details.nonEmpty) {
      <span onclick="clickDetail(this)" class="expand-details">
        +details
      </span> ++
      <div class="stage-details collapsed">
        <pre>{execution.details}</pre>
      </div>
    } else {
      Nil
    }

    val desc = {
      <a href={executionURL(execution.executionId)}>{execution.description}</a>
    }

    <div>{desc} {details}</div>
  }

  def toNodeSeq: Seq[Node] = {
    <div>
      <h4>{tableName}</h4>
      {UIUtils.listingTable[SQLExecutionUIData](
        header, row(currentTime, _), executionUIDatas, id = Some(tableId))}
    </div>
  }

  private def jobURL(jobId: Long): String =
    "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), jobId)

  private def executionURL(executionID: Long): String =
    s"${UIUtils.prependBaseUri(parent.basePath)}/${parent.prefix}/execution?id=$executionID"
}

private[ui] class RunningExecutionTable(
    parent: SQLTab,
    tableName: String,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "running-execution-table",
    tableName,
    currentTime,
    executionUIDatas,
    showRunningJobs = true,
    showSucceededJobs = true,
    showFailedJobs = true) {

  override protected def header: Seq[String] =
    baseHeader ++ Seq("Running Jobs", "Succeeded Jobs", "Failed Jobs")
}

private[ui] class CompletedExecutionTable(
    parent: SQLTab,
    tableName: String,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "completed-execution-table",
    tableName,
    currentTime,
    executionUIDatas,
    showRunningJobs = false,
    showSucceededJobs = true,
    showFailedJobs = false) {

  override protected def header: Seq[String] = baseHeader ++ Seq("Jobs")
}

private[ui] class FailedExecutionTable(
    parent: SQLTab,
    tableName: String,
    currentTime: Long,
    executionUIDatas: Seq[SQLExecutionUIData])
  extends ExecutionTable(
    parent,
    "failed-execution-table",
    tableName,
    currentTime,
    executionUIDatas,
    showRunningJobs = false,
    showSucceededJobs = true,
    showFailedJobs = true) {

  override protected def header: Seq[String] =
    baseHeader ++ Seq("Succeeded Jobs", "Failed Jobs")
}
