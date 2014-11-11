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

package org.apache.spark.ui.jobs

import scala.xml.{Node, NodeSeq}

import javax.servlet.http.HttpServletRequest

import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.ui.jobs.UIData.JobUIData


/** Page showing list of all ongoing and recently finished jobs */
private[ui] class AllJobsPage(parent: JobsTab) extends WebUIPage("") {
  private val sc = parent.sc
  private val listener = parent.listener

  private def getSubmissionTime(job: JobUIData): Option[Long] = {
    for (
      firstStageId <- job.stageIds.headOption;
      firstStageInfo <- listener.stageIdToInfo.get(firstStageId);
      submitTime <- firstStageInfo.submissionTime
    ) yield submitTime
  }

  private def jobsTable(jobs: Seq[JobUIData]): Seq[Node] = {
    val columns: Seq[Node] = {
      <th>Job Id (Job Group)</th>
      <th>Description</th>
      <th>Submitted</th>
      <th>Duration</th>
      <th>Tasks: Succeeded/Total</th>
    }

    def makeRow(job: JobUIData): Seq[Node] = {
      val lastStageInfo = job.stageIds.lastOption.flatMap(listener.stageIdToInfo.get)
      val lastStageData = lastStageInfo.flatMap { s =>
        listener.stageIdToData.get((s.stageId, s.attemptId))
      }
      val lastStageName = lastStageInfo.map(_.name).getOrElse("(Unknown Stage Name)")
      val lastStageDescription = lastStageData.flatMap(_.description).getOrElse("")
      val duration: Option[Long] = {
        job.startTime.map { start =>
          val end = job.endTime.getOrElse(System.currentTimeMillis())
          end - start
        }
      }
      val formattedDuration = duration.map(d => UIUtils.formatDuration(d)).getOrElse("Unknown")
      val formattedSubmissionTime = job.startTime.map(UIUtils.formatDate).getOrElse("Unknown")
      val detailUrl =
        "%s/jobs/job?id=%s".format(UIUtils.prependBaseUri(parent.basePath), job.jobId)

      <tr>
        <td sorttable_customkey={job.jobId.toString}>
          {job.jobId} {job.jobGroup.map(id => s"($id)").getOrElse("")}
        </td>
        <td>
          <div><em>{lastStageDescription}</em></div>
          <a href={detailUrl}>{lastStageName}</a>
        </td>
        <td sorttable_customkey={job.startTime.getOrElse(-1).toString}>
          {formattedSubmissionTime}
        </td>
        <td sorttable_customkey={duration.getOrElse(-1).toString}>{formattedDuration}</td>
        <td class="progress-cell">
          {UIUtils.makeProgressBar(job.numActiveTasks, job.numCompletedTasks,
          job.numFailedTasks, job.numTasks)}
        </td>
      </tr>
    }

    <table class="table table-bordered table-striped table-condensed sortable">
      <thead>{columns}</thead>
      <tbody>
        {jobs.map(makeRow)}
      </tbody>
    </table>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      val activeJobs = listener.activeJobs.values.toSeq
      val completedJobs = listener.completedJobs.reverse.toSeq
      val failedJobs = listener.failedJobs.reverse.toSeq
      val now = System.currentTimeMillis

      val activeJobsTable =
        jobsTable(activeJobs.sortBy(getSubmissionTime(_).getOrElse(-1L)).reverse)
      val completedJobsTable =
        jobsTable(completedJobs.sortBy(getSubmissionTime(_).getOrElse(-1L)).reverse)
      val failedJobsTable =
        jobsTable(failedJobs.sortBy(getSubmissionTime(_).getOrElse(-1L)).reverse)

      val summary: NodeSeq =
        <div>
          <ul class="unstyled">
            {if (sc.isDefined) {
              // Total duration is not meaningful unless the UI is live
              <li>
                <strong>Total Duration: </strong>
                {UIUtils.formatDuration(now - sc.get.startTime)}
              </li>
            }}
            <li>
              <strong>Scheduling Mode: </strong>
              {listener.schedulingMode.map(_.toString).getOrElse("Unknown")}
            </li>
            <li>
              <a href="#active"><strong>Active Jobs:</strong></a>
              {activeJobs.size}
            </li>
            <li>
              <a href="#completed"><strong>Completed Jobs:</strong></a>
              {completedJobs.size}
            </li>
            <li>
              <a href="#failed"><strong>Failed Jobs:</strong></a>
              {failedJobs.size}
            </li>
          </ul>
        </div>

      val content = summary ++
        <h4 id="active">Active Jobs ({activeJobs.size})</h4> ++ activeJobsTable ++
        <h4 id="completed">Completed Jobs ({completedJobs.size})</h4> ++ completedJobsTable ++
        <h4 id ="failed">Failed Jobs ({failedJobs.size})</h4> ++ failedJobsTable

      UIUtils.headerSparkPage("Spark Jobs", content, parent)
    }
  }
}
