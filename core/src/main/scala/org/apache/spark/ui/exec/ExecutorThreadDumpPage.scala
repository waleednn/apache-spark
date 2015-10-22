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

package org.apache.spark.ui.exec

import java.net.URLDecoder
import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest

import scala.util.Try
import scala.xml.{Text, Node}

import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class ExecutorThreadDumpPage(parent: ExecutorsTab) extends WebUIPage("threadDump") {

  private val sc = parent.sc

  def render(request: HttpServletRequest): Seq[Node] = {
    val executorId = Option(request.getParameter("executorId")).map {
      executorId =>
        // Due to YARN-2844, "<driver>" in the url will be encoded to "%25253Cdriver%25253E" when
        // running in yarn-cluster mode. `request.getParameter("executorId")` will return
        // "%253Cdriver%253E". Therefore we need to decode it until we get the real id.
        var id = executorId
        var decodedId = URLDecoder.decode(id, "UTF-8")
        while (id != decodedId) {
          id = decodedId
          decodedId = URLDecoder.decode(id, "UTF-8")
        }
        id
    }.getOrElse {
      throw new IllegalArgumentException(s"Missing executorId parameter")
    }
    val time = System.currentTimeMillis()
    val maybeThreadDump = sc.get.getExecutorThreadDump(executorId)

    val content = maybeThreadDump.map { threadDump =>
     val dumpRows = threadDump.sortWith {
        case (threadTrace1, threadTrace2) => {
          val v1 = if (threadTrace1.threadName.contains("Executor task launch")) 1 else 0
          val v2 = if (threadTrace2.threadName.contains("Executor task launch")) 1 else 0
          if (v1 == v2) {
            threadTrace1.threadName.toLowerCase < threadTrace2.threadName.toLowerCase
          } else {
            v1 > v2
          }
        }
      }.map { thread =>
        val threadId = thread.threadId
        <tr id={s"thread_${threadId}_tr"} class="accordion-heading"
            onclick={s"toggleThreadStackTrace($threadId, false)"}
            onmouseover={s"onMouseOverAndOut($threadId)"}
            onmouseout={s"onMouseOverAndOut($threadId)"}>
          <td id={s"${threadId}_td_id"}>{threadId}</td>
          <td id={s"${threadId}_td_name"}>{thread.threadName}</td>
          <td id={s"${threadId}_td_state"}>{thread.threadState}</td>
          <td id={s"${threadId}_td_stacktrace"} class="hidden">{thread.stackTrace}</td>
        </tr>
      }

    <div class="row-fluid">
      <p>Updated at {UIUtils.formatDate(time)}</p>
      {
        // scalastyle:off
        <p><a class="expandbutton" onClick="expandOrCollapseAllThreadStackTrace(true, true)">
          Expand All
        </a></p>
        <p><a class="expandbutton hidden" onClick="expandOrCollapseAllThreadStackTrace(false, true)">
          Collapse All
        </a></p>
        <div class="form-inline">
        <div class="bs-example" data-example-id="simple-form-inline">
          <div class="form-group">
            <div class="input-group">
              Search: <input type="text" class="form-control" id="search" oninput="onSearchStringChange()"></input>
            </div>
          </div>
        </div>
        </div>
        <p></p>
        // scalastyle:on
      }
      <table class={UIUtils.TABLE_CLASS_STRIPED + " accordion-group" + " sortable"}>
        <thead>
          <th onClick="expandOrCollapseAllThreadStackTrace(false, false)">Thread ID</th>
          <th onClick="expandOrCollapseAllThreadStackTrace(false, false)">Thread Name</th>
          <th onClick="expandOrCollapseAllThreadStackTrace(false, false)">Thread State</th>
        </thead>
        <tbody>{dumpRows}</tbody>
      </table>
    </div>
    }.getOrElse(Text("Error fetching thread dump"))
    UIUtils.headerSparkPage(s"Thread dump for executor $executorId", content, parent)
  }
}
