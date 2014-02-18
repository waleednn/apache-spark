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

import java.text.SimpleDateFormat

import javax.servlet.http.HttpServletRequest

import org.eclipse.jetty.server.Handler

import org.apache.spark.ui.JettyUtils._
import org.apache.spark.SparkContext
import org.apache.spark.util.Utils

/** Web UI showing progress status of all jobs in the given SparkContext. */
private[spark] class JobProgressUI(val sc: SparkContext, fromDisk: Boolean = false) {
  private var _listener: Option[JobProgressListener] = None
  private val indexPage = new IndexPage(this, fromDisk)
  private val stagePage = new StagePage(this, fromDisk)
  private val poolPage = new PoolPage(this)

  val dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")

  def listener = _listener.get

  def start() {
    _listener = Some(new JobProgressListener(sc, fromDisk))
    if (!fromDisk) {
      sc.addSparkListener(listener)
    }
  }

  def formatDuration(ms: Long) = Utils.msDurationToString(ms)

  def getHandlers = Seq[(String, Handler)](
    ("/stages/stage", (request: HttpServletRequest) => stagePage.render(request)),
    ("/stages/pool", (request: HttpServletRequest) => poolPage.render(request)),
    ("/stages", (request: HttpServletRequest) => indexPage.render(request))
  )
}
