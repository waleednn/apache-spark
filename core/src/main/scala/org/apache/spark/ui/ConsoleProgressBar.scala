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

package org.apache.spark.ui

import java.util.{Timer, TimerTask}

import org.apache.spark._

/**
 * ConsoleProgressBar shows the progress of stages in the next line of the console. It poll the
 * status of active stages from `sc.statusTracker` in every 200ms, the progress bar will be showed
 * up after the stage has ran at least 500ms. If multiple stages run in the same time, the status
 * of them will be combined together, showed in one line.
 */
private[spark] class ConsoleProgressBar(sc: SparkContext) extends Logging {

  // Update period of progress bar, in milli seconds
  val UPDATE_PERIOD = 200L
  // Delay to show up a progress bar, in milli seconds
  val DELAY_SHOW_UP = 500L

  // The width of terminal
  val TerminalWidth = if (!sys.env.getOrElse("COLUMNS", "").isEmpty) {
    sys.env.get("COLUMNS").get.toInt
  } else {
    80
  }

  var hasShowed = false
  var lastFinishTime = 0L

  // Schedule a refresh thread to run in every 200ms
  private val timer = new Timer("refresh progress", true)
  timer.schedule(new TimerTask{
    override def run() {
      refresh()
    }
  }, DELAY_SHOW_UP, UPDATE_PERIOD)

  /**
   * Try to refresh the progress bar in every cycle
   */
  private def refresh(): Unit = synchronized {
    val now = System.currentTimeMillis()
    if (now - lastFinishTime < DELAY_SHOW_UP) {
      return
    }
    var running = 0
    var finished = 0
    var tasks = 0
    var failed = 0
    val stageIds = sc.statusTracker.getActiveStageIds()
    stageIds.map(sc.statusTracker.getStageInfo).flatten.foreach { stage =>
      // Don't show progress for stage which has only one task (useless),
      // also don't show progress for stage which had started in 500 ms
      if (stage.numTasks > 1 && now - stage.submissionTime() > DELAY_SHOW_UP) {
        tasks += stage.numTasks()
        running += stage.numActiveTasks()
        finished += stage.numCompletedTasks()
        failed += stage.numFailedTasks()
      }
    }
    if (tasks > 0) {
      show(stageIds, tasks, running, finished, failed)
      hasShowed = true
    }
  }

  /**
   * Show progress bar in console. The progress bar is displayed in the next line
   * after your last output, keeps overwriting itself to hold in one line. The logging will follow
   * the progress bar, then progress bar will be showed in next line without overwrite logs.
   */
  private def show(stageIds: Seq[Int], total: Int, running: Int, finished: Int,
                              failed: Int): Unit = {
    val ids = stageIds.mkString(",")
    val header = s"Stage $ids: ["
    val tailer = s"] ($finished + $running) / $total"
    val width = TerminalWidth - header.size - tailer.size
    val percent = finished * width / total
    val bar = (0 until width).map { i =>
      if (i < percent) "=" else if (i == percent) ">" else " "
    }.mkString("")
    System.err.printf("\r" + header + bar + tailer)
  }

  /**
   * Clear the progress bar if showed.
   */
  private def clear() = {
    if (hasShowed) {
      System.err.printf("\r" + " " * TerminalWidth + "\r")
      hasShowed = false
    }
  }

  /**
   * Mark all the stages as finished, clear the progress bar if showed, then the progress will not
   * interwave with output of jobs.
   */
  def finishAll(): Unit = synchronized {
    clear()
    lastFinishTime = System.currentTimeMillis()
  }
}
