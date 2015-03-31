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

package org.apache.spark

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util.{TaskInterruptionListener, TaskInterruptionListenerException, TaskCompletionListener, TaskCompletionListenerException}

private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    val runningLocally: Boolean = false,
    val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  // For backwards-compatibility; this method is now deprecated as of 1.3.0.
  override def attemptId(): Long = taskAttemptId

  // List of callback functions to execute when interrupt the task.
  @transient private val onInterruptedCallbacks = new ArrayBuffer[TaskInterruptionListener]

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  override def addTaskInterruptionListener(listener: TaskInterruptionListener): this.type = {
    onInterruptedCallbacks += listener
    this
  }

  override def addTaskInterruptionListener(f: TaskContext => Unit): this.type = {
    onInterruptedCallbacks += new TaskInterruptionListener {
      override def onTaskInterrupted(context: TaskContext): Unit = f(context)
    }
    this
  }

  /** Marks the task as completed and triggers the listeners. */
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case NonFatal(e) =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task as interrupted and triggers the listeners. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process interruption callbacks in the reverse order of registration
    onInterruptedCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskInterrupted(this)
      } catch {
        case NonFatal(e) =>
          errorMsgs += e.getMessage
          logError("Error in TaskInterruptionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskInterruptionListenerException(errorMsgs)
    }
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = runningLocally

  override def isInterrupted(): Boolean = interrupted
}

