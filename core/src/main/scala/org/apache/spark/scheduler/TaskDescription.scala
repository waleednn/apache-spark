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

package org.apache.spark.scheduler

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, Map}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.TaskNotSerializableException
import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

/**
 * Description of a task that gets passed onto executors to be executed, usually created by
 * `TaskSetManager.resourceOffer`.
 *
 * TaskDescriptions and the associated Task need to be serialized carefully for two reasons:
 *
 *     (1) When a TaskDescription is received by an Executor, the Executor needs to first get the
 *         list of JARs and files and add these to the classpath, and set the properties, before
 *         deserializing the Task object (serializedTask). This is why the Properties are included
 *         in the TaskDescription, even though they're also in the serialized task.
 *     (2) Because a TaskDescription is serialized and sent to an executor for each task, efficient
 *         serialization (both in terms of serialization time and serialized buffer size) is
 *         important. For this reason, we serialize TaskDescriptions ourselves with the
 *         TaskDescription.encode and TaskDescription.decode methods.  This results in a smaller
 *         serialized size because it avoids serializing unnecessary fields in the Map objects
 *         (which can introduce significant overhead when the maps are small).
 */
private[spark] class TaskDescription(
    val taskId: Long,
    val attemptNumber: Int,
    val executorId: String,
    val name: String,
    val index: Int,    // Index within this task's TaskSet
    val addedFiles: Map[String, Long],
    val addedJars: Map[String, Long],
    val properties: Properties,
    private var serializedTask_ : ByteBuffer) extends  Logging {

  // Only be called in driver
  def this(
      taskId: Long,
      attemptNumber: Int,
      executorId: String,
      name: String,
      index: Int, // Index within this task's TaskSet
      addedFiles: Map[String, Long],
      addedJars: Map[String, Long],
      properties: Properties,
      task: Task[_]) {
      this(taskId, attemptNumber, executorId, name, index,
        addedFiles, addedJars, properties, null.asInstanceOf[ByteBuffer])
      task_ = task
  }

  // This is only used on the driver to pass the Task object between various scheduler components.
  @transient private var task_ : Task[_] = null

  def serializedTask: ByteBuffer = {
    if (serializedTask_ == null) {
      // This is where we serialize the task on the driver before sending it to the executor.
      // This is not done when creating the TaskDescription so we can postpone this serialization
      // to later in the scheduling process -- particularly,
      // so it can happen in another thread by the CoarseGrainedSchedulerBackend.
      // On the executors, this will already be populated by decode
      serializedTask_ = try {
        ByteBuffer.wrap(Utils.serialize(task_))
      } catch {
        case NonFatal(e) =>
          val msg = s"Failed to serialize task $taskId, not attempting to retry it."
          logError(msg, e)
          throw new TaskNotSerializableException(e)
      }
    }
    serializedTask_
  }

  override def toString: String = "TaskDescription(TID=%d, index=%d)".format(taskId, index)
}

private[spark] object TaskDescription {
  private def serializeStringLongMap(map: Map[String, Long], dataOut: DataOutputStream): Unit = {
    dataOut.writeInt(map.size)
    for ((key, value) <- map) {
      dataOut.writeUTF(key)
      dataOut.writeLong(value)
    }
  }

  @throws[TaskNotSerializableException]
  def encode(taskDescription: TaskDescription): ByteBuffer = {
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)

    dataOut.writeLong(taskDescription.taskId)
    dataOut.writeInt(taskDescription.attemptNumber)
    dataOut.writeUTF(taskDescription.executorId)
    dataOut.writeUTF(taskDescription.name)
    dataOut.writeInt(taskDescription.index)

    // Write files.
    serializeStringLongMap(taskDescription.addedFiles, dataOut)

    // Write jars.
    serializeStringLongMap(taskDescription.addedJars, dataOut)

    // Write properties.
    dataOut.writeInt(taskDescription.properties.size())
    taskDescription.properties.asScala.foreach { case (key, value) =>
      dataOut.writeUTF(key)
      dataOut.writeUTF(value)
    }

    // Write the task. The task is already serialized, so write it directly to the byte buffer.
    Utils.writeByteBuffer(taskDescription.serializedTask, bytesOut)

    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
  }

  private def deserializeStringLongMap(dataIn: DataInputStream): HashMap[String, Long] = {
    val map = new HashMap[String, Long]()
    val mapSize = dataIn.readInt()
    for (i <- 0 until mapSize) {
      map(dataIn.readUTF()) = dataIn.readLong()
    }
    map
  }

  def decode(byteBuffer: ByteBuffer): TaskDescription = {
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val taskId = dataIn.readLong()
    val attemptNumber = dataIn.readInt()
    val executorId = dataIn.readUTF()
    val name = dataIn.readUTF()
    val index = dataIn.readInt()

    // Read files.
    val taskFiles = deserializeStringLongMap(dataIn)

    // Read jars.
    val taskJars = deserializeStringLongMap(dataIn)

    // Read properties.
    val properties = new Properties()
    val numProperties = dataIn.readInt()
    for (i <- 0 until numProperties) {
      properties.setProperty(dataIn.readUTF(), dataIn.readUTF())
    }

    // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
    val serializedTask = byteBuffer.slice()

    new TaskDescription(taskId, attemptNumber, executorId, name, index, taskFiles, taskJars,
      properties, serializedTask)
  }
}
