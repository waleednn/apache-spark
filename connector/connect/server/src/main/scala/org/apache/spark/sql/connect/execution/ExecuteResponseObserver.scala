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

package org.apache.spark.sql.connect.execution

import java.util.UUID

import scala.collection.mutable

import com.google.protobuf.MessageLite
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.service.ExecuteHolder

/**
 * This StreamObserver is running on the execution thread. Execution pushes responses to it, it
 * caches them. ExecuteResponseGRPCSender is the consumer of the responses ExecuteResponseObserver
 * "produces". It waits on the monitor of ExecuteResponseObserver. New produced responses notify
 * the monitor.
 * @see
 *   getResponse.
 *
 * ExecuteResponseObserver controls how responses stay cached after being returned to consumer,
 * @see
 *   removeCachedResponses.
 *
 * A single ExecuteResponseGRPCSender can be attached to the ExecuteResponseObserver. Attaching a
 * new one will notify an existing one that it was detached.
 * @see
 *   attachConsumer
 */
private[connect] class ExecuteResponseObserver[T <: MessageLite](val executeHolder: ExecuteHolder)
    extends StreamObserver[T]
    with Logging {

  /**
   * Cached responses produced by the execution. Map from response index -> response. Response
   * indexes are numbered consecutively starting from 1.
   */
  private val responses: mutable.Map[Long, CachedStreamResponse[T]] =
    new mutable.HashMap[Long, CachedStreamResponse[T]]()

  private val responseIndexToId: mutable.Map[Long, String] = new mutable.HashMap[Long, String]()

  private val responseIdToIndex: mutable.Map[String, Long] = new mutable.HashMap[String, Long]()

  /** Cached error of the execution, if an error was thrown. */
  private var error: Option[Throwable] = None

  /**
   * If execution stream is finished (completed or with error), the index of the final response.
   */
  private var finalProducedIndex: Option[Long] = None // index of final response before completed.

  /** The index of the last response produced by execution. */
  private var lastProducedIndex: Long = 0 // first response will have index 1

  /**
   * Highest response index that was consumed. Keeps track of it to decide which responses needs
   * to be cached, and to assert that all responses are consumed.
   */
  private var highestConsumedIndex: Long = 0

  /**
   * Consumer that waits for available responses. There can be only one at a time, @see
   * attachConsumer.
   */
  private var responseSender: Option[ExecuteGrpcResponseSender[T]] = None

  def onNext(r: T): Unit = synchronized {
    if (finalProducedIndex.nonEmpty) {
      throw new IllegalStateException("Stream onNext can't be called after stream completed")
    }
    lastProducedIndex += 1
    val processedResponse = setCommonResponseFields(r)
    responses +=
      ((lastProducedIndex, CachedStreamResponse[T](processedResponse, lastProducedIndex)))
    responseIndexToId += ((lastProducedIndex, getResponseId(r)))
    responseIdToIndex += ((getResponseId(r), lastProducedIndex))
    logDebug(s"Saved response with index=$lastProducedIndex")
    notifyAll()
  }

  def onError(t: Throwable): Unit = synchronized {
    if (finalProducedIndex.nonEmpty) {
      throw new IllegalStateException("Stream onError can't be called after stream completed")
    }
    error = Some(t)
    finalProducedIndex = Some(lastProducedIndex) // no responses to be send after error.
    logDebug(s"Error. Last stream index is $lastProducedIndex.")
    notifyAll()
  }

  def onCompleted(): Unit = synchronized {
    if (finalProducedIndex.nonEmpty) {
      throw new IllegalStateException("Stream onCompleted can't be called after stream completed")
    }
    finalProducedIndex = Some(lastProducedIndex)
    logDebug(s"Completed. Last stream index is $lastProducedIndex.")
    notifyAll()
  }

  /** Attach a new consumer (ExecuteResponseGRPCSender). */
  def attachConsumer(newSender: ExecuteGrpcResponseSender[T]): Unit = synchronized {
    // detach the current sender before attaching new one
    // this.synchronized() needs to be held while detaching a sender, and the detached sender
    // needs to be notified with notifyAll() afterwards.
    responseSender.foreach(_.detach())
    responseSender = Some(newSender)
    notifyAll() // consumer
  }

  /** Get response with a given index in the stream, if set. */
  def getResponse(index: Long): Option[CachedStreamResponse[T]] = synchronized {
    // we index stream responses from 1, getting a lower index would be invalid.
    assert(index >= 1)
    // it would be invalid if consumer would skip a response
    assert(index <= highestConsumedIndex + 1)
    val ret = responses.get(index)
    if (ret.isDefined) {
      if (index > highestConsumedIndex) highestConsumedIndex = index
      removeCachedResponses()
    } else if (index <= highestConsumedIndex) {
      // If index is <= highestConsumedIndex and not available, it was already removed from cache.
      // This may happen if ReattachExecute is too late and the cached response was evicted.
      val responseId = responseIndexToId.getOrElse(index, "<UNKNOWN>")
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.POSITION_NOT_AVAILABLE",
        messageParameters = Map("index" -> index.toString, "responseId" -> responseId))
    } else if (getLastIndex.forall(index > _)) {
      // If index > lastIndex, it's out of bounds. This is an internal error.
      throw new IllegalStateException(
        s"Cursor position $index is beyond last index $getLastIndex.")
    }
    ret
  }

  /** Get the stream error if there is one, otherwise None. */
  def getError(): Option[Throwable] = synchronized {
    error
  }

  /** If the stream is finished, the index of the last response, otherwise None. */
  def getLastIndex(): Option[Long] = synchronized {
    finalProducedIndex
  }

  /** Returns if the stream is finished. */
  def completed(): Boolean = synchronized {
    finalProducedIndex.isDefined
  }

  /** Get the index in the stream for ginen response id. */
  def getIndexById(responseId: String): Long = {
    responseIdToIndex.getOrElse(
      responseId,
      throw new SparkSQLException(
        errorClass = "INVALID_CURSOR.POSITION_NOT_FOUND",
        messageParameters = Map("responseId" -> responseId)))
  }

  /** Consumer (ExecuteResponseGRPCSender) waits on the monitor of ExecuteResponseObserver. */
  private def notifyConsumer(): Unit = {
    notifyAll()
  }

  /**
   * Remove cached responses after response with lastReturnedIndex is returned from getResponse.
   * Remove according to caching policy:
   *   - if query is not reattachable, remove all responses up to and including
   *     highestConsumedIndex.
   */
  private def removeCachedResponses() = {
    var i = highestConsumedIndex
    while (i >= 1 && responses.get(i).isDefined) {
      responses.remove(i)
      i -= 1
    }
  }

  /**
   * Populate response fields that are common and should be set in every response.
   */
  private def setCommonResponseFields(response: T): T = {
    response match {
      case executePlanResponse: proto.ExecutePlanResponse =>
        executePlanResponse
          .toBuilder()
          .setSessionId(executeHolder.sessionHolder.sessionId)
          .setOperationId(executeHolder.operationId)
          .setResponseId(UUID.randomUUID.toString)
          .build()
          .asInstanceOf[T]
    }
  }

  /**
   * Get the response id from the response proto message.
   */
  private def getResponseId(response: T): String = {
    response match {
      case executePlanResponse: proto.ExecutePlanResponse =>
        executePlanResponse.getResponseId
    }
  }
}
