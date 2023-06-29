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

package org.apache.spark.sql.connect.service

import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.{Clock}

/**
 * Post session Connect events to @link org.apache.spark.scheduler.LiveListenerBus.
 *
 * @param sessionHolder:
 *   Session for which the events are generated.
 * @param clock:
 *   Source of time for unit tests.
 */
case class SessionEvents(sessionHolder: SessionHolder, clock: Clock) {

  /**
   * Post @link org.apache.spark.sql.connect.service.SparkListenerConnectSessionStarted.
   */
  def postStarted(): Unit = {
    sessionHolder.session.sparkContext.listenerBus
      .post(
        SparkListenerConnectSessionStarted(
          sessionHolder.sessionId,
          sessionHolder.userId,
          clock.getTimeMillis()))
  }

  /**
   * Post @link org.apache.spark.sql.connect.service.SparkListenerConnectSessionClosed.
   */
  def postClosed(): Unit = {
    sessionHolder.session.sparkContext.listenerBus
      .post(
        SparkListenerConnectSessionClosed(
          sessionHolder.sessionId,
          sessionHolder.userId,
          clock.getTimeMillis()))
  }
}

/**
 * Event sent after a Connect session has been started.
 *
 * @param sessionId:
 *   ID assigned by the client or Connect the operation was executed on.
 * @param eventTime:
 *   The time in ms when the event was generated.
 * @param extraTags:
 *   Additional metadata
 */
case class SparkListenerConnectSessionStarted(
    sessionId: String,
    userId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent

/**
 * Event sent after a Connect session has been closed.
 *
 * @param sessionId:
 *   ID assigned by the client or Connect the operation was executed on.
 * @param eventTime:
 *   The time in ms when the event was generated.
 * @param extraTags:
 *   Additional metadata
 */
case class SparkListenerConnectSessionClosed(
    sessionId: String,
    userId: String,
    eventTime: Long,
    extraTags: Map[String, String] = Map.empty)
    extends SparkListenerEvent
