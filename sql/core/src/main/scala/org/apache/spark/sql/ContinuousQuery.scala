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

package org.apache.spark.sql

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * A handle to a query that is executing continuously in the background as new data arrives.
 * All these methods are thread-safe.
 * @since 2.0.0
 */
@Experimental
trait ContinuousQuery {

  /** Returns the name of the query */
  def name: String

  /** Returns the SQLContext associated with `this` query */
  def sqlContext: SQLContext

  /** Whether the query is currently active or not */
  def isActive: Boolean

  /** Returns the [[ContinuousQueryException]] if the query was terminated by an exception. */
  def exception: Option[ContinuousQueryException]

  /** Returns current status of all the sources. */
  def sourceStatuses: Array[SourceStatus]

  /** Returns current status of the sink. */
  def sinkStatus: SinkStatus

  /**
   * Waits for the termination of this query, either by `stop` or by any exception.
   * @throws ContinuousQueryException, if the query terminated by an exception.
   */
  def awaitTermination(): Unit

  /**
   * Waits for the termination of this query, either by `stop` or by any exception.
   * Returns whether the query has terminated or not.
   * @throws ContinuousQueryException, if the query terminated by an exception before
   *         `timeoutMs` milliseconds
   */
  def awaitTermination(timeoutMs: Long): Boolean

  /**
   * Stops the execution of this query if it is running. This method blocks until the threads
   * performing execution has stopped.
   */
  def stop(): Unit
}
