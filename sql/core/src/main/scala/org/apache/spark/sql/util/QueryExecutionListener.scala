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

package org.apache.spark.sql.util

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution

/**
 * :: Experimental ::
 * The interface of query execution listener that can be used to analyze execution metrics.
 *
 * @note Implementations should guarantee thread-safety as they can be invoked by
 * multiple different threads.
 */
@Experimental
@InterfaceStability.Evolving
trait QueryExecutionListener {

  /**
   * A callback function that will be called when a query executed successfully.
   *
   * @param funcName name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param durationNs the execution time for this query in nanoseconds.
   * @param outputParams The output parameters in case the method is invoked as a result of a
   *                     write operation. In case of a read will be @see[[None]]
   */
  @DeveloperApi
  def onSuccess(
      funcName: String,
      qe: QueryExecution,
      durationNs: Long,
      outputParams: Option[OutputParams]): Unit
  /**
   * A callback function that will be called when a query execution failed.
   *
   * @param funcName the name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param exception the exception that failed this query.
   * @param outputParams The output parameters in case the method is invoked as a result of a
   *                     write operation. In case of a read will be @see[[None]]
   *
   * @note This can be invoked by multiple different threads.
   */
  @DeveloperApi
  def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception,
      outputParams: Option[OutputParams]): Unit
}

/**
 * Contains extra information useful for query analysis passed on from the methods in
 * @see[[org.apache.spark.sql.DataFrameWriter]] while writing to a datasource
 * @param datasourceType type of data source written to like csv, parquet, json, hive, jdbc etc.
 * @param destination path or table name written to
 * @param options the map containing the output options for the underlying datasource
 *                specified by using the @see [[org.apache.spark.sql.DataFrameWriter#option]] method
 * @param writeParams will contain any extra information that the write method wants to provide
 */
case class OutputParams(
    datasourceType: String,
    destination: Option[String],
    options: Map[String, String],
    writeParams: Map[String, String] = Map.empty)
/**
 * :: Experimental ::
 *
 * Manager for [[QueryExecutionListener]]. See `org.apache.spark.sql.SQLContext.listenerManager`.
 */
@Experimental
@InterfaceStability.Evolving
class ExecutionListenerManager private[sql] () extends Logging {

  /**
   * Registers the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def register(listener: QueryExecutionListener): Unit = writeLock {
    listeners += listener
  }

  /**
   * Unregisters the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def unregister(listener: QueryExecutionListener): Unit = writeLock {
    listeners -= listener
  }

  /**
   * Removes all the registered [[QueryExecutionListener]].
   */
  @DeveloperApi
  def clear(): Unit = writeLock {
    listeners.clear()
  }

  private[sql] def onSuccess(
      funcName: String,
      qe: QueryExecution,
      duration: Long,
      outputParams: Option[OutputParams] = None): Unit = {
    readLock {
      withErrorHandling { listener =>
        listener.onSuccess(funcName, qe, duration, outputParams)
      }
    }
  }

  private[sql] def onFailure(
      funcName: String,
      qe: QueryExecution,
      exception: Exception,
      outputParams: Option[OutputParams] = None): Unit = {
    readLock {
      withErrorHandling { listener =>
        listener.onFailure(funcName, qe, exception, outputParams)
      }
    }
  }

  private[this] val listeners = ListBuffer.empty[QueryExecutionListener]

  /** A lock to prevent updating the list of listeners while we are traversing through them. */
  private[this] val lock = new ReentrantReadWriteLock()

  private def withErrorHandling(f: QueryExecutionListener => Unit): Unit = {
    for (listener <- listeners) {
      try {
        f(listener)
      } catch {
        case NonFatal(e) => logWarning("Error executing query execution listener", e)
      }
    }
  }

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val rl = lock.readLock()
    rl.lock()
    try f finally {
      rl.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val wl = lock.writeLock()
    wl.lock()
    try f finally {
      wl.unlock()
    }
  }
}
