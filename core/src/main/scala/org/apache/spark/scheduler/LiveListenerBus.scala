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

import java.util.{List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import com.codahale.metrics.{Counter, MetricRegistry, Timer}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(conf: SparkConf) {

  import LiveListenerBus._

  private var sparkContext: SparkContext = _

  private[spark] val metrics = new LiveListenerBusMetrics(conf)

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  private val queues = new CopyOnWriteArrayList[AsyncEventQueue]()

  /** Add a listener to the default queue. */
  def addListener(listener: SparkListenerInterface): Unit = {
    addToQueue(listener, "default")
  }

  /**
   * Add a listener to a specific queue, creating a new queue if needed. Queues are independent
   * of each other (each one uses a separate thread for delivering events), allowing slower
   * listeners to be somewhat isolated from others.
   */
  def addToQueue(listener: SparkListenerInterface, queue: String): Unit = synchronized {
    if (stopped.get()) {
      throw new IllegalStateException("LiveListenerBus is stopped.")
    }

    queues.asScala.find(_.name == queue) match {
      case Some(queue) =>
        queue.addListener(listener)

      case None =>
        val newQueue = new AsyncEventQueue(queue, conf, metrics)
        newQueue.addListener(listener)
        if (started.get() && !stopped.get()) {
          newQueue.start(sparkContext)
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: SparkListenerInterface): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /** Post an event to all queues. */
  def post(event: SparkListenerEvent): Unit = {
    if (!stopped.get()) {
      metrics.numEventsPosted.inc()
      val it = queues.iterator()
      while (it.hasNext()) {
        it.next().post(event)
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   *
   * @param sc Used to stop the SparkContext in case the listener thread dies.
   */
  def start(sc: SparkContext, metricsSystem: MetricsSystem): Unit = synchronized {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("LiveListenerBus already started.")
    }

    queues.asScala.foreach(_.start(sc))
    metricsSystem.registerSource(metrics)
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
      }
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop bus that has not yet started!")
    }

    if (!stopped.compareAndSet(false, true)) {
      return
    }

    synchronized {
      queues.asScala.foreach(_.stop())
      queues.clear()
    }
  }

  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T] = {
    queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
  }

  private[spark] def listeners: JList[SparkListenerInterface] = {
    queues.asScala.flatMap(_.listeners.asScala).asJava
  }

  // For testing only.
  private[scheduler] def activeQueues(): Seq[String] = {
    queues.asScala.map(_.name).toSeq
  }

}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** Queue name where status-related listeners are grouped together. */
  val APP_STATUS_QUEUE = "appStatus"

  /** Queue name where executor management-related listeners are grouped together. */
  val EXECUTOR_MGMT_QUEUE = "executorMgmt"
}

private[spark] class LiveListenerBusMetrics(conf: SparkConf)
  extends Source with Logging {

  override val sourceName: String = "LiveListenerBus"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * The total number of events posted to the LiveListenerBus. This is a count of the total number
   * of events which have been produced by the application and sent to the listener bus, NOT a
   * count of the number of events which have been processed and delivered to listeners (or dropped
   * without being delivered).
   */
  val numEventsPosted: Counter = metricRegistry.counter(MetricRegistry.name("numEventsPosted"))

  // Guarded by synchronization.
  private val perListenerClassTimers = mutable.Map[String, Timer]()

  /**
   * Returns a timer tracking the processing time of the given listener class.
   * events processed by that listener. This method is thread-safe.
   */
  def getTimerForListenerClass(cls: Class[_ <: SparkListenerInterface]): Option[Timer] = {
    synchronized {
      val className = cls.getName
      val maxTimed = conf.get(LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED)
      perListenerClassTimers.get(className).orElse {
        if (perListenerClassTimers.size == maxTimed) {
          logError(s"Not measuring processing time for listener class $className because a " +
            s"maximum of $maxTimed listener classes are already timed.")
          None
        } else {
          perListenerClassTimers(className) =
            metricRegistry.timer(MetricRegistry.name("listenerProcessingTime", className))
          perListenerClassTimers.get(className)
        }
      }
    }
  }

}
