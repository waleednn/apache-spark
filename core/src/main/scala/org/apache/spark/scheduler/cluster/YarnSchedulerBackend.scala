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

package org.apache.spark.scheduler.cluster

import akka.actor.{Actor, ActorRef, Props}
import akka.remote.{DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.AkkaUtils

/**
 * Abstract Yarn scheduler backend that contains common logic
 * between the client and cluster Yarn scheduler backends.
 */
private[spark] abstract class YarnSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.actorSystem) {

  if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
    minRegisteredRatio = 0.8
  }

  protected var totalExpectedExecutors = 0

  private val yarnSchedulerActor: ActorRef =
    actorSystem.actorOf(
      Props(new YarnSchedulerActor),
      name = YarnSchedulerBackend.ACTOR_NAME)

  private implicit val askTimeout = AkkaUtils.askTimeout(sc.conf)

  /**
   * Request the given number of executors from the ApplicationMaster.
   */
  override def requestPendingExecutors(numPendingExecutors: Int): Boolean = synchronized {
    AkkaUtils.askWithAck(
      RequestPendingExecutors(numPendingExecutors, totalRegisteredExecutors.get),
      yarnSchedulerActor,
      askTimeout)
  }

  /**
   * Request the ApplicationMaster to kill the specified executors.
   */
  override def killExecutors(executorIds: Seq[String]): Boolean = {
    AkkaUtils.askWithAck(KillExecutors(executorIds), yarnSchedulerActor, askTimeout)
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= totalExpectedExecutors * minRegisteredRatio
  }

  /**
   * Add filters to the SparkUI.
   */
  private def addWebUIFilter(
      filterName: String,
      filterParams: Map[String, String],
      proxyBase: String): Unit = {
    if (proxyBase != null && proxyBase.nonEmpty) {
      System.setProperty("spark.ui.proxyBase", proxyBase)
    }

    val hasFilter =
      filterName != null && filterName.nonEmpty &&
      filterParams != null && filterParams.nonEmpty
    if (hasFilter) {
      logInfo(s"Add WebUI Filter. $filterName, $filterParams, $proxyBase")
      conf.set("spark.ui.filters", filterName)
      filterParams.foreach { case (k, v) => conf.set(s"spark.$filterName.param.$k", v) }
      scheduler.sc.ui.foreach { ui => JettyUtils.addFilters(ui.getHandlers, conf) }
    }
  }

  /**
   * An actor that communicates with the ApplicationMaster.
   */
  private class YarnSchedulerActor extends Actor {
    private var amActor: Option[ActorRef] = None

    override def preStart(): Unit = {
      // Listen for disassociation events
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case RegisterClusterManager =>
        logInfo(s"ApplicationMaster registered as $sender")
        amActor = Some(sender)

      case r: RequestPendingExecutors =>
        val acked = amActor match {
          case Some(actor) => AkkaUtils.askWithAck(r, actor, askTimeout)
          case None => logWarning(
            "Attempted to request executors before the ApplicationMaster has registered!")
            false
        }
        sender ! acked

      case k: KillExecutors =>
        val acked = amActor match {
          case Some(actor) => AkkaUtils.askWithAck(k, actor, askTimeout)
          case None => logWarning(
            "Attempted to kill executors before the ApplicationMaster has registered!")
            false
        }
        sender ! acked

      case AddWebUIFilter(filterName, filterParams, proxyBase) =>
        addWebUIFilter(filterName, filterParams, proxyBase)
        sender ! true

      case d: DisassociatedEvent =>
        if (amActor.isDefined && sender == amActor.get) {
          logWarning(s"ApplicationMaster has disassociated: $d")
        }
    }
  }
}

private[spark] object YarnSchedulerBackend {
  val ACTOR_NAME = "YarnScheduler"
}
