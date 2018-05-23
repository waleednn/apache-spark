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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.{Future, TimeUnit}

import com.google.common.cache.CacheBuilder
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.ExecutorExited
import org.apache.spark.util.Utils

private[spark] class ExecutorPodsLifecycleEventHandler(
    conf: SparkConf,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    podsEventQueue: ExecutorPodsEventQueue) extends Logging {

  import ExecutorPodsLifecycleEventHandler._

  // Use a best-effort to track which executors have been removed already. It's not generally
  // job-breaking if we remove executors more than once but it's ideal if we make an attempt
  // to avoid doing so. Expire cache entries so that this data structure doesn't grow beyond
  // bounds.
  private val removedExecutors = CacheBuilder.newBuilder()
    .expireAfterWrite(3, TimeUnit.MINUTES)
    .build[Long, java.lang.Long]

  def start(schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    podsEventQueue.addSubscriber(1000L) { updatedPods =>
      updatedPods.foreach { updatedPod =>
        processUpdatedPod(schedulerBackend, updatedPod)
      }
    }
  }

  private def processUpdatedPod(
      schedulerBackend: KubernetesClusterSchedulerBackend, updatedPod: Pod) = {
    val execId = updatedPod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong
    if (isDeleted(updatedPod)) {
      removeExecutorFromSpark(schedulerBackend, updatedPod, execId)
    } else {
      updatedPod.getStatus.getPhase.toLowerCase match {
        // TODO (SPARK-24135) - handle more classes of errors
        case "error" | "failed" | "succeeded" =>
          // If deletion failed on a previous try, we can try again if resync informs us the pod
          // is still around.
          // Delete as best attempt - duplicate deletes will throw an exception but the end state
          // of getting rid of the pod is what matters.
          Utils.tryLogNonFatalError {
            kubernetesClient
              .pods()
              .withName(updatedPod.getMetadata.getName)
              .delete()
          }
          removeExecutorFromSpark(schedulerBackend, updatedPod, execId)
      }
    }
  }

  private def removeExecutorFromSpark(
      schedulerBackend: KubernetesClusterSchedulerBackend,
      updatedPod: Pod,
      execId: Long): Unit = {
    if (removedExecutors.getIfPresent(execId) == null) {
      removedExecutors.put(execId, execId)
      val exitReason = findExitReason(updatedPod, execId)
      schedulerBackend.doRemoveExecutor(execId.toString, exitReason)
    }
  }

  private def findExitReason(pod: Pod, execId: Long): ExecutorExited = {
    val exitCode = findExitCode(pod)
    val (exitCausedByApp, exitMessage) = if (isDeleted(pod)) {
      (false, s"The executor with id $execId was deleted by a user or the framework.")
    } else {
      val msg = exitReasonMessage(pod, execId, exitCode)
      (true, msg)
    }
    ExecutorExited(exitCode, exitCausedByApp, exitMessage)
  }

  private def exitReasonMessage(pod: Pod, execId: Long, exitCode: Int) = {
    s"""
       |The executor with id $execId exited with exit code $exitCode.
       |The API gave the following brief reason: ${pod.getStatus.getReason}
       |The API gave the following message: ${pod.getStatus.getMessage}
       |The API gave the following container statuses:
       |
       |${pod.getStatus.getContainerStatuses.asScala.map(_.toString).mkString("\n===\n")}
      """.stripMargin
  }

  private def isDeleted(pod: Pod): Boolean = pod.getMetadata.getDeletionTimestamp != null

  private def findExitCode(pod: Pod): Int = {
    pod.getStatus.getContainerStatuses.asScala.find { containerStatus =>
      containerStatus.getState.getTerminated != null
    }.map { terminatedContainer =>
      terminatedContainer.getState.getTerminated.getExitCode.toInt
    }.getOrElse(UNKNOWN_EXIT_CODE)
  }
}

private object ExecutorPodsLifecycleEventHandler {
  val UNKNOWN_EXIT_CODE = -1
}

