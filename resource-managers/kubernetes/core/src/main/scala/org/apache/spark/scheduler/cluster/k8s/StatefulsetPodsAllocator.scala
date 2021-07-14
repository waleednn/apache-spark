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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.fabric8.kubernetes.api.model.{PersistentVolumeClaim,
  PersistentVolumeClaimBuilder, PodSpec, PodSpecBuilder, PodTemplateSpec}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.util.{Clock, Utils}

private[spark] class StatefulsetPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends AbstractPodsAllocator(
    conf, secMgr, executorBuilder, kubernetesClient, snapshotsStore, clock) with Logging {

  private val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  private val driverPodReadinessTimeout = conf.get(KUBERNETES_ALLOCATION_DRIVER_READINESS_TIMEOUT)

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $name in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  def start(applicationId: String, schedulerBackend: KubernetesClusterSchedulerBackend): Unit = {
    driverPod.foreach { pod =>
      // Wait until the driver pod is ready before starting executors, as the headless service won't
      // be resolvable by DNS until the driver pod is ready.
      Utils.tryLogNonFatalError {
        kubernetesClient
          .pods()
          .withName(pod.getMetadata.getName)
          .waitUntilReady(driverPodReadinessTimeout, TimeUnit.SECONDS)
      }
    }
  }

  def setTotalExpectedExecutors(applicationId: String,
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = {

    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      setTargetExecutorsReplicaset(numExecs, applicationId, rp.id)
    }
  }

  def isDeleted(executorId: String): Boolean = false

  // For now just track the sets created, in the future maybe track requested value too.
  val setsCreated = new mutable.HashSet[Int]()

  private def setName(applicationId: String, rpid: Int): String = {
    s"spark-s-${applicationId}-${rpid}"
  }

  private def setTargetExecutorsReplicaset(
      expected: Int,
      applicationId: String,
      resourceProfileId: Int): Unit = {
    if (setsCreated.contains(resourceProfileId)) {
      val statefulset = kubernetesClient.apps().statefulSets().withName(
        setName(applicationId, resourceProfileId: Int))
      statefulset.scale(expected)
    } else {
      // We need to make the new replicaset which is going to involve building
      // a pod.
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        "EXECID",// template exec IDs
        applicationId,
        driverPod,
        resourceProfileId)
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
        kubernetesClient, rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod
      val podWithAttachedContainer: PodSpec = new PodSpecBuilder(executorPod.pod.getSpec())
        .addToContainers(executorPod.container)
        .build()

      val meta = executorPod.pod.getMetadata()

      // Create a pod template spec from the pod.
      val podTemplateSpec = new PodTemplateSpec(meta, podWithAttachedContainer)
      // Resources that need to be created, volumes are per-pod which is all we care about here.
      val resources = resolvedExecutorSpec.executorKubernetesResources
      // We'll let PVCs be handled by the statefulset, we need
      val volumes = resources
        .filter(_.getKind == "PersistentVolumeClaim")
        .map { hm =>
          val v = hm.asInstanceOf[PersistentVolumeClaim]
          new PersistentVolumeClaimBuilder(v)
            .editMetadata()
              .withName(v.getMetadata().getName().replace("EXECID", ""))
            .endMetadata()
            .build()
        }.asJava

      val statefulSet = new io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder()
        .withNewMetadata()
          .withName(setName(applicationId, resourceProfileId))
          .withNamespace(conf.get(KUBERNETES_NAMESPACE))
        .endMetadata()
        .withNewSpec()
          .withPodManagementPolicy("Parallel")
          .withReplicas(expected)
          .withNewSelector()
            .addToMatchLabels(SPARK_APP_ID_LABEL, applicationId)
            .addToMatchLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .addToMatchLabels(SPARK_RESOURCE_PROFILE_ID_LABEL, resourceProfileId.toString)
          .endSelector()
          .withTemplate(podTemplateSpec)
          .addAllToVolumeClaimTemplates(volumes)
        .endSpec()
        .build()

      kubernetesClient.apps().statefulSets().create(statefulSet)
    }
  }

  override def stop(applicationId: String): Unit = {
    // Cleanup the statefulsets when we stop
    setsCreated.foreach { rpid =>
      kubernetesClient.apps().statefulSets().withName(setName(applicationId, rpid)).delete()
    }
  }
}
