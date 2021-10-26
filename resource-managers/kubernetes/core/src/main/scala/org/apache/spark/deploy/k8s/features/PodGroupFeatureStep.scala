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

package org.apache.spark.deploy.k8s.features

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{KubernetesConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

private[spark] class PodGroupFeatureStep(kubernetesConf: KubernetesConf)
  extends KubernetesFeatureConfigStep {

  val conf: SparkConf = kubernetesConf.sparkConf
  val POD_GROUP_ANNOTATION = "scheduling.k8s.io/group-name"
  val podGroupName = s"${kubernetesConf.appId}-podgroup"
  val enablePodGroup: Boolean = conf.get(KUBERNETES_ENABLE_PODGROUP)

  override def configurePod(pod: SparkPod): SparkPod = {
    if (enablePodGroup) {
      val k8sPodBuilder = new PodBuilder(pod.pod)
        .editMetadata()
        .addToAnnotations(POD_GROUP_ANNOTATION, podGroupName)
        .endMetadata()
      val k8sPod = k8sPodBuilder.build()
      SparkPod(k8sPod, pod.container)
    } else {
      pod
    }
  }

  private def getPodGroupMinResources(): java.util.HashMap[String, Quantity] = {
    val cpu = kubernetesConf.get(KUBERNETES_PODGROUP_MIN_CPU)
    val memory = kubernetesConf.get(KUBERNETES_PODGROUP_MIN_MEMORY)

    val cpuQ = new QuantityBuilder(false)
      .withAmount(s"${cpu}")
      .build()
    val memoryQ = new QuantityBuilder(false)
      .withAmount(s"${memory}Mi")
      .build()
    new java.util.HashMap(Map("cpu" -> cpuQ, "memory" -> memoryQ).asJava)
  }

  private def getVolcanoResources(): Seq[HasMetadata] = {
    val podGroup = new PodGroupBuilder()
      .editOrNewMetadata()
        .withName(podGroupName)
        .withNamespace(kubernetesConf.get(KUBERNETES_NAMESPACE))
      .endMetadata()
      .editOrNewSpec()
        .withMinResources(getPodGroupMinResources())
      .endSpec()
      .build()

    Seq{podGroup}
  }

  private def getPodGroupResources(): Seq[HasMetadata] = {
    if (enablePodGroup) {
      kubernetesConf.schedulerName match {
        case "volcano" =>
          getVolcanoResources()
        case _ =>
          Seq.empty
      }
    } else {
      Seq.empty
    }
  }

  override def getAdditionalKubernetesResources(): Seq[HasMetadata] = {
    getPodGroupResources()
  }

  override def getAdditionalPreKubernetesResources(): Seq[HasMetadata] = {
    getPodGroupResources()
  }
}
