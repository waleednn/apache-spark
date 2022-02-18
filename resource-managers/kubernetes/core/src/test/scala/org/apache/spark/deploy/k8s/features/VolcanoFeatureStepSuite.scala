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

import io.fabric8.volcano.scheduling.v1beta1.PodGroup

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._

class VolcanoFeatureStepSuite extends SparkFunSuite {

  test("SPARK-36061: Driver Pod with Volcano PodGroup") {
    val sparkConf = new SparkConf()
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf)
    val step = new VolcanoFeatureStep()
    step.init(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    val annotations = configuredPod.pod.getMetadata.getAnnotations

    assert(annotations.get("scheduling.k8s.io/group-name") === s"${kubernetesConf.appId}-podgroup")
    val podGroup = step.getAdditionalPreKubernetesResources().head.asInstanceOf[PodGroup]
    assert(podGroup.getMetadata.getName === s"${kubernetesConf.appId}-podgroup")
  }

  test("SPARK-36061: Executor Pod with Volcano PodGroup") {
    val sparkConf = new SparkConf()
    val kubernetesConf = KubernetesTestConf.createExecutorConf(sparkConf)
    val step = new VolcanoFeatureStep()
    step.init(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())
    val annotations = configuredPod.pod.getMetadata.getAnnotations
    assert(annotations.get("scheduling.k8s.io/group-name") === s"${kubernetesConf.appId}-podgroup")
  }
}
