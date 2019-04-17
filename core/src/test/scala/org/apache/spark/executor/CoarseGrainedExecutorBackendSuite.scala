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

package org.apache.spark.executor


import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission.{OWNER_EXECUTE, OWNER_READ, OWNER_WRITE}
import java.util.EnumSet

import com.google.common.io.Files
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.util.Utils


class CoarseGrainedExecutorBackendSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar {

  test("parsing no resources") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")

    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    var testResourceArgs = Some("")
    var error = intercept[SparkException] {
      val parsedResources = backend.parseResources(testResourceArgs)
    }.getMessage()

    assert(error.contains("Format of the resourceAddrs parameter is invalid"))

    testResourceArgs = Some("gpu=::")
    error = intercept[SparkException] {
      val parsedResources = backend.parseResources(testResourceArgs)
    }.getMessage()

    assert(error.contains("Format of the resourceAddrs parameter is invalid"))
  }


  test("parsing one resources") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")

    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    val testResourceArgs = Some("gpu=2::0,1")
    val parsedResources = backend.parseResources(testResourceArgs)

    assert(parsedResources.size === 1)
    assert(parsedResources.get("gpu").nonEmpty)
    assert(parsedResources.get("gpu").get.getName() === "gpu")
    assert(parsedResources.get("gpu").get.getUnits() === "")
    assert(parsedResources.get("gpu").get.getCount() === 2)
    assert(parsedResources.get("gpu").get.getAddresses().deep === Array("0", "1").deep)
  }

  test("parsing multiple resources") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")

    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    val testResourceArgs = Some("gpu=2::0,1;fpga=3:mb:f1,f2,f3")
    val parsedResources = backend.parseResources(testResourceArgs)

    assert(parsedResources.size === 2)
    assert(parsedResources.get("gpu").nonEmpty)
    assert(parsedResources.get("gpu").get.getName() === "gpu")
    assert(parsedResources.get("gpu").get.getUnits() === "")
    assert(parsedResources.get("gpu").get.getCount() === 2)
    assert(parsedResources.get("gpu").get.getAddresses().deep === Array("0", "1").deep)
    assert(parsedResources.get("fpga").nonEmpty)
    assert(parsedResources.get("fpga").get.getName() === "fpga")
    assert(parsedResources.get("fpga").get.getUnits() === "mb")
    assert(parsedResources.get("fpga").get.getCount() === 3)
    assert(parsedResources.get("fpga").get.getAddresses().deep === Array("f1", "f2", "f3").deep)
  }

  test("use discoverer") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")

    assume(!(Utils.isWindows))

    withTempDir { dir =>
      val fpgaDiscovery = new File(dir, "resourceDiscoverScriptfpga")
      Files.write("echo 3::f1,f2,f3", fpgaDiscovery, StandardCharsets.UTF_8)
      JavaFiles.setPosixFilePermissions(fpgaDiscovery.toPath(),
        EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
      conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, fpgaDiscovery.getPath())

      val serializer = new JavaSerializer(conf)
      val env = createMockEnv(conf, serializer)

      // we don't really use this, just need it to get at the parser function
      val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
        4, Seq.empty[URL], env, None)

      val parsedResources = backend.parseResources(None)

      assert(parsedResources.size === 1)
      assert(parsedResources.get("fpga").nonEmpty)
      assert(parsedResources.get("fpga").get.getName() === "fpga")
      assert(parsedResources.get("fpga").get.getUnits() === "")
      assert(parsedResources.get("fpga").get.getCount() === 3)
      assert(parsedResources.get("fpga").get.getAddresses().deep === Array("f1", "f2", "f3").deep)
    }
  }

  private def createMockEnv(conf: SparkConf, serializer: JavaSerializer): SparkEnv = {
    val mockEnv = mock[SparkEnv]
    val mockRpcEnv = mock[RpcEnv]
    when(mockEnv.conf).thenReturn(conf)
    when(mockEnv.serializer).thenReturn(serializer)
    when(mockEnv.closureSerializer).thenReturn(serializer)
    when(mockEnv.rpcEnv).thenReturn(mockRpcEnv)
    SparkEnv.set(mockEnv)
    mockEnv
  }
}
