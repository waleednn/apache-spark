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


import java.io.{File, PrintWriter}
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission.{OWNER_EXECUTE, OWNER_READ, OWNER_WRITE}
import java.util.EnumSet

import com.google.common.io.Files
import org.json4s.JsonAST.{JArray, JObject, JString}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils

class CoarseGrainedExecutorBackendSuite extends SparkFunSuite
    with LocalSparkContext with MockitoSugar {

  // scalastyle:off println
  private def writeFileWithJson(dir: File, strToWrite: JArray): String = {
    val f1 = File.createTempFile("test-resource-parser1", "", dir)
    val writer1 = new PrintWriter(f1)
    writer1.println(compact(render(strToWrite)))
    writer1.close()
    f1.getPath()
  }
  // scalastyle:on println

  test("parsing no resources") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)

    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)
    withTempDir { tmpDir =>
      val testResourceArgs: JObject = ("" -> "")
      val ja = JArray(List(testResourceArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("Exception parsing the resources passed"))
    }
  }

  test("parsing one resources") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)
    withTempDir { tmpDir =>
      val testResourceArgs =
        ("name" -> "gpu") ~
        ("units" -> "") ~
        ("count" -> 2) ~
        ("addresses" -> JArray(Array("0", "1").map(JString(_)).toList))
      val ja = JArray(List(testResourceArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      val parsedResources = backend.parseResources(Some(f1))

      assert(parsedResources.size === 1)
      assert(parsedResources.get("gpu").nonEmpty)
      assert(parsedResources.get("gpu").get.name === "gpu")
      assert(parsedResources.get("gpu").get.units === "")
      assert(parsedResources.get("gpu").get.count === 2)
      assert(parsedResources.get("gpu").get.addresses.deep === Array("0", "1").deep)
    }
  }

  test("parsing multiple resources") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_POSTFIX, "3")
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_UNITS_POSTFIX, "gb")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_POSTFIX, "1024")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_UNITS_POSTFIX, "mb")
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0", "1").map(JString(_)).toList))
      val fpgaArgs =
        ("name" -> "fpga") ~
          ("units" -> "gb") ~
          ("count" -> 3) ~
          ("addresses" -> JArray(Array("f1", "f2", "f3").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs, fpgaArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      val parsedResources = backend.parseResources(Some(f1))

      assert(parsedResources.size === 2)
      assert(parsedResources.get("gpu").nonEmpty)
      assert(parsedResources.get("gpu").get.name === "gpu")
      assert(parsedResources.get("gpu").get.units === "")
      assert(parsedResources.get("gpu").get.count === 2)
      assert(parsedResources.get("gpu").get.addresses.deep === Array("0", "1").deep)
      assert(parsedResources.get("fpga").nonEmpty)
      assert(parsedResources.get("fpga").get.name === "fpga")
      assert(parsedResources.get("fpga").get.units === "gb")
      assert(parsedResources.get("fpga").get.count === 3)
      assert(parsedResources.get("fpga").get.addresses.deep === Array("f1", "f2", "f3").deep)
    }
  }

  test("error checking parsing resources and executor and task configs") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend( env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    // not enough gpu's on the executor
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 1) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("isn't large enough to meet task requirements"))
    }

    // missing resource on the executor
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "fpga") ~
          ("units" -> "") ~
          ("count" -> 1) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("Executor resource config missing required task resource"))
    }

    // extra unit in executor config
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "m") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("the task units config is missing"))
    }

    // number of addresses with count >, this is ok
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0", "1", "2").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)
      val parsedResources = backend.parseResources(Some(f1))
      assert(parsedResources.size === 1)
      assert(parsedResources.get("gpu").nonEmpty)
      assert(parsedResources.get("gpu").get.name === "gpu")
      assert(parsedResources.get("gpu").get.units === "")
      assert(parsedResources.get("gpu").get.count === 2)
      assert(parsedResources.get("gpu").get.addresses.deep === Array("0", "1", "2").deep)
    }

    // count of gpu's > then user executor config
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 3) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("gpu, count: 3 doesn't match what user requests for executor count: 2"))
    }

    // number of addresses mismatch with count <
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("The number of resource addresses is expected to " +
        "either be >= to the count or be empty if not applicable"))
    }
  }

  test("parsing resources task configs with units missing executor units") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_UNITS_POSTFIX, "g")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    // executor config doesn't have units on gpu and task one does
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("executor units config is missing"))
    }
  }

  test("parsing resources task configs with missing executor count config") {
    val conf = new SparkConf
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "gpu" + SPARK_RESOURCE_COUNT_POSTFIX, "2")
    val serializer = new JavaSerializer(conf)
    val env = createMockEnv(conf, serializer)
    // we don't really use this, just need it to get at the parser function
    val backend = new CoarseGrainedExecutorBackend(env.rpcEnv, "driverurl", "1", "host1",
      4, Seq.empty[URL], env, None)

    // executor config doesn't have units on gpu and task one does
    withTempDir { tmpDir =>
      val gpuArgs =
        ("name" -> "gpu") ~
          ("units" -> "") ~
          ("count" -> 2) ~
          ("addresses" -> JArray(Array("0").map(JString(_)).toList))
      val ja = JArray(List(gpuArgs))
      val f1 = writeFileWithJson(tmpDir, ja)

      var error = intercept[SparkException] {
        val parsedResources = backend.parseResources(Some(f1))
      }.getMessage()

      assert(error.contains("Executor resource: gpu not specified via config: " +
        "spark.executor.resource.gpu.count, but required by the task, please " +
        "fix your configuration"))
    }
  }

  test("use discoverer") {
    val conf = new SparkConf
    conf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_POSTFIX, "3")
    conf.set(SPARK_TASK_RESOURCE_PREFIX + "fpga" + SPARK_RESOURCE_COUNT_POSTFIX, "3")
    assume(!(Utils.isWindows))
    withTempDir { dir =>
      val fpgaDiscovery = new File(dir, "resourceDiscoverScriptfpga")
      Files.write("echo {\\\"name\\\":\\\"fpga\\\", \\\"count\\\":3, \\\"units\\\":\\\"\\\"," +
        " \\\"addresses\\\":[\\\"f1\\\",\\\"f2\\\",\\\"f3\\\"]}",
        fpgaDiscovery, StandardCharsets.UTF_8)
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
      assert(parsedResources.get("fpga").get.name === "fpga")
      assert(parsedResources.get("fpga").get.units === "")
      assert(parsedResources.get("fpga").get.count === 3)
      assert(parsedResources.get("fpga").get.addresses.deep === Array("f1", "f2", "f3").deep)
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
