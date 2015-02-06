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

package org.apache.spark.deploy.rest

/**
 * An abstract request sent from the client in the REST application submission protocol.
 */
private[spark] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  var clientSparkVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(clientSparkVersion, "clientSparkVersion")
  }
}

/**
 * A request to launch a new application in the REST application submission protocol.
 */
private[spark] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
  var appResource: String = null
  var mainClass: String = null
  var appArgs: Array[String] = null
  var sparkProperties: Map[String, String] = null
  var environmentVariables: Map[String, String] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assert(sparkProperties != null, "No Spark properties set!")
    assertFieldIsSet(appResource, "appResource")
    assertPropertyIsSet("spark.app.name")
    assertPropertyIsBoolean("spark.driver.supervise")
    assertPropertyIsNumeric("spark.driver.cores")
    assertPropertyIsNumeric("spark.cores.max")
    assertPropertyIsMemory("spark.driver.memory")
    assertPropertyIsMemory("spark.executor.memory")
  }

  private def assertPropertyIsSet(key: String): Unit =
    assertFieldIsSet(sparkProperties.getOrElse(key, null), key)

  private def assertPropertyIsBoolean(key: String): Unit =
    assertFieldIsBoolean(sparkProperties.getOrElse(key, null), key)

  private def assertPropertyIsNumeric(key: String): Unit =
    assertFieldIsNumeric(sparkProperties.getOrElse(key, null), key)

  private def assertPropertyIsMemory(key: String): Unit =
    assertFieldIsMemory(sparkProperties.getOrElse(key, null), key)
}
