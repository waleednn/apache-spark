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

package org.apache.spark.deploy.mesos

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder

package object config {

  /* Common app configuration. */

  private[spark] val SHUFFLE_CLEANER_INTERVAL_S =
    ConfigBuilder("spark.shuffle.cleaner.interval")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("30s")

  private[spark] val RECOVERY_MODE =
    ConfigBuilder("spark.deploy.recoveryMode")
      .stringConf
      .createWithDefault("NONE")

  private[spark] val DISPATCHER_WEBUI_URL =
    ConfigBuilder("spark.mesos.dispatcher.webui.url")
      .doc("Set the Spark Mesos dispatcher webui_url for interacting with the " +
        "framework. If unset it will point to Spark's internal web UI.")
      .stringConf
      .createOptional

  private[spark] val ZOOKEEPER_URL =
    ConfigBuilder("spark.deploy.zookeeper.url")
      .doc("When `spark.deploy.recoveryMode` is set to ZOOKEEPER, this " +
        "configuration is used to set the zookeeper URL to connect to.")
      .stringConf
      .createOptional

  private[spark] val HISTORY_SERVER_URL =
    ConfigBuilder("spark.mesos.dispatcher.historyServer.url")
      .doc("Set the URL of the history server. The dispatcher will then " +
        "link each driver to its entry in the history server.")
      .stringConf
      .createOptional

  private [spark] val DRIVER_LABELS =
    ConfigBuilder("spark.mesos.driver.labels")
      .doc("Mesos labels to add to the driver.  Labels are free-form key-value pairs. Key-value " +
        "pairs should be separated by a colon, and commas used to list more than one." +
        "Ex. key:value,key2:value2")
      .stringConf
      .createOptional

  private[spark] val SECRET_NAME =
    ConfigBuilder("spark.mesos.driver.secret.name")
      .doc("A comma-separated list of secret references. Consult the Mesos Secret protobuf for " +
        "more information.")
      .stringConf
      .createOptional

  private[spark] val SECRET_VALUE =
    ConfigBuilder("spark.mesos.driver.secret.value")
      .doc("A comma-separated list of secret values.")
      .stringConf
      .createOptional

  private[spark] val SECRET_ENVKEY =
    ConfigBuilder("spark.mesos.driver.secret.envkey")
      .doc("A comma-separated list of the environment variables to contain the secrets." +
        "The environment variable will be set on the driver.")
      .stringConf
      .createOptional

  private[spark] val SECRET_FILENAME =
    ConfigBuilder("spark.mesos.driver.secret.filename")
      .doc("A comma-seperated list of file paths secret will be written to.  Consult the Mesos " +
        "Secret protobuf for more information.")
      .stringConf
      .createOptional

  private [spark] val DRIVER_FAILOVER_TIMEOUT =
    ConfigBuilder("spark.mesos.driver.failoverTimeout")
      .doc("Amount of time in seconds that the master will wait to hear from the driver, " +
          "during a temporary disconnection, before tearing down all the executors.")
      .doubleConf
      .createWithDefault(0.0)
}
