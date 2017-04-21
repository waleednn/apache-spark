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

package org.apache.spark.deploy.yarn.security

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials

import org.apache.spark.SparkConf
import org.apache.spark.deploy.security.ConfigurableCredentialManager
import org.apache.spark.util.Utils

/**
 * This class exists for backwards compatibility.  It loads services registered under the
 * deprecated [[org.apache.spark.deploy.yarn.security.ServiceCredentialProvider]].
 */
private[yarn] class YARNConfigurableCredentialManager(
    sparkConf: SparkConf,
    hadoopConf: Configuration)
    extends ConfigurableCredentialManager(
      sparkConf,
      hadoopConf,
      new YARNHadoopAccessManager(hadoopConf, sparkConf)) {

  private val deprecatedCredentialProviders = getDeprecatedCredentialProviders

  def getDeprecatedCredentialProviders:
    Map[String, org.apache.spark.deploy.yarn.security.ServiceCredentialProvider] = {
    val deprecatedProviders = loadDeprecatedCredentialProviders

    deprecatedProviders.
      filter(p => isServiceEnabled(p.serviceName))
      .map(p => (p.serviceName, p))
      .toMap
  }

  def loadDeprecatedCredentialProviders:
    List[org.apache.spark.deploy.yarn.security.ServiceCredentialProvider] = {
    ServiceLoader.load(
      classOf[org.apache.spark.deploy.yarn.security.ServiceCredentialProvider],
      Utils.getContextOrSparkClassLoader)
      .asScala
      .toList
  }

  override def obtainCredentials(hadoopConf: Configuration, creds: Credentials): Long = {
    val superInterval = super.obtainCredentials(hadoopConf, creds)

    deprecatedCredentialProviders.values.flatMap { provider =>
      if (provider.credentialsRequired(hadoopConf)) {
        provider.obtainCredentials(hadoopConf, sparkConf, creds)
      } else {
        logDebug(s"Service ${provider.serviceName} does not require a token." +
          s" Check your configuration to see if security is disabled or not.")
        None
      }
    }.foldLeft(superInterval)(math.min)
  }
}
