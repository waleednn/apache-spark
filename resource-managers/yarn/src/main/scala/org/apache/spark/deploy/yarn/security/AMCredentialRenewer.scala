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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.security.AbstractCredentialRenewer
import org.apache.spark.rpc.RpcEndpointRef

/**
 * YARN-specific implementation of AbstractCredentialRenewer.
 */
private[yarn] class AMCredentialRenewer(
    _sparkConf: SparkConf,
    _hadoopConf: Configuration)
  extends AbstractCredentialRenewer(_sparkConf, _hadoopConf) {

  private val credentialManager = new YARNHadoopDelegationTokenManager(sparkConf, hadoopConf)

  override protected def obtainDelegationTokens(creds: Credentials): Long = {
    credentialManager.obtainDelegationTokens(hadoopConf, creds)
  }

  // Override the parent method just to make it public.
  override def setDriverRef(ref: RpcEndpointRef): Unit = super.setDriverRef(ref)

}
