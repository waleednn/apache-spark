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

package org.apache.spark.sql.connect.client

import scala.language.existentials

import io.grpc.{ChannelCredentials, Grpc, ManagedChannel, ManagedChannelBuilder}

import org.apache.spark.sql.connect.client.SparkConnectClient.MetadataHeaderClientInterceptor
import org.apache.spark.sql.connect.common.config.ConnectCommon

class ChannelBuilder() {
  def createChannelBuilder(
      host: String,
      port: Int,
      credentials: ChannelCredentials): ManagedChannelBuilder[_] = {
    val builder = Grpc.newChannelBuilderForAddress(host, port, credentials)
    builder.maxInboundMessageSize(ConnectCommon.CONNECT_GRPC_MAX_MESSAGE_SIZE)
    builder
  }

  private[sql] def createChannelFromConf(
      conf: SparkConnectClient.Configuration): ManagedChannel = {
    val channel = createChannelBuilder(conf.host, conf.port, conf.credentials)
    if (conf.metadata.nonEmpty) {
      channel.intercept(new MetadataHeaderClientInterceptor(conf.metadata))
    }

    channel.build()
  }
}
