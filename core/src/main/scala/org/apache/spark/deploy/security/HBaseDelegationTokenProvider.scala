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

package org.apache.spark.deploy.security

import java.io.Closeable
import java.lang.reflect.Method

import scala.reflect.runtime.universe
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[security] class HBaseDelegationTokenProvider
  extends HadoopDelegationTokenProvider with Logging {

  override def serviceName: String = "hbase"

  override def obtainDelegationTokens(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val obtainToken = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
        getMethod("obtainToken", classOf[Configuration])

      logDebug("Attempting to fetch HBase security token.")
      val token = obtainToken.invoke(null, hbaseConf(hadoopConf))
        .asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(s"Get token from HBase: ${token.toString}")
      creds.addToken(token.getService, token)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get token from service $serviceName. " +
          s"Retrying to invoke service with hbase connection.")
        // Seems to be spark is trying to get the token from HBase 2.x.x  version or above where the
        // obtainToken(Configuration conf) API is been removed. Lets try obtaining the token from
        // another compatible API of HBase service.
        obtainDelegationTokensWithHBaseConn(hadoopConf, creds)
    }
    None
  }

  /**
   * The HBase client API used in below method is introduced from HBase 0.98.9,1.0.0 version
   * to invoke this api first connection object has to be retrieved from ConnectionFactory and the
   * same connection can be passed to
   * Token<AuthenticationTokenIdentifier> obtainToken(Connection conn) API
   *
   * @param hadoopConf
   * @param creds
   */
  private def obtainDelegationTokensWithHBaseConn(
      hadoopConf: Configuration, creds: Credentials): Unit = {
    val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
    // Token<AuthenticationTokenIdentifier> obtainToken(Configuration conf) is a deprecated
    // method and in Hbase 2.0.0 the method is already removed. there is one more public
    // consistent API Token<AuthenticationTokenIdentifier> obtainToken(Connection conn),
    // in TokenUtil class which can be invoked using reflection.
    val obtainHBaseConnection = mirror.classLoader.
      loadClass("org.apache.hadoop.hbase.client.ConnectionFactory").
      getMethod("createConnection", classOf[Configuration])
    val conn = obtainHBaseConnection.invoke(null, hbaseConf(hadoopConf))
    try {
      val arrayOfMethod: Array[Method] = ClassLoader.getSystemClassLoader
        .loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").getMethods
      val obtainParamTypeClass = getClassParamType(arrayOfMethod)
      if (null == obtainParamTypeClass) {
        logDebug(s"Failed to invoke service to get token from service $serviceName")
      }
      else {
        val obtainTokenMethod = mirror.classLoader.
          loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
          getMethod("obtainToken", obtainParamTypeClass)
        logDebug("Attempting to fetch HBase security token.")
        val token = obtainTokenMethod.invoke(null, conn)
          .asInstanceOf[Token[_ <: TokenIdentifier]]
        logInfo(s"Get token from HBase: ${token.toString}")
        creds.addToken(token.getService, token)
      }
    }
    catch {
      case NonFatal(e) =>
        logError(s"Failed to get token from service $serviceName", e)
    }
    finally {
      conn.asInstanceOf[Closeable].close()
    }
  }

  private def getClassParamType(arrayOfMethod: Array[Method]): Class[_] = {
    val seqOfFiltMethod = arrayOfMethod.filter(f => f.getName.equalsIgnoreCase("obtainToken"))
    val paramClassType = seqOfFiltMethod.map(m => m.getParameterTypes
      .filter(className => className.getName
        .equalsIgnoreCase("org.apache.hadoop.hbase.client.Connection")).head).head
    paramClassType
  }

  override def delegationTokensRequired(
      sparkConf: SparkConf,
      hadoopConf: Configuration): Boolean = {
    hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
  }

  private def hbaseConf(conf: Configuration): Configuration = {
    try {
      val mirror = universe.runtimeMirror(Utils.getContextOrSparkClassLoader)
      val confCreate = mirror.classLoader.
        loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
        getMethod("create", classOf[Configuration])
      confCreate.invoke(null, conf).asInstanceOf[Configuration]
    } catch {
      case NonFatal(e) =>
        logWarning("Fail to invoke HBaseConfiguration", e)
        conf
    }
  }
}
