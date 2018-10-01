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

package org.apache.spark.sql.kafka010

import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[kafka010] object KafkaSecurityHelper extends Logging {
  def getKeytabJaasParams(sparkConf: SparkConf): Option[String] = {
    if (sparkConf.get(KEYTAB).nonEmpty) {
      Some(getKrbJaasParams(sparkConf))
    } else {
      None
    }
  }

  def getKrbJaasParams(sparkConf: SparkConf): String = {
    val serviceName = sparkConf.get(KAFKA_KERBEROS_SERVICE_NAME)
    require(serviceName.nonEmpty, "Kerberos service name must be defined")
    val keytab = sparkConf.get(KEYTAB)
    require(keytab.nonEmpty, "Keytab must be defined")
    val principal = sparkConf.get(PRINCIPAL)
    require(principal.nonEmpty, "Principal must be defined")

    val params =
      s"""
      |com.sun.security.auth.module.Krb5LoginModule required
      | useKeyTab=true
      | serviceName="${serviceName.get}"
      | keyTab="${keytab.get}"
      | principal="${principal.get}";
      """.stripMargin.replace("\n", "")
    logInfo(s"Krb JAAS params: $params")

    params
  }

  def getTokenJaasParams(sparkConf: SparkConf): Option[String] = {
    val token = UserGroupInformation.getCurrentUser().getCredentials.getToken(
      TokenUtil.TOKEN_SERVICE)
    if (token != null) {
      Some(getScramJaasParams(sparkConf, token))
    } else {
      None
    }
  }

  private def getScramJaasParams(
      sparkConf: SparkConf, token: Token[_ <: TokenIdentifier]): String = {
    val serviceName = sparkConf.get(KAFKA_KERBEROS_SERVICE_NAME)
    require(serviceName.nonEmpty, "Kerberos service name must be defined")
    val username = new String(token.getIdentifier)
    val password = new String(token.getPassword)

    val params =
      s"""
      |org.apache.kafka.common.security.scram.ScramLoginModule required
      | tokenauth=true
      | serviceName="${serviceName.get}"
      | username="$username"
      | password="$password";
      """.stripMargin.replace("\n", "")
    logInfo(s"Scram JAAS params: ${params.replaceAll("password=\".*\"", "password=\"[hidden]\"")}")

    params
  }
}
