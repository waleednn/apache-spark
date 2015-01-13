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

package org.apache.spark.sql.hive.thriftserver

import java.io.IOException
import java.util.{List => JList}
import javax.security.auth.login.LoginException

import scala.collection.JavaConversions._

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.{AbstractService, Service, ServiceException}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.util.Utils

private[hive] class SparkSQLCLIService(hiveContext: HiveContext)
  extends CLIService
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "hiveConf", hiveConf)

    val sparkSqlSessionManager = new SparkSQLSessionManager(hiveContext)
    setSuperField(this, "sessionManager", sparkSqlSessionManager)
    addService(sparkSqlSessionManager)
    var sparkServiceUGI: UserGroupInformation = null

    if (ShimLoader.getHadoopShims.isSecurityEnabled) {
      try {
        HiveAuthFactory.loginFromKeytab(hiveConf)
        sparkServiceUGI = ShimLoader.getHadoopShims.getUGIForConf(hiveConf)
        HiveThriftServerShim.setServerUserName(sparkServiceUGI, this)
      } catch {
        case e @ (_: IOException | _: LoginException) =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }
    }

    initCompositeService(hiveConf)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(hiveContext.sparkContext.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(hiveConf: HiveConf, level: Int = 2) {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, level, "serviceList")
    serviceList.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, level + 1, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Log](this, level + 1, "LOG").info(s"Service: $getName is inited.")
  }
}
