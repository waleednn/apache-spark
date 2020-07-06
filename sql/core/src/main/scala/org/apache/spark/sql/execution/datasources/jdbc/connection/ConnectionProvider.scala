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

package org.apache.spark.sql.execution.datasources.jdbc.connection

import java.sql.{Connection, Driver}
import java.util.ServiceLoader

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.util.Utils

private[jdbc] object ConnectionProvider extends Logging {
  val providers = loadProviders()

  private def loadProviders(): Seq[JdbcConnectionProvider] = {
    val loader = ServiceLoader.load(classOf[JdbcConnectionProvider],
      Utils.getContextOrSparkClassLoader)
    val providers = mutable.ArrayBuffer[JdbcConnectionProvider]()

    val iterator = loader.iterator
    while (iterator.hasNext) {
      try {
        val provider = iterator.next
        logDebug(s"Loaded built in provider: $provider")
        providers += provider
      } catch {
        case t: Throwable =>
          logError(s"Failed to load built in provider.", t)
      }
    }
    providers
  }

  def create(driver: Driver, options: JDBCOptions): Connection = {
    val filteredProviders = providers.filter(_.canHandle(driver, options))
    logDebug(s"Filtered providers: $filteredProviders")
    require(filteredProviders.size == 1,
      "JDBC connection initiated but not exactly one connection provider found which can handle it")
    filteredProviders.head.getConnection(driver, options)
  }
}
