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

import javax.security.auth.login.Configuration

class ConnectionProviderSuite extends ConnectionProviderSuiteBase {
  test("All built-in provides must be loaded") {
    IntentionallyFaultyConnectionProvider.constructed = false
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[BasicConnectionProvider]))
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[DB2ConnectionProvider]))
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[MariaDBConnectionProvider]))
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[MSSQLConnectionProvider]))
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[PostgresConnectionProvider]))
    assert(ConnectionProvider.providers.exists(_.isInstanceOf[OracleConnectionProvider]))
    assert(IntentionallyFaultyConnectionProvider.constructed)
    assert(!ConnectionProvider.providers.
      exists(_.isInstanceOf[IntentionallyFaultyConnectionProvider]))
    assert(ConnectionProvider.providers.size === 6)
  }

  test("Multiple security configs must be reachable") {
    Configuration.setConfiguration(null)
    val postgresProvider = new PostgresConnectionProvider()
    val postgresDriver = registerDriver(postgresProvider.driverClass)
    val postgresOptions = options("jdbc:postgresql://localhost/postgres")
    val postgresAppEntry = postgresProvider.appEntry(postgresDriver, postgresOptions)
    val db2Provider = new DB2ConnectionProvider()
    val db2Driver = registerDriver(db2Provider.driverClass)
    val db2Options = options("jdbc:db2://localhost/db2")
    val db2AppEntry = db2Provider.appEntry(db2Driver, db2Options)

    // Make sure no authentication for the databases are set
    val oldConfig = Configuration.getConfiguration
    assert(oldConfig.getAppConfigurationEntry(postgresAppEntry) == null)
    assert(oldConfig.getAppConfigurationEntry(db2AppEntry) == null)

    postgresProvider.setAuthenticationConfigIfNeeded(postgresDriver, postgresOptions)
    db2Provider.setAuthenticationConfigIfNeeded(db2Driver, db2Options)

    // Make sure authentication for the databases are set
    val newConfig = Configuration.getConfiguration
    assert(oldConfig != newConfig)
    assert(newConfig.getAppConfigurationEntry(postgresAppEntry) != null)
    assert(newConfig.getAppConfigurationEntry(db2AppEntry) != null)
  }
}
