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

package org.apache.spark.sql.jdbc

import javax.security.auth.login.Configuration

import com.spotify.docker.client.messages.{ContainerConfig, HostConfig}

import org.apache.spark.sql.execution.datasources.jdbc.connection.SecureConnectionProvider
import org.apache.spark.tags.DockerTest

@DockerTest
class MariaDBKrbIntegrationSuite extends DockerKrbJDBCIntegrationSuite {
  override protected val userName = s"mariadb/$dockerIp"
  override protected val keytabFileName = "mariadb.keytab"

  override val db = new DatabaseOnDocker {
    // If you change `imageName`, you need to update the version of `mariadb-plugin-gssapi-server`
    // in `resources/mariadb_docker_entrypoint.sh` accordingly.
    override val imageName = sys.env.getOrElse("MARIADB_DOCKER_IMAGE_NAME", "mariadb:10.5.8")
    override val env = Map(
      "MYSQL_ROOT_PASSWORD" -> "rootpass",
      "MARIADB_GSSAPI_VERSION" ->
        sys.env.getOrElse("MARIADB_GSSAPI_VERSION", "1:10.5.8+maria~focal")
    )
    override val usesIpc = false
    override val jdbcPort = 3306

    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:mysql://$ip:$port/mysql?user=$principal"

    override def getEntryPoint: Option[String] =
      Some("/docker-entrypoint/mariadb_docker_entrypoint.sh")

    override def beforeContainerStart(
        hostConfigBuilder: HostConfig.Builder,
        containerConfigBuilder: ContainerConfig.Builder): Unit = {
      copyExecutableResource("mariadb_docker_entrypoint.sh", entryPointDir, replaceIp)
      copyExecutableResource("mariadb_krb_setup.sh", initDbDir, replaceIp)

      hostConfigBuilder.appendBinds(
        HostConfig.Bind.from(entryPointDir.getAbsolutePath)
          .to("/docker-entrypoint").readOnly(true).build(),
        HostConfig.Bind.from(initDbDir.getAbsolutePath)
          .to("/docker-entrypoint-initdb.d").readOnly(true).build()
      )
    }
  }

  override protected def setAuthentication(keytabFile: String, principal: String): Unit = {
    val config = new SecureConnectionProvider.JDBCConfiguration(
      Configuration.getConfiguration, "Krb5ConnectorContext", keytabFile, principal)
    Configuration.setConfiguration(config)
  }
}
