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
package org.apache.spark.deploy.k8s

private[spark] object Constants {

  // Labels
  val SPARK_APP_ID_LABEL = "spark-app-selector"
  val SPARK_EXECUTOR_ID_LABEL = "spark-exec-id"
  val SPARK_ROLE_LABEL = "spark-role"
  val SPARK_POD_DRIVER_ROLE = "driver"
  val SPARK_POD_EXECUTOR_ROLE = "executor"

  // Annotations
  val SPARK_APP_NAME_ANNOTATION = "spark-app-name"

  // Credentials secrets
  val DRIVER_CREDENTIALS_SECRETS_BASE_DIR =
    "/mnt/secrets/spark-kubernetes-credentials"
  val DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME = "ca-cert"
  val DRIVER_CREDENTIALS_CA_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CA_CERT_SECRET_NAME"
  val DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME = "client-key"
  val DRIVER_CREDENTIALS_CLIENT_KEY_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_KEY_SECRET_NAME"
  val DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME = "client-cert"
  val DRIVER_CREDENTIALS_CLIENT_CERT_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_CLIENT_CERT_SECRET_NAME"
  val DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME = "oauth-token"
  val DRIVER_CREDENTIALS_OAUTH_TOKEN_PATH =
    s"$DRIVER_CREDENTIALS_SECRETS_BASE_DIR/$DRIVER_CREDENTIALS_OAUTH_TOKEN_SECRET_NAME"
  val DRIVER_CREDENTIALS_SECRET_VOLUME_NAME = "kubernetes-credentials"

  // Default and fixed ports
  val DEFAULT_DRIVER_PORT = 7078
  val DEFAULT_BLOCKMANAGER_PORT = 7079
  val DRIVER_PORT_NAME = "driver-rpc-port"
  val BLOCK_MANAGER_PORT_NAME = "blockmanager"
  val EXECUTOR_PORT_NAME = "executor"

  // Environment Variables
  val ENV_EXECUTOR_PORT = "SPARK_EXECUTOR_PORT"
  val ENV_DRIVER_URL = "SPARK_DRIVER_URL"
  val ENV_EXECUTOR_CORES = "SPARK_EXECUTOR_CORES"
  val ENV_EXECUTOR_MEMORY = "SPARK_EXECUTOR_MEMORY"
  val ENV_APPLICATION_ID = "SPARK_APPLICATION_ID"
  val ENV_EXECUTOR_ID = "SPARK_EXECUTOR_ID"
  val ENV_EXECUTOR_POD_IP = "SPARK_EXECUTOR_POD_IP"
  val ENV_MOUNTED_CLASSPATH = "SPARK_MOUNTED_CLASSPATH"
  val ENV_JAVA_OPT_PREFIX = "SPARK_JAVA_OPT_"
  val ENV_CLASSPATH = "SPARK_CLASSPATH"
  val ENV_DRIVER_BIND_ADDRESS = "SPARK_DRIVER_BIND_ADDRESS"
  val ENV_SPARK_CONF_DIR = "SPARK_CONF_DIR"
  val ENV_SPARK_USER = "SPARK_USER"
  // Spark app configs for containers
  val SPARK_CONF_VOLUME = "spark-conf-volume"
  val SPARK_CONF_DIR_INTERNAL = "/opt/spark/conf"
  val SPARK_CONF_FILE_NAME = "spark.properties"
  val SPARK_CONF_PATH = s"$SPARK_CONF_DIR_INTERNAL/$SPARK_CONF_FILE_NAME"
  val ENV_HADOOP_TOKEN_FILE_LOCATION = "HADOOP_TOKEN_FILE_LOCATION"

  // BINDINGS
  val ENV_PYSPARK_PRIMARY = "PYSPARK_PRIMARY"
  val ENV_PYSPARK_FILES = "PYSPARK_FILES"
  val ENV_PYSPARK_ARGS = "PYSPARK_APP_ARGS"
  val ENV_PYSPARK_MAJOR_PYTHON_VERSION = "PYSPARK_MAJOR_PYTHON_VERSION"

  // Miscellaneous
  val KUBERNETES_MASTER_INTERNAL_URL = "https://kubernetes.default.svc"
  val DRIVER_CONTAINER_NAME = "spark-kubernetes-driver"
  val MEMORY_OVERHEAD_MIN_MIB = 384L

  // Hadoop Configuration
  val HADOOP_FILE_VOLUME = "hadoop-properties"
  val HADOOP_CONF_DIR_PATH = "/etc/hadoop/conf"
  val ENV_HADOOP_CONF_DIR = "HADOOP_CONF_DIR"
  val HADOOP_CONF_DIR_LOC = "spark.kubernetes.hadoop.conf.dir"
  val HADOOP_CONFIG_MAP_SPARK_CONF_NAME =
    "spark.kubernetes.hadoop.executor.hadoopConfigMapName"

  // Kerberos Configuration
  val KERBEROS_DELEGEGATION_TOKEN_SECRET_NAME =
    "spark.kubernetes.kerberos.delegation-token-secret-name"
  val KERBEROS_KEYTAB_SECRET_NAME =
    "spark.kubernetes.kerberos.key-tab-secret-name"
  val KERBEROS_KEYTAB_SECRET_KEY =
    "spark.kubernetes.kerberos.key-tab-secret-key"
  val KERBEROS_SPARK_USER_NAME =
    "spark.kubernetes.kerberos.spark-user-name"
  val KERBEROS_SECRET_LABEL_PREFIX =
    "hadoop-tokens"
  val SPARK_HADOOP_PREFIX = "spark.hadoop."
  val HADOOP_SECURITY_AUTHENTICATION =
    SPARK_HADOOP_PREFIX + "hadoop.security.authentication"

  // Kerberos Token-Refresh Server
  val KERBEROS_REFRESH_LABEL_KEY = "refresh-hadoop-tokens"
  val KERBEROS_REFRESH_LABEL_VALUE = "yes"

  // Hadoop credentials secrets for the Spark app.
  val SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR = "/mnt/secrets/hadoop-credentials"
  val SPARK_APP_HADOOP_SECRET_VOLUME_NAME = "hadoop-secret"
}
