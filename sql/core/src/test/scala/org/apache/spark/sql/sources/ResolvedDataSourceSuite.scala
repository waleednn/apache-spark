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

package org.apache.spark.sql.sources

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSQLContext

class ResolvedDataSourceSuite extends SparkFunSuite with SharedSQLContext {
  private def getProvidingClass(name: String): Class[_] =
    DataSource(
      sparkSession = spark,
      className = name,
      options = Map(DateTimeUtils.TIMEZONE_OPTION -> DateTimeUtils.defaultTimeZone().getID)
    ).providingClass

  test("jdbc") {
    assert(
      getProvidingClass("jdbc") ===
      classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.jdbc") ===
      classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
    assert(
      getProvidingClass("org.apache.spark.sql.jdbc") ===
        classOf[org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider])
  }

  test("json") {
    assert(
      getProvidingClass("json") ===
      classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.json") ===
        classOf[org.apache.spark.sql.execution.datasources.json.JsonFileFormat])
  }

  test("parquet") {
    assert(
      getProvidingClass("parquet") ===
      classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.execution.datasources.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
    assert(
      getProvidingClass("org.apache.spark.sql.parquet") ===
        classOf[org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat])
  }

  test("csv") {
    assert(
      getProvidingClass("csv") ===
        classOf[org.apache.spark.sql.execution.datasources.csv.CSVFileFormat])
    assert(
      getProvidingClass("com.databricks.spark.csv") ===
        classOf[org.apache.spark.sql.execution.datasources.csv.CSVFileFormat])
  }

  test("error message for unknown data sources") {
    val error = intercept[ClassNotFoundException] {
      getProvidingClass("asfdwefasdfasdf")
    }
    assert(error.getMessage.contains("Failed to find data source: asfdwefasdfasdf."))
  }

  test("support custom mapping for data source names") {
    val csv = classOf[org.apache.spark.sql.execution.datasources.csv.CSVFileFormat]

    // Map a new data source name to a built-in data source
    withSQLConf("spark.sql.datasource.map.myDatasource" -> csv.getCanonicalName) {
      assert(getProvidingClass("myDatasource") === csv)
    }

    // Map a existing built-in data source name to new data source
    val testDataSource = classOf[TestDataSource]
    withSQLConf(
      "spark.sql.datasource.map.org.apache.spark.sql.avro" -> testDataSource.getCanonicalName,
      "spark.sql.datasource.map.com.databricks.spark.csv" -> testDataSource.getCanonicalName,
      "spark.sql.datasource.map.com.databricks.spark.avro" -> testDataSource.getCanonicalName) {
      assert(getProvidingClass("org.apache.spark.sql.avro") === testDataSource)
      assert(getProvidingClass("com.databricks.spark.csv") === testDataSource)
      assert(getProvidingClass("com.databricks.spark.avro") === testDataSource)
    }
  }
}

class TestDataSource extends DataSourceRegister {
  override def shortName(): String = "test"
}
