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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command.{ShowTablesSuite => CommonShowTablesSuite}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

class ShowTablesSuite extends CommonShowTablesSuite {
  override def catalog: String = CatalogManager.SESSION_CATALOG_NAME
  override protected def defaultUsing: String = "USING parquet"
  override protected def showSchema: StructType = {
    new StructType()
      .add("database", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
      .add("isTemporary", BooleanType, nullable = false)
  }
  override protected def getRows(showRows: Seq[ShowRow]): Seq[Row] = {
    showRows.map {
      case ShowRow(namespace, table, isTemporary) => Row(namespace, table, isTemporary)
    }
  }

  // `SHOW TABLES` returns empty result in V2 catalog instead of throwing the exception.
  test("show table in a not existing namespace") {
    val msg = intercept[NoSuchDatabaseException] {
      runShowTablesSql(s"SHOW TABLES IN $catalog.unknown", Seq())
    }.getMessage
    assert(msg.contains("Database 'unknown' not found"))
  }

  test("namespace is not specified and the default catalog is set") {
    withSQLConf(SQLConf.DEFAULT_CATALOG.key -> catalog) {
      withTable("table") {
        spark.sql(s"CREATE TABLE table (id bigint, data string) $defaultUsing")
        runShowTablesSql("SHOW TABLES", Seq(ShowRow("default", "table", false)))
      }
    }
  }

  // `SHOW TABLES` from v2 catalog returns empty result.
  test("v1 SHOW TABLES list the temp views") {
    withSourceViews {
      runShowTablesSql(
        "SHOW TABLES FROM default",
        Seq(ShowRow("", "source", true), ShowRow("", "source2", true)))
    }
  }

  test("using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq())
    }
    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("namespace not specified and default v2 catalog not set - fallback to v1") {
    withSourceViews {
      runShowTablesSql(
        "SHOW TABLES",
        Seq(ShowRow("", "source", true), ShowRow("", "source2", true)))
      runShowTablesSql("SHOW TABLES LIKE '*2'", Seq(ShowRow("", "source2", true)))
    }
  }

  test("SHOW TABLE EXTENDED for default") {
    withSourceViews {
      val expected = Seq(Row("", "source", true), Row("", "source2", true))
      val schema = new StructType()
        .add("database", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
        .add("isTemporary", BooleanType, nullable = false)
        .add("information", StringType, nullable = false)

      val df = sql("SHOW TABLE EXTENDED FROM default LIKE '*source*'")
      val result = df.collect()
      val resultWithoutInfo = result.map { case Row(db, table, temp, _) => Row(db, table, temp) }

      assert(df.schema === schema)
      assert(resultWithoutInfo === expected)
      result.foreach { case Row(_, _, _, info: String) => assert(info.nonEmpty) }
    }
  }
}
