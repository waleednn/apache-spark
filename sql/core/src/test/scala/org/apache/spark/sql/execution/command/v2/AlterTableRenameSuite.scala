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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. RENAME` command to check V2 table catalogs.
 */
class AlterTableRenameSuite extends command.AlterTableRenameSuiteBase with CommandSuiteBase {
  test("destination namespace is different") {
    withNamespaceAndTable("dst_ns", "dst_tbl") { dst =>
      withNamespace("src_ns") {
        sql(s"CREATE NAMESPACE $catalog.src_ns")
        val src = dst.replace("dst", "src")
        sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
        sql(s"ALTER TABLE $src RENAME TO dst_ns.dst_tbl")
        checkTables("dst_ns", "dst_tbl")
      }
    }
  }

  test("include catalog in the destination table") {
    withNamespaceAndTable("ns", "dst_tbl", catalog) { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")

      sql(s"ALTER TABLE $src RENAME TO $catalog.ns.dst_tbl")
      checkTables("ns", "dst_tbl")
    }
  }
}
