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

package org.apache.spark.sql.execution.command

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.test.SQLTestUtils

trait AlterTableAddPartitionSuiteBase extends QueryTest with SQLTestUtils {
  protected def version: String
  protected def catalog: String
  protected def defaultUsing: String

  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    super.test(s"ALTER TABLE .. ADD PARTITION $version: " + testName, testTags: _*)(testFun)
  }

  protected def checkPartitions(t: String, expected: Map[String, String]*): Unit = {
    val partitions = sql(s"SHOW PARTITIONS $t")
      .collect()
      .toSet
      .map((row: Row) => row.getString(0))
      .map(PartitioningUtils.parsePathFragment)
    assert(partitions === expected.toSet)
  }
  protected def checkLocation(t: String, spec: TablePartitionSpec, expected: String): Unit

  test("one partition") {
    withNamespace(s"$catalog.ns") {
      sql(s"CREATE NAMESPACE $catalog.ns")
      val t = s"$catalog.ns.tbl"
      withTable(t) {
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing PARTITIONED BY (id)")
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")

        checkPartitions(t, Map("id" -> "1"))
        checkLocation(t, Map("id" -> "1"), "loc")
      }
    }
  }
}
