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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command.v1

class AlterTableRenamePartitionSuite
  extends v1.AlterTableRenamePartitionSuiteBase
  with CommandSuiteBase {

  test("target partition exists") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createSinglePartTable(t)
      sql(s"INSERT INTO $t PARTITION (id = 2) SELECT 'def'")
      checkPartitions(t, Map("id" -> "1"), Map("id" -> "2"))
      // TODO(SPARK-33862): Throw PartitionAlreadyExistsException
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION (id = 1) RENAME TO PARTITION (id = 2)")
      }.getMessage
      assert(errMsg.contains("Partition already exists"))
    }
  }
}
