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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.PartitionsAlreadyExistException
import org.apache.spark.sql.connector.{InMemoryAtomicPartitionTable, InMemoryTableCatalog}
import org.apache.spark.sql.connector.expressions.{LogicalExpressions, NamedReference}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SupportsAtomicPartitionManagementSuite extends SparkFunSuite {

  private val ident: Identifier = Identifier.of(Array("ns"), "test_table")

  def ref(name: String): NamedReference = LogicalExpressions.parseReference(name)

  private val catalog: InMemoryTableCatalog = {
    val newCatalog = new InMemoryTableCatalog
    newCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    newCatalog.createTable(
      ident,
      new StructType()
        .add("id", IntegerType)
        .add("data", StringType)
        .add("dt", StringType),
      Array(LogicalExpressions.identity(ref("dt"))),
      util.Collections.emptyMap[String, String])
    newCatalog
  }

  test("createPartitions") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryAtomicPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)

    val partIdents = Array(InternalRow.apply("3"), InternalRow.apply("4"))
    partTable.createPartitions(
      partIdents,
      Array(new util.HashMap[String, String](), new util.HashMap[String, String]()))
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).nonEmpty)
    assert(partTable.partitionExists(InternalRow.apply("3")))
    assert(partTable.partitionExists(InternalRow.apply("4")))

    partTable.dropPartition(InternalRow.apply("3"))
    partTable.dropPartition(InternalRow.apply("4"))
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
  }

  test("createPartitions failed if partition already exists") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryAtomicPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)

    val partIdent = InternalRow.apply("4")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).nonEmpty)
    assert(partTable.partitionExists(partIdent))

    val partIdents = Array(InternalRow.apply("3"), InternalRow.apply("4"))
    assertThrows[PartitionsAlreadyExistException](
      partTable.createPartitions(
        partIdents,
        Array(new util.HashMap[String, String](), new util.HashMap[String, String]())))
    assert(!partTable.partitionExists(InternalRow.apply("3")))

    partTable.dropPartition(partIdent)
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
  }

  test("dropPartitions") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryAtomicPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)

    val partIdents = Array(InternalRow.apply("3"), InternalRow.apply("4"))
    partTable.createPartitions(
      partIdents,
      Array(new util.HashMap[String, String](), new util.HashMap[String, String]()))
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).nonEmpty)
    assert(partTable.partitionExists(InternalRow.apply("3")))
    assert(partTable.partitionExists(InternalRow.apply("4")))

    partTable.dropPartitions(partIdents)
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
  }

  test("dropPartitions failed if partition not exists") {
    val table = catalog.loadTable(ident)
    val partTable = new InMemoryAtomicPartitionTable(
      table.name(), table.schema(), table.partitioning(), table.properties())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)

    val partIdent = InternalRow.apply("4")
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).length == 1)

    val partIdents = Array(InternalRow.apply("3"), InternalRow.apply("4"))
    assert(!partTable.dropPartitions(partIdents))
    assert(partTable.partitionExists(partIdent))

    partTable.dropPartition(partIdent)
    assert(partTable.listPartitionIdentifiers(InternalRow.empty).isEmpty)
  }
}
