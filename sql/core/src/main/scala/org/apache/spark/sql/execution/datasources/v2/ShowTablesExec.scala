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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog}
import org.apache.spark.sql.catalog.v2.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.ShowTablesSchema
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing tables.
 */
case class ShowTablesExec(catalog: TableCatalog, ident: Identifier, pattern: Option[String])
  extends LeafExecNode {
  override def output: Seq[AttributeReference] = ShowTablesSchema.attributes()

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    val encoder = RowEncoder(ShowTablesSchema.schema).resolveAndBind()

    val tables = catalog.listTables(ident.namespace() :+ ident.name())
    tables.map { table =>
      if (pattern.map(StringUtils.filterPattern(Seq(table.name()), _).nonEmpty).getOrElse(true)) {
        rows += encoder
          .toRow(
            new GenericRowWithSchema(
              // TODO: there is no v2 catalog API to retrieve 'isTemporary',
              //  and it is set to false for the time being.
              Array(table.namespace().quoted, table.name(), false),
              ShowTablesSchema.schema))
          .copy()
      }
    }

    sparkContext.parallelize(rows, 1)
  }
}
