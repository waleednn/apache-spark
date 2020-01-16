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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, Table, TableCatalog}

/**
 * Holds the name of a namespace that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedNamespace]] during analysis.
 */
case class UnresolvedNamespace(multipartIdentifier: Seq[String]) extends LeafNode {
  override lazy val resolved: Boolean = false

  override def output: Seq[Attribute] = Nil
}

/**
 * Holds the name of a table that has yet to be looked up in a catalog. It will be resolved to
 * [[ResolvedTable]] during analysis.
 */
case class UnresolvedTable(multipartIdentifier: Seq[String]) extends LeafNode {
  override lazy val resolved: Boolean = false

  override def output: Seq[Attribute] = Nil
}

/**
 * Holds the name of a table or view that has yet to be looked up in a catalog. It will
 * be resolved to [[ResolvedTable]] or [[ResolvedView]] during analysis.
 */
case class UnresolvedTableOrView(multipartIdentifier: Seq[String]) extends LeafNode {
  override lazy val resolved: Boolean = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved namespace.
 */
case class ResolvedNamespace(catalog: SupportsNamespaces, namespace: Seq[String])
  extends LeafNode {
  override def output: Seq[Attribute] = Nil
}

/**
 * A plan containing resolved table.
 */
case class ResolvedTable(catalog: TableCatalog, identifier: Identifier, table: Table)
  extends LeafNode {
  override lazy val output: Seq[Attribute] = table.schema().toAttributes
}

/**
 * A plan containing resolved (temp) views.
 */
// TODO: create a generic representation for temp view, v1 view and v2 view, after we add view
//       support to v2 catalog. For now we only need the identifier to fallback to v1 command.
case class ResolvedView(identifier: Identifier) extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
