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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewDescription
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing view create statement.
 */
case class ShowCreateViewExec(
    desc: ViewDescription,
    output: Seq[Attribute]) extends V2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    val schema = desc.schema.map(_.name).mkString("(", ", ", ")")
    val create = s"CREATE VIEW ${desc.ident} $schema AS\n${desc.query}\n"
    Seq(toCatalystRow(create))
  }
}
