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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Represents an expression when used with SQL pipe operators like |> SELECT or |> EXTEND.
 * We use this to make sure that no aggregate functions exist in these expressions.
 */
case class PipeSelect(child: Expression, clause: String = "SELECT")
  extends UnaryExpression with RuntimeReplaceable {
  final override val nodePatterns: Seq[TreePattern] = Seq(PIPE_OPERATOR_SELECT, RUNTIME_REPLACEABLE)
  override def withNewChildInternal(newChild: Expression): Expression = PipeSelect(newChild)
  override lazy val replacement: Expression = {
    def visit(e: Expression): Unit = e match {
      case a: AggregateFunction =>
        // If we used the pipe operator |> SELECT clause to specify an aggregate function, this is
        // invalid; return an error message instructing the user to use the pipe operator
        // |> AGGREGATE clause for this purpose instead.
        throw QueryCompilationErrors.pipeOperatorContainsAggregateFunction(a, clause)
      case _: WindowExpression =>
        // Window functions are allowed in pipe SELECT operators, so do not traverse into children.
      case _ =>
        e.children.foreach(visit)
    }
    visit(child)
    child
  }
}

/**
 * Represents an expression when used with the SQL pipe operator |> AGGREGATE.
 * We use this to make sure that at least one aggregate function exists in each of these
 * expressions.
 */
case class PipeAggregate(child: Expression) extends UnaryExpression with RuntimeReplaceable {
  final override val nodePatterns: Seq[TreePattern] = Seq(RUNTIME_REPLACEABLE)
  override def withNewChildInternal(newChild: Expression): Expression = PipeAggregate(newChild)
  override lazy val replacement: Expression = {
    var foundAggregate = false
    def visit(e: Expression): Unit = {
      e match {
        case _: AggregateFunction =>
          foundAggregate = true
        case _ =>
          e.children.foreach(visit)
      }
    }
    visit(child)
    if (!foundAggregate) {
      throw QueryCompilationErrors.pipeOperatorAggregateExpressionContainsNoAggregateFunction(child)
    }
    child
  }
}

object PipeOperators {
  // These are definitions of query result clauses that can be used with the pipe operator.
  val clusterByClause = "CLUSTER BY"
  val distributeByClause = "DISTRIBUTE BY"
  val extendClause = "EXTEND"
  val limitClause = "LIMIT"
  val offsetClause = "OFFSET"
  val orderByClause = "ORDER BY"
  val selectClause = "SELECT"
  val sortByClause = "SORT BY"
  val sortByDistributeByClause = "SORT BY ... DISTRIBUTE BY ..."
  val windowClause = "WINDOW"
}
