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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

/**
 * This suite is used to test [[LogicalPlan]]'s `resolveOperators` and make sure it can correctly
 * skips sub-trees that have already been marked as analyzed.
 */
class LogicalPlanSuite extends SparkFunSuite {
  private var invocationCount = 0
  private val function: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p: Project =>
      invocationCount += 1
      p
  }

  private val testRelation = LocalRelation()

  test("resolveOperator runs on operators") {
    invocationCount = 0
    val plan = Project(Nil, testRelation)
    plan resolveOperators function

    assert(invocationCount === 1)
  }

  test("resolveOperator runs on operators recursively") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan resolveOperators function

    assert(invocationCount === 2)
  }

  test("resolveOperator skips all ready resolved plans") {
    invocationCount = 0
    val plan = Project(Nil, Project(Nil, testRelation))
    plan.foreach(_.setAnalyzed())
    plan resolveOperators function

    assert(invocationCount === 0)
  }

  test("resolveOperator skips partially resolved plans") {
    invocationCount = 0
    val plan1 = Project(Nil, testRelation)
    val plan2 = Project(Nil, plan1)
    plan1.foreach(_.setAnalyzed())
    plan2 resolveOperators function

    assert(invocationCount === 1)
  }

  test("isStreaming") {
    val relation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
    val incrementalRelation = new LocalRelation(
      Seq(AttributeReference("a", IntegerType, nullable = true)())) {
      override def isStreaming(): Boolean = true
    }

    case class TestBinaryRelation(left: LogicalPlan, right: LogicalPlan) extends BinaryNode {
      override def output: Seq[Attribute] = left.output ++ right.output
    }

    require(relation.isStreaming === false)
    require(incrementalRelation.isStreaming === true)
    assert(TestBinaryRelation(relation, relation).isStreaming === false)
    assert(TestBinaryRelation(incrementalRelation, relation).isStreaming === true)
    assert(TestBinaryRelation(relation, incrementalRelation).isStreaming === true)
    assert(TestBinaryRelation(incrementalRelation, incrementalRelation).isStreaming)
  }

  test("getAliasedConstraints") {
    val expressionNum = 100
    val aggExpression = (1 to expressionNum).map(i => Alias(Count(Literal(1)), s"cnt$i")())
    val aggPlan = Aggregate(Nil, aggExpression, LocalRelation())

    val expressions = aggPlan.validConstraints
    // The size of Aliased expression is n * (n - 1) / 2 + n
    assert( expressions.size === expressionNum * (expressionNum - 1) / 2 + expressionNum)
  }
}
