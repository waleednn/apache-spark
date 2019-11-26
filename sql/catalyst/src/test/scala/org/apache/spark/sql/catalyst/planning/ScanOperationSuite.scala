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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualTo, Literal, MonotonicallyIncreasingID, Rand}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, OneRowRelation, Project}
import org.apache.spark.sql.types.{DoubleType, StringType}

case class TestRelation() extends LeafNode {
  override def output: Seq[Attribute] = Seq(
    AttributeReference("a", StringType)(),
    AttributeReference("b", StringType)())
}

class ScanOperationSuite extends SparkFunSuite {
  test("Collect projects and filters through non-deterministic expressions") {
    val relation = TestRelation()
    val colA = relation.output(0)
    val colB = relation.output(1)
    val aliasR = Alias(Rand(1), "r")()
    val aliasId = Alias(MonotonicallyIncreasingID(), "id")()
    val colR = AttributeReference("r", DoubleType)(aliasR.exprId, aliasR.qualifier)

    // Project with a non-deterministic field and a deterministic child Filter
    val project1 = Project(Seq(colB, aliasR), Filter(EqualTo(colA, Literal(1)), relation))
    project1 match {
      case ScanOperation(projects, filters, _: TestRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colB)
        assert(projects(1) === aliasR)
        assert(filters.size === 1)
    }

    // Project with all deterministic fields but a non-deterministic Filter
    val project2 = Project(Seq(colA, colB), Filter(EqualTo(aliasR, Literal(1)), relation))
    project2 match {
      case ScanOperation(projects, filters, _: TestRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === colB)
        assert(filters.size === 1)
    }

    // Project which has the same non-deterministic expression with its child Project
    val project3 = Project(Seq(colA, colR), Project(Seq(colA, aliasR), relation))
    assert(ScanOperation.unapply(project3).isEmpty)

    // Project which has different non-deterministic expressions with its child Project
    val project4 = Project(Seq(colA, aliasId), Project(Seq(colA, aliasR), relation))
    project4 match {
      case ScanOperation(projects, _, _: TestRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === aliasId)
    }

    // Filter which has the same non-deterministic expression with its child Project
    val filter1 = Filter(EqualTo(colR, Literal(1)), Project(Seq(colA, aliasR), relation))
    assert(ScanOperation.unapply(filter1).isEmpty)

    // Filter which doesn't have the same non-deterministic expression with its child Project
    val filter2 = Filter(EqualTo(colA, Literal(1)), Project(Seq(colA, aliasR), relation))
    filter2 match {
      case ScanOperation(projects, filters, _: TestRelation) =>
        assert(projects.size === 2)
        assert(projects(0) === colA)
        assert(projects(1) === aliasR)
        assert(filters.size === 1)
    }

    // Filter which has a non-deterministic child Filter
    val filter3 = Filter(EqualTo(colA, Literal(1)), Filter(EqualTo(aliasR, Literal(1)), relation))
    assert(ScanOperation.unapply(filter3).isEmpty)

  }
}
