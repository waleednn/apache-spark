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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{DomainJoin, LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class OptimizeOneRowRelationSubquerySuite extends PlanTest {

  private var optimizeOneRowRelationSubqueryEnabled: Boolean = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    optimizeOneRowRelationSubqueryEnabled =
      SQLConf.get.getConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY)
    SQLConf.get.setConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY, true)
  }

  protected override def afterAll(): Unit = {
    SQLConf.get.setConf(SQLConf.OPTIMIZE_ONE_ROW_RELATION_SUBQUERY,
      optimizeOneRowRelationSubqueryEnabled)
    super.afterAll()
  }

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subquery", Once,
        OptimizeOneRowRelationSubquery,
        PullupCorrelatedPredicates) :: Nil
  }

  private def assertHasDomainJoin(plan: LogicalPlan): Unit = {
    assert(plan.collectWithSubqueries { case d: DomainJoin => d }.nonEmpty,
      s"Plan does not contain DomainJoin:\n$plan")
  }

  val t0 = OneRowRelation()
  val a = 'a.int
  val b = 'b.int
  val t1 = LocalRelation(a, b)
  val t2 = LocalRelation('c.int, 'd.int)

  test("Optimize scalar subquery with a single project") {
    // SELECT (SELECT a) FROM t1
    val query = t1.select(ScalarSubquery(t0.select('a)).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = t1.select('a.as("sub"))
    comparePlans(optimized, correctAnswer.analyze)
  }

  test("Optimize lateral subquery with a single project") {
    Seq(Inner, LeftOuter, Cross).foreach { joinType =>
      // SELECT * FROM t1 JOIN LATERAL (SELECT a, b)
      val query = t1.lateralJoin(t0.select('a, 'b), joinType, None)
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer = t1.select('a, 'b, 'a.as("a"), 'b.as("b"))
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("Optimize subquery with subquery alias") {
    val inner = t0.select('a).as("t2")
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = t1.select('a.as("sub"))
    comparePlans(optimized, correctAnswer.analyze)
  }

  test("Optimize scalar subquery with multiple projects") {
    // SELECT (SELECT a1 + b1 FROM (SELECT a AS a1, b AS b1)) FROM t1
    val inner = t0.select('a.as("a1"), 'b.as("b1")).select(('a1 + 'b1).as("c"))
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Project(Alias(Alias(a + b, "c")(), "sub")() :: Nil, t1)
    comparePlans(optimized, correctAnswer)
  }

  test("Optimize lateral subquery with multiple projects") {
    Seq(Inner, LeftOuter, Cross).foreach { joinType =>
      val inner = t0.select('a.as("a1"), 'b.as("b1"))
        .select(('a1 + 'b1).as("c1"), ('a1 - 'b1).as("c2"))
      val query = t1.lateralJoin(inner, joinType, None)
      val optimized = Optimize.execute(query.analyze)
      val correctAnswer = t1.select('a, 'b, ('a + 'b).as("c1"), ('a - 'b).as("c2"))
      comparePlans(optimized, correctAnswer.analyze)
    }
  }

  test("Optimize subquery with nested correlated subqueries") {
    // SELECT (SELECT (SELECT b) FROM (SELECT a AS b)) FROM t1
    val inner = t0.select('a.as("b")).select(ScalarSubquery(t0.select('b)).as("s"))
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = Project(Alias(Alias(a, "s")(), "sub")() :: Nil, t1)
    comparePlans(optimized, correctAnswer)
  }

  test("Batch should be idempotent") {
    // SELECT (SELECT 1 WHERE a = a + 1) FROM t1
    val inner = t0.select(1).where('a === 'a + 1)
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    val doubleOptimized = Optimize.execute(optimized)
    comparePlans(optimized, doubleOptimized, checkAnalysis = false)
  }

  test("Should not optimize scalar subquery with operators other than project") {
    // SELECT (SELECT a AS a1 WHERE a = 1) FROM t1
    val inner = t0.where('a === 1).select('a.as("a1"))
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    assertHasDomainJoin(optimized)
  }

  test("Should not optimize subquery with non-deterministic expressions") {
    // SELECT (SELECT r FROM (SELECT a + rand() AS r)) FROM t1
    val inner = t0.select(('a + rand(0)).as("r")).select('r)
    val query = t1.select(ScalarSubquery(inner).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    assertHasDomainJoin(optimized)
  }

  test("Should not optimize lateral join with non-empty join conditions") {
    Seq(Inner, LeftOuter).foreach { joinType =>
      // SELECT * FROM t1 JOIN LATERAL (SELECT a AS a1, b AS b1) ON a = b1
      val query = t1.lateralJoin(t0.select('a.as("a1"), 'b.as("b1")), joinType, Some('a === 'b1))
      val optimized = Optimize.execute(query.analyze)
      assertHasDomainJoin(optimized)
    }
  }

  test("Should not optimize subquery with nested subqueries") {
    // SELECT (SELECT (SELECT a WHERE a = 1) FROM (SELECT a AS a)) FROM t1
    val inner = t0.select('a).where('a === 1)
    val subquery = t0.select('a.as("a"))
      .select(ScalarSubquery(inner).as("s")).select('s + 1)
    val query = t1.select(ScalarSubquery(subquery).as("sub"))
    val optimized = Optimize.execute(query.analyze)
    assertHasDomainJoin(optimized)
  }
}
