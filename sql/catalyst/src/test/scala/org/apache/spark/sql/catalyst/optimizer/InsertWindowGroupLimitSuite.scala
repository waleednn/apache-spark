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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, DenseRank, Literal, NthValue, NTile, Rank, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class InsertWindowGroupLimitSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert WindowGroupLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates,
        InsertWindowGroupLimit) :: Nil
  }

  private object WithoutOptimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Insert WindowGroupLimit", FixedPoint(10),
        CollapseProject,
        RemoveNoopOperators,
        PushDownPredicates) :: Nil
  }

  private val testRelation = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int, "c".attr.int),
    1.to(10).map(i => Row(i % 3, 2, i)))
  private val a = testRelation.output(0)
  private val b = testRelation.output(1)
  private val c = testRelation.output(2)
  private val rankLikeFunctions = Seq(RowNumber(), Rank(c :: Nil), DenseRank(c :: Nil))
  private val unsupportedFunctions = Seq(new NthValue(c, Literal(1)), new NTile())
  private val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)
  private val supportedConditions = Seq($"rn" === 2, $"rn" < 3, $"rn" <= 2)
  private val unsupportedConditions = Seq($"rn" > 2, $"rn" === 1 || b > 2)

  test("window without filter") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      for (function <- rankLikeFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("spark.sql.window.group.limit.enabled = false") {
    for (condition <- supportedConditions; function <- rankLikeFunctions) {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(function,
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
          .where(condition)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("Insert window group limit node for top-k computation") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      for (condition <- supportedConditions; function <- rankLikeFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        val correctAnswer =
          testRelation
            .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 2)
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("Insert window group limit node for top-k computation: supported condition && b > 0") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      for (condition <- supportedConditions; function <- rankLikeFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition && b > 0)

        val correctAnswer =
          testRelation
            .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 2)
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition && b > 0)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("Unsupported conditions") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      for (condition <- unsupportedConditions; function <- rankLikeFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("Unsupported window functions") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      for (condition <- supportedConditions; function <- unsupportedFunctions) {
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where(condition)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("Insert window group limit node for top-k computation: Empty partitionSpec") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      rankLikeFunctions.foreach { function =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn <= 4)

        val correctAnswer =
          testRelation
            .windowGroupLimit(Nil, c.desc :: Nil, function, 4)
            .select(a, b, c,
              windowExpr(function,
                windowSpec(Nil, c.desc :: Nil, windowFrame)).as("rn"))
            .where('rn <= 4)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("Insert window group limit node for top-k computation: multiple rank-like functions") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      rankLikeFunctions.foreach { function =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn2"))
            .where('rn < 2 && 'rn2 === 3)

        val correctAnswer =
          testRelation
            .windowGroupLimit(a :: Nil, c.desc :: Nil, function, 3)
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn2"))
            .where('rn < 2 && 'rn2 === 3)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(correctAnswer.analyze))
      }
    }
  }

  test("Insert window group limit node for top-k computation: different window specification") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      rankLikeFunctions.foreach { function =>
        val originalQuery =
          testRelation
            .select(a, b, c,
              windowExpr(function,
                windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
              windowExpr(function,
                windowSpec(a :: Nil, c.asc :: Nil, windowFrame)).as("rn2"))
            .where('rn < 2 && 'rn2 === 1)

        comparePlans(
          Optimize.execute(originalQuery.analyze),
          WithoutOptimize.execute(originalQuery.analyze))
      }
    }
  }

  test("multiple different rank-like window function") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      val originalQuery =
        testRelation
          .select(a, b, c,
            windowExpr(RowNumber(),
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"),
            windowExpr(Rank(c :: Nil),
              windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rank"))
          .where('rn < 2 && 'rank === 1)

      comparePlans(
        Optimize.execute(originalQuery.analyze),
        WithoutOptimize.execute(originalQuery.analyze))
    }
  }

  test("Insert window group limit node for top-k computation: empty relation") {
    withSQLConf(SQLConf.WINDOW_GROUP_LIMIT_ENABLE.key -> "true") {
      Seq($"rn" === 0, $"rn" < 1, $"rn" <= 0).foreach { condition =>
        rankLikeFunctions.foreach { function =>
          val originalQuery =
            testRelation
              .select(a, b, c,
                windowExpr(function,
                  windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
              .where(condition)

          val correctAnswer = LocalRelation(originalQuery.output)

          comparePlans(
            Optimize.execute(originalQuery.analyze),
            WithoutOptimize.execute(correctAnswer.analyze))
        }
      }
    }
  }
}
