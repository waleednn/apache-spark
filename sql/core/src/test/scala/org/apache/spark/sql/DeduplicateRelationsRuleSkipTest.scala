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

package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession, TestSQLSessionStateBuilder}

class DeduplicateRelationsRuleSkipTest extends QueryTest with SharedSparkSession {
  import testImplicits._
  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new CustomRuleTestSession(sparkConf)
  }

  test("basic skip  DedupRels rule") {
    val td = testData2
    val x = withExpectedSkipFlag( true, td.as("x"))
    val y = withExpectedSkipFlag( true, td.as("y"))
    withExpectedSkipFlag(false, x.join(y, $"x.a" === $"y.a", "inner").queryExecution.analyzed)

    val tab1 = testData2.as("testData2")
    val tab2 = testData3.as("testData3")
    withExpectedSkipFlag(true, tab1.join(tab2, usingColumns = Seq("a"), joinType = "fullouter")
      .queryExecution.analyzed)
  }

  test("test dedup rule skip flag for joins where dedup relations rule is not needed") {
    // join - join using
    var df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    var df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.join(df2, "int").queryExecution.analyzed)
    // join using multiple columns
    df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")
    withExpectedSkipFlag(true, df.join(df2, Seq("int", "int2")).queryExecution.analyzed)
    // join using multiple columns array
    df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")
    withExpectedSkipFlag(true, df.join(df2, Array("int", "int2")).queryExecution.analyzed)
    // join with select
    df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str_sort").as("df1")
    df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("df2")
    withExpectedSkipFlag(true,
      withExpectedSkipFlag(true,
        withExpectedSkipFlag(true, df.join(df2, $"df1.int" === $"df2.int", "outer").
          queryExecution.analyzed).select($"df1.int", $"df2.int2").queryExecution.analyzed).
          orderBy($"str_sort".asc, $"str".asc).queryExecution.analyzed)
    // cross join
    var df1 = Seq((1, "1"), (3, "3")).toDF("int", "str")
    df2 = Seq((2, "2"), (4, "4")).toDF("int", "str")
    withExpectedSkipFlag(true, df1.crossJoin(df2).queryExecution.analyzed)


  }

  test("test dedup rule skip flag for joins where dedup relations rule is needed") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val df = spark.range(2)
      withExpectedSkipFlag(false, df.join(df, df("id") <=> df("id")).queryExecution.analyzed)
    }
    var df1 = testData.select(testData("key")).as("df1")
    var df2 = testData.select(testData("key")).as("df2")
    withExpectedSkipFlag(false, df1.join(df2, $"df1.key" === $"df2.key").queryExecution.analyzed)

    var df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    withExpectedSkipFlag(true,
      withExpectedSkipFlag(false,
        withExpectedSkipFlag(true, df.as("x").queryExecution.analyzed).join(
          withExpectedSkipFlag(true, df.as("y").queryExecution.analyzed), $"x.str" === $"y.str").
            queryExecution.analyzed).groupBy("x.str").count().queryExecution.analyzed)
  }

  private def withExpectedSkipFlag[T](flag: Boolean, func : => T): T = {
    DedupFlagVerifierRule.expectedSkipFlag.set(Option(flag))
    try {
      func
    } finally {
      DedupFlagVerifierRule.expectedSkipFlag.set(None)
    }
  }
}

class CustomRuleTestSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  @transient
  override lazy val sessionState: SessionState = {
    new CustomRuleTestSQLSessionStateBuilder(this, None).build()
  }
}

class CustomRuleTestSQLSessionStateBuilder(session: SparkSession, state: Option[SessionState])
  extends TestSQLSessionStateBuilder(session, state) {
  override def newBuilder: NewBuilder = new CustomRuleTestSQLSessionStateBuilder(_, _)

  override protected def customResolutionRules: Seq[Rule[LogicalPlan]] = {
    super.customResolutionRules :+ DedupFlagVerifierRule
  }
}

object DedupFlagVerifierRule extends Rule[LogicalPlan] {

  val expectedSkipFlag: ThreadLocal[Option[Boolean]] = new ThreadLocal[Option[Boolean]]() {
    override protected def  initialValue(): Option[Boolean] = {
       None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    expectedSkipFlag.get().foreach(expected =>
    assert(expected == AnalysisContext.get.skipDedupRelations,
      s"expected flag = $expected, actual flag = ${AnalysisContext.get.skipDedupRelations}"))
    plan
  }
}
