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
package org.apache.spark.sql.connect.planner

import org.apache.spark.connect.proto
<<<<<<< HEAD
import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter, UsingJoin}
=======
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.plans.UsingJoin
// import org.apache.spark.sql.execution.streaming.state.Struct
 import org.apache.spark.connect.proto.Join.JoinType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.AttributeReference
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.plans.PlanTest
// import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
>>>>>>> 9ec54755db (update)
=======
// import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, PlanTest, RightOuter}
>>>>>>> 922b994805 (update)
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.dsl.expressions._
import org.apache.spark.sql.connect.dsl.plans._
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * This suite is based on connect DSL and test that given same dataframe operations, whether
 * connect could construct a proto plan that can be translated back, and after analyzed, be the
 * same as Spark dataframe's generated plan.
 */
class SparkConnectProtoSuite extends PlanTest with SparkConnectPlanTest {

  lazy val connectTestRelation =
    createLocalRelationProto(
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("name", StringType)()))

<<<<<<< HEAD
  lazy val connectTestRelation2 = createLocalRelationProto(
    Seq($"key".int, $"value".int, $"name".string))
=======
  lazy val connectTestRelation2 =
    createLocalRelationProto(
<<<<<<< HEAD
      Seq(AttributeReference("key", IntegerType)(), AttributeReference("value", StringType)()))
>>>>>>> 9ec54755db (update)
=======
      Seq(AttributeReference("id", IntegerType)(), AttributeReference("name", StringType)()))
>>>>>>> 922b994805 (update)

  lazy val sparkTestRelation: LocalRelation =
    LocalRelation(
      AttributeReference("id", IntegerType)(),
      AttributeReference("name", StringType)())

  lazy val sparkTestRelation2: LocalRelation =
<<<<<<< HEAD
    LocalRelation($"key".int, $"value".int, $"name".string)
=======
    LocalRelation(
<<<<<<< HEAD
      AttributeReference("key", IntegerType)(),
      AttributeReference("value", StringType)())
>>>>>>> 9ec54755db (update)
=======
      AttributeReference("id", IntegerType)(),
      AttributeReference("name", StringType)())
>>>>>>> 922b994805 (update)

  test("Basic select") {
    val connectPlan = connectTestRelation.select("id".protoAttr)
    val sparkPlan = sparkTestRelation.select("id")
    comparePlans(connectPlan, sparkPlan)
  }

  test("UnresolvedFunction resolution.") {
    assertThrows[IllegalArgumentException] {
      transform(connectTestRelation.select(callFunction("default.hex", Seq("id".protoAttr))))
    }

    val connectPlan =
      connectTestRelation.select(callFunction(Seq("default", "hex"), Seq("id".protoAttr)))

    assertThrows[UnsupportedOperationException] {
      analyzePlan(transform(connectPlan))
    }

    val validPlan = connectTestRelation.select(callFunction(Seq("hex"), Seq("id".protoAttr)))
    assert(analyzePlan(transform(validPlan)) != null)
  }

  test("Basic filter") {
    val connectPlan = connectTestRelation.where("id".protoAttr < 0)
    val sparkPlan = sparkTestRelation.where(Column("id") < 0)
    comparePlans(connectPlan, sparkPlan)
  }

  test("Basic joins with different join types") {
    val connectPlan = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.join(connectTestRelation2))
    }
    val sparkPlan = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)

    val connectPlan2 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(connectTestRelation.join(connectTestRelation2, condition = None))
    }
    val sparkPlan2 = sparkTestRelation.join(sparkTestRelation2, condition = None)
    comparePlans(connectPlan2.analyze, sparkPlan2.analyze, false)

  test("Basic joins with different join types") {
    val connectPlan = connectTestRelation.join(connectTestRelation2, Inner)
    val sparkPlan = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.join(connectTestRelation2, Inner, condition = None)
    val sparkPlan2 = sparkTestRelation.join(sparkTestRelation2)
    comparePlans(connectPlan2, sparkPlan2)


    for ((t, y) <- Seq(
        (JoinType.JOIN_TYPE_LEFT_OUTER, LeftOuter),
        (JoinType.JOIN_TYPE_RIGHT_OUTER, RightOuter),
        (JoinType.JOIN_TYPE_FULL_OUTER, FullOuter),
        (JoinType.JOIN_TYPE_LEFT_ANTI, LeftAnti),
        (JoinType.JOIN_TYPE_LEFT_SEMI, LeftSemi),
        (JoinType.JOIN_TYPE_INNER, Inner))) {

      val connectPlan3 = {
        import org.apache.spark.sql.connect.dsl.plans._
        transform(connectTestRelation.join(connectTestRelation2, t))
      }
      val sparkPlan3 = sparkTestRelation.join(sparkTestRelation2, y)
      comparePlans(connectPlan3.analyze, sparkPlan3.analyze, false)
    }

    val connectPlan4 = {
      import org.apache.spark.sql.connect.dsl.plans._
      transform(
        connectTestRelation.join(connectTestRelation2, JoinType.JOIN_TYPE_INNER, Seq("name")))
    }
    val sparkPlan4 = sparkTestRelation.join(sparkTestRelation2, UsingJoin(Inner, Seq("name")))
    comparePlans(connectPlan4.analyze, sparkPlan4.analyze, false)
  }

//  test("Basic joins with different join types") {
//    val connectPlan = connectTestRelation.join(connectTestRelation2)
//    val sparkPlan = sparkTestRelation.join(sparkTestRelation2)
//    comparePlans(connectPlan, sparkPlan)
//
//    val connectPlan2 = connectTestRelation.join(connectTestRelation2, condition = None)
//    val sparkPlan2 = sparkTestRelation.join(sparkTestRelation2)
//    comparePlans(connectPlan2, sparkPlan2)
//
//    for ((t, y) <- Seq(
//        (JoinType.JOIN_TYPE_LEFT_OUTER, LeftOuter),
//        (JoinType.JOIN_TYPE_RIGHT_OUTER, RightOuter),
//        (JoinType.JOIN_TYPE_FULL_OUTER, FullOuter),
//        (JoinType.JOIN_TYPE_LEFT_ANTI, LeftAnti),
//        (JoinType.JOIN_TYPE_LEFT_SEMI, LeftSemi),
//        (JoinType.JOIN_TYPE_INNER, Inner))) {
//      val connectPlan3 = connectTestRelation.join(connectTestRelation2, t)
//      val sparkPlan3 =
//        sparkTestRelation.join(sparkTestRelation2, JoinType = y.toString)
//      comparePlans(connectPlan3, sparkPlan3)
//    }
//  }

      val connectPlan3 = connectTestRelation.join(
        connectTestRelation2, UsingJoin(t, Seq("id")))
      val sparkPlan3 =
        sparkTestRelation.join(sparkTestRelation2, Seq("id"), y.toString)
      comparePlans(connectPlan3, sparkPlan3)
    }
  }


  test("Test sample") {
    val connectPlan = connectTestRelation.sample(0, 0.2, false, 1)
    val sparkPlan = sparkTestRelation.sample(false, 0.2 - 0, 1)
    comparePlans(connectPlan, sparkPlan)
  }

  test("column alias") {
    val connectPlan = connectTestRelation.select("id".protoAttr.as("id2"))
    val sparkPlan = sparkTestRelation.select(Column("id").alias("id2"))
    comparePlans(connectPlan, sparkPlan)
  }

//  test("Aggregate with more than 1 grouping expressions") {
//    val connectPlan =
//      connectTestRelation.groupBy("id".protoAttr, "name".protoAttr)()
//    val sparkPlan = sparkTestRelation.groupBy(Column("id"), Column("name"))
//      .agg(Column("id"), Column("name"))
//    comparePlans(connectPlan, sparkPlan)
//  }

//  test("Test as(alias: String)") {
//    val connectPlan = connectTestRelation.as("target_table")
//    val sparkPlan = sparkTestRelation.as("target_table")
//    comparePlans(connectPlan, sparkPlan)
//  }

//  test("Test StructType in LocalRelation") {
//    val connectPlan = createLocalRelationProtoByQualifiedAttributes(Seq("a".struct("id".int)))
//    val sparkPlan = LocalRelation(Struct(Column("a").as("id")))
//    comparePlans(connectPlan.analyze, sparkPlan.analyze, false)
//  }

  test("Test limit offset") {
    val connectPlan = connectTestRelation.limit(10)
    val sparkPlan = sparkTestRelation.limit(10)
    comparePlans(connectPlan, sparkPlan)

    val connectPlan2 = connectTestRelation.offset(2)
    val sparkPlan2 = sparkTestRelation.offset(2)
    comparePlans(connectPlan2, sparkPlan2)

    val connectPlan3 = connectTestRelation.limit(10).offset(2)
    val sparkPlan3 = sparkTestRelation.limit(10).offset(2)
    comparePlans(connectPlan3, sparkPlan3)

    val connectPlan4 = connectTestRelation.offset(2).limit(10)
    val sparkPlan4 = sparkTestRelation.offset(2).limit(10)
    comparePlans(connectPlan4, sparkPlan4)
  }

  private def createLocalRelationProtoByQualifiedAttributes(
      attrs: Seq[proto.Expression.QualifiedAttribute]): proto.Relation = {
    val localRelationBuilder = proto.LocalRelation.newBuilder()
    for (attr <- attrs) {
      localRelationBuilder.addAttributes(attr)
    }
    proto.Relation.newBuilder().setLocalRelation(localRelationBuilder.build()).build()
  }

  private def analyzePlan(plan: LogicalPlan): LogicalPlan = {
    val connectAnalyzed = analysis.SimpleAnalyzer.execute(plan)
    analysis.SimpleAnalyzer.checkAnalysis(connectAnalyzed)
    EliminateSubqueryAliases(connectAnalyzed)
    connectAnalyzed
  }

  private def comparePlans(connectPlan: proto.Relation, sparkPlan: DataFrame): Unit = {
    val connectAnalyzed = analyzePlan(transform(connectPlan))
    comparePlans(connectAnalyzed, sparkPlan.queryExecution.analyzed, false)
  }
}
