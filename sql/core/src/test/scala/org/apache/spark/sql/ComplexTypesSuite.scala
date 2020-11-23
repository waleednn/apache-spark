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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.CreateNamedStruct
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, StructType}

class ComplexTypesSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.range(10).selectExpr(
      "id + 1 as i1", "id + 2 as i2", "id + 3 as i3", "id + 4 as i4", "id + 5 as i5")
      .write.saveAsTable("tab")
  }

  override def afterAll(): Unit = {
    try {
      spark.sql("DROP TABLE IF EXISTS tab")
    } finally {
      super.afterAll()
    }
  }

  def checkNamedStruct(plan: LogicalPlan, expectedCount: Int): Unit = {
    var count = 0
    plan.foreach { operator =>
      operator.transformExpressions {
        case c: CreateNamedStruct =>
          count += 1
          c
      }
    }

    if (expectedCount != count) {
      fail(s"expect $expectedCount CreateNamedStruct but got $count.")
    }
  }

  test("simple case") {
    val df = spark.table("tab").selectExpr(
      "i5", "named_struct('a', i1, 'b', i2) as col1", "named_struct('a', i3, 'c', i4) as col2")
      .filter("col2.c > 11").selectExpr("col1.a")
    checkAnswer(df, Row(9) :: Row(10) :: Nil)
    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 0)
  }

  test("named_struct is used in the top Project") {
    val df = spark.table("tab").selectExpr(
      "i5", "named_struct('a', i1, 'b', i2) as col1", "named_struct('a', i3, 'c', i4)")
      .selectExpr("col1.a", "col1")
      .filter("col1.a > 8")
    checkAnswer(df, Row(9, Row(9, 10)) :: Row(10, Row(10, 11)) :: Nil)
    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 1)

    val df1 = spark.table("tab").selectExpr(
      "i5", "named_struct('a', i1, 'b', i2) as col1", "named_struct('a', i3, 'c', i4)")
      .sort("col1")
      .selectExpr("col1.a")
      .filter("col1.a > 8")
    checkAnswer(df1, Row(9) :: Row(10) :: Nil)
    checkNamedStruct(df1.queryExecution.optimizedPlan, expectedCount = 1)
  }

  test("expression in named_struct") {
    val df = spark.table("tab")
      .selectExpr("i5", "struct(i1 as exp, i2, i3) as cola")
      .selectExpr("cola.exp", "cola.i3").filter("cola.i3 > 10")
    checkAnswer(df, Row(9, 11) :: Row(10, 12) :: Nil)
    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 0)

    val df1 = spark.table("tab")
      .selectExpr("i5", "struct(i1 + 1 as exp, i2, i3) as cola")
      .selectExpr("cola.i3").filter("cola.exp > 10")
    checkAnswer(df1, Row(12) :: Nil)
    checkNamedStruct(df1.queryExecution.optimizedPlan, expectedCount = 0)
  }

  test("nested case") {
    val df = spark.table("tab")
      .selectExpr("struct(struct(i2, i3) as exp, i4) as cola")
      .selectExpr("cola.exp.i2", "cola.i4").filter("cola.exp.i2 > 10")
    checkAnswer(df, Row(11, 13) :: Nil)
    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 0)

    val df1 = spark.table("tab")
      .selectExpr("struct(i2, i3) as exp", "i4")
      .selectExpr("struct(exp, i4) as cola")
      .selectExpr("cola.exp.i2", "cola.i4").filter("cola.i4 > 11")
    checkAnswer(df1, Row(10, 12) :: Row(11, 13) :: Nil)
    checkNamedStruct(df.queryExecution.optimizedPlan, expectedCount = 0)
  }

  test("SPARK-32167: get field from an array of struct") {
    val innerStruct = new StructType().add("i", "int", nullable = true)
    val schema = new StructType().add("arr", ArrayType(innerStruct, containsNull = false))
    val df = spark.createDataFrame(List(Row(Seq(Row(1), Row(null)))).asJava, schema)
    checkAnswer(df.select($"arr".getField("i")), Row(Seq(1, null)))
  }

  test("SPARK-32002: Support ExtractValue from nested ArrayStruct") {
    val jsonStr1 = """{"a": [{"b": [{"c": [1,2]}]}]}"""
    val jsonStr2 = """{"a": [{"b": [{"c": [1]}, {"c": [2]}]}]}"""
    val df = spark.read.json(Seq(jsonStr1, jsonStr2).toDS())
    checkAnswer(df.select($"a.b.c"), Row(Seq(Seq(Seq(1, 2))))
      :: Row(Seq(Seq(Seq(1), Seq(2)))) :: Nil)

    def genJson(start: Char, end: Char, vStr: String): String = {
      (start to end).map(c => s"""{"$c": [""").mkString +
        vStr + (start to end).map(_ => "]}").mkString
    }

    def genResult(start: Char, end: Char, r: Seq[Int]): Any = {
      (start until end).fold(r) { (z, _) => Seq(z)}
    }

    val start: Char = 'a'
    for (i <- 2 to 10) {
      val end: Char = (start + i).toChar
      val jsonAToZ = genJson(start, end, "1,2,3")
      val dfAToZ = spark.read.json(Seq(jsonAToZ).toDS())
      checkAnswer(dfAToZ.select((start to end).mkString(".")),
        Row(genResult(start, end, Seq(1, 2, 3))))
    }

  }

}
