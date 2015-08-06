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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class RFormulaParserSuite extends SparkFunSuite {
  private def checkParse(
      formula: String,
      label: String,
      terms: Seq[String],
      schema: StructType = null) {
    val resolved = RFormulaParser.parse(formula).resolve(schema)
    assert(resolved.label == label)
    val simpleTerms = terms.map { t =>
      if (t.contains(":")) {
        t.split(":").toSeq
      } else {
        Seq(t)
      }
    }
    assert(resolved.terms == simpleTerms)
  }

  test("parse simple formulas") {
    checkParse("y ~ x", "y", Seq("x"))
    checkParse("y ~ x + x", "y", Seq("x"))
    checkParse("y~x+z", "y", Seq("x", "z"))
    checkParse("y ~   ._fo..o  ", "y", Seq("._fo..o"))
    checkParse("resp ~ A_VAR + B + c123", "resp", Seq("A_VAR", "B", "c123"))
  }

  test("parse dot") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ .", "a", Seq("b", "c"), schema)
  }

  test("parse deletion") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ c - b", "a", Seq("c"), schema)
  }

  test("parse additions and deletions in order") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ . - b + . - c", "a", Seq("b"), schema)
  }

  test("dot ignores complex column types") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "tinyint", false)
      .add("c", "map<string, string>", true)
    checkParse("a ~ .", "a", Seq("b"), schema)
  }

  test("parse intercept") {
    assert(RFormulaParser.parse("a ~ b").hasIntercept)
    assert(RFormulaParser.parse("a ~ b + 1").hasIntercept)
    assert(RFormulaParser.parse("a ~ b - 0").hasIntercept)
    assert(RFormulaParser.parse("a ~ b - 1 + 1").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b + 0").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b - 1").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b + 1 - 1").hasIntercept)
  }

  test("parse interactions") {
    checkParse("y ~ a:b", "y", Seq("a:b"))
    checkParse("y ~ ._a:._x", "y", Seq("._a:._x"))
    checkParse("y ~ foo:bar", "y", Seq("foo:bar"))
    checkParse("y ~ a : b : c", "y", Seq("a:b:c"))
    checkParse("y ~ q + a:b:c + b:c + c:d + z", "y", Seq("q", "a:b:c", "b:c", "c:d", "z"))
  }
}
