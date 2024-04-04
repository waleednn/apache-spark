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

import scala.collection.immutable.Seq

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Collation, ConcatWs, ExpressionEvalHelper, Literal, StringRepeat}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.{Collation, ExpressionEvalHelper, FindInSet, Literal, StringInstr, StringRepeat}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

class CollationStringExpressionsSuite
  extends QueryTest
  with SharedSparkSession
  with ExpressionEvalHelper {

  case class CollationTestCase[R](s1: String, s2: String, collation: String, expectedResult: R)
  case class CollationTestFail[R](s1: String, s2: String, collation: String)


  test("Support ConcatWs string expression with Collation") {
    def prepareConcatWs(
        sep: String,
        collation: String,
        inputs: Any*): ConcatWs = {
      val collationId = CollationFactory.collationNameToId(collation)
      val inputExprs = inputs.map(s => Literal.create(s, StringType(collationId)))
      val sepExpr = Literal.create(sep, StringType(collationId))
      ConcatWs(sepExpr +: inputExprs)
    }
    // Supported Collations
    val checks = Seq(
      CollationTestCase("Spark", "SQL", "UTF8_BINARY", "Spark SQL")
    )
    checks.foreach(ct =>
      checkEvaluation(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2), ct.expectedResult)
    )

    // Unsupported Collations
    val fails = Seq(
      CollationTestFail("ABC", "%b%", "UTF8_BINARY_LCASE"),
      CollationTestFail("ABC", "%B%", "UNICODE"),
      CollationTestFail("ABC", "%b%", "UNICODE_CI")
    )
    fails.foreach(ct =>
      assert(prepareConcatWs(" ", ct.collation, ct.s1, ct.s2)
        .checkInputDataTypes() ==
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> "first",
            "requiredType" -> """"STRING"""",
            "inputSql" -> s""""' ' collate ${ct.collation}"""",
            "inputType" -> s""""STRING COLLATE ${ct.collation}""""
          )
        )
      )
    )
  }

  test("INSTR check result on explicitly collated strings") {
    def testInStr(str: String, substr: String, collationId: Integer, expected: Integer): Unit = {
      val string = Literal.create(str, StringType(collationId))
      val substring = Literal.create(substr, StringType(collationId))

      checkEvaluation(StringInstr(string, substring), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testInStr("aaads", "Aa", collationId, 0)
    testInStr("aaaDs", "de", collationId, 0)
    testInStr("aaads", "ds", collationId, 4)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "大千", collationId, 5)
    testInStr("test大千世界X大千世界", "界X", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testInStr("aaads", "Aa", collationId, 1)
    testInStr("aaaDs", "de", collationId, 0)
    testInStr("aaaDs", "ds", collationId, 4)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "大千", collationId, 5)
    testInStr("test大千世界X大千世界", "界x", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE")
    testInStr("aaads", "Aa", collationId, 0)
    testInStr("aaads", "aa", collationId, 1)
    testInStr("aaads", "de", collationId, 0)
    testInStr("xxxx", "", collationId, 1)
    testInStr("", "xxxx", collationId, 0)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "界x", collationId, 0)
    testInStr("test大千世界X大千世界", "界X", collationId, 8)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testInStr("aaads", "AD", collationId, 3)
    testInStr("aaads", "dS", collationId, 4)
    // scalastyle:off
    testInStr("test大千世界X大千世界", "界x", collationId, 8)
    // scalastyle:on
  }

  test("FIND_IN_SET check result on explicitly collated strings") {
    def testFindInSet(word: String, set: String, collationId: Integer, expected: Integer): Unit = {
      val w = Literal.create(word, StringType(collationId))
      val s = Literal.create(set, StringType(collationId))

      checkEvaluation(FindInSet(w, s), expected)
    }

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testFindInSet("AB", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("abc", "abc,b,ab,c,def", collationId, 1)
    testFindInSet("def", "abc,b,ab,c,def", collationId, 5)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("", "abc,b,ab,c,def", collationId, 0)

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("c", "abc,b,ab,c,def", collationId, 4)
    testFindInSet("AB", "abc,b,ab,c,def", collationId, 3)
    testFindInSet("AbC", "abc,b,ab,c,def", collationId, 1)
    testFindInSet("abcd", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("XX", "xx", collationId, 1)
    testFindInSet("", "abc,b,ab,c,def", collationId, 0)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 4)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("ab", "abc,b,ab,c,def", collationId, 3)
    testFindInSet("Ab", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("d,ef", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("xx", "xx", collationId, 1)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 0)
    testFindInSet("大", "test,大千,世,界X,大,千,世界", collationId, 5)
    // scalastyle:on

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testFindInSet("a", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("C", "abc,b,ab,c,def", collationId, 4)
    testFindInSet("DeF", "abc,b,ab,c,dEf", collationId, 5)
    testFindInSet("DEFG", "abc,b,ab,c,def", collationId, 0)
    testFindInSet("XX", "xx", collationId, 1)
    // scalastyle:off
    testFindInSet("界x", "test,大千,世,界X,大,千,世界", collationId, 4)
    testFindInSet("界x", "test,大千,界Xx,世,界X,大,千,世界", collationId, 5)
    testFindInSet("大", "test,大千,世,界X,大,千,世界", collationId, 5)
    // scalastyle:on
  }

  test("REPEAT check output type on explicitly collated string") {
    def testRepeat(input: String, n: Int, collationId: Int, expected: String): Unit = {
      val s = Literal.create(input, StringType(collationId))

      checkEvaluation(Collation(StringRepeat(s, Literal.create(n))).replacement, expected)
    }

    // Not important for this test
    val repeatNum = 2;

    var collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    testRepeat("abc", repeatNum, collationId, "UTF8_BINARY")

    collationId = CollationFactory.collationNameToId("UTF8_BINARY_LCASE")
    testRepeat("abc", repeatNum, collationId, "UTF8_BINARY_LCASE")

    collationId = CollationFactory.collationNameToId("UNICODE")
    testRepeat("abc", repeatNum, collationId, "UNICODE")

    collationId = CollationFactory.collationNameToId("UNICODE_CI")
    testRepeat("abc", repeatNum, collationId, "UNICODE_CI")
  }

  // TODO: Add more tests for other string expressions

}

class CollationStringExpressionsANSISuite extends CollationRegexpExpressionsSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(SQLConf.ANSI_ENABLED, true)
}
