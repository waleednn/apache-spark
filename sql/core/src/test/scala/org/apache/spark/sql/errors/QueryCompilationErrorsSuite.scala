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

package org.apache.spark.sql.errors

import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest}
import org.apache.spark.sql.api.java.{UDF1, UDF2, UDF23Test}
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions.{grouping, grouping_id, sum, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType}

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class QueryCompilationErrorsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("CANNOT_UP_CAST_DATATYPE: invalid upcast data type") {
    val msg1 = intercept[AnalysisException] {
      sql("select 'value1' as a, 1L as b").as[StringIntClass]
    }.message
    assert(msg1 ===
      s"""
         |Cannot up cast b from bigint to int.
         |The type path of the target object is:
         |- field (class: "scala.Int", name: "b")
         |- root class: "org.apache.spark.sql.errors.StringIntClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")

    val msg2 = intercept[AnalysisException] {
      sql("select 1L as a," +
        " named_struct('a', 'value1', 'b', cast(1.0 as decimal(38,18))) as b")
        .as[ComplexClass]
    }.message
    assert(msg2 ===
      s"""
         |Cannot up cast b.`b` from decimal(38,18) to bigint.
         |The type path of the target object is:
         |- field (class: "scala.Long", name: "b")
         |- field (class: "org.apache.spark.sql.errors.StringLongClass", name: "b")
         |- root class: "org.apache.spark.sql.errors.ComplexClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: filter with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq("grouping", "grouping_id").foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max"))
          .filter(s"$grouping(CustomerId)=17850")
      }
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
    }
  }

  test("UNSUPPORTED_GROUPING_EXPRESSION: Sort with grouping/grouping_Id expression") {
    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    Seq(grouping("CustomerId"), grouping_id("CustomerId")).foreach { grouping =>
      val errMsg = intercept[AnalysisException] {
        df.groupBy("CustomerId").agg(Map("Quantity" -> "max")).
          sort(grouping)
      }
      assert(errMsg.errorClass === Some("UNSUPPORTED_GROUPING_EXPRESSION"))
      assert(errMsg.message ===
        "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    }
  }

  test("ILLEGAL_SUBSTRING: the argument_index of string format is invalid") {
    val e = intercept[AnalysisException] {
      sql("select format_string('%0$s', 'Hello')")
    }
    assert(e.errorClass === Some("ILLEGAL_SUBSTRING"))
    assert(e.message ===
      "The argument_index of string format cannot contain position 0$.")
  }

  test("CANNOT_USE_MIXTURE: Using aggregate function with grouped aggregate pandas UDF") {
    import IntegratedUDFTestUtils._

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy("CustomerId")
        .agg(pandasTestUDF(df("Quantity")), sum(df("Quantity"))).collect()
    }

    assert(e.errorClass === Some("CANNOT_USE_MIXTURE"))
    assert(e.message ===
      "Cannot use a mixture of aggregate function and group aggregate pandas UDF")
  }

  test("UNSUPPORTED_FEATURE: Using Python UDF with unsupported join condition") {
    import IntegratedUDFTestUtils._

    val df1 = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")
    val df2 = Seq(
      ("Bob", 17850),
      ("Alice", 17850),
      ("Tom", 17851)
    ).toDF("CustomerName", "CustomerID")

    val e = intercept[AnalysisException] {
      val pythonTestUDF = TestPythonUDF(name = "python_udf")
      df1.join(
        df2, pythonTestUDF(df1("CustomerID") === df2("CustomerID")), "leftouter").collect()
    }

    assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
    assert(e.getSqlState === "0A000")
    assert(e.message ===
      "The feature is not supported: " +
      "Using PythonUDF in join condition of join type LeftOuter is not supported")
  }

  test("UNSUPPORTED_FEATURE: Using pandas UDF aggregate expression with pivot") {
    import IntegratedUDFTestUtils._

    val df = Seq(
      (536361, "85123A", 2, 17850),
      (536362, "85123B", 4, 17850),
      (536363, "86123A", 6, 17851)
    ).toDF("InvoiceNo", "StockCode", "Quantity", "CustomerID")

    val e = intercept[AnalysisException] {
      val pandasTestUDF = TestGroupedAggPandasUDF(name = "pandas_udf")
      df.groupBy(df("CustomerID")).pivot(df("CustomerID")).agg(pandasTestUDF(df("Quantity")))
    }

    assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
    assert(e.getSqlState === "0A000")
    assert(e.message ===
      "The feature is not supported: " +
      "Pandas UDF aggregate expressions don't support pivot.")
  }

  test("NO_HANDLER_FOR_UDAF: No handler for UDAF error") {
    val functionName = "myCast"
    withUserDefinedFunction(functionName -> true) {
      sql(
        s"""
          |CREATE TEMPORARY FUNCTION $functionName
          |AS 'org.apache.spark.sql.errors.MyCastToString'
          |""".stripMargin)

      val e = intercept[AnalysisException] (
        sql("SELECT myCast(123) as value")
      )

      assert(e.errorClass === Some("NO_HANDLER_FOR_UDAF"))
      assert(e.message ===
        s"No handler for UDAF 'org.apache.spark.sql.errors.MyCastToString'. " +
          s"Use sparkSession.udf.register(...) instead.")
    }
  }

  test("UNTYPED_SCALA_UDF: use untyped Scala UDF should fail by default") {
    val e = intercept[AnalysisException](udf((x: Int) => x, IntegerType))

    assert(e.errorClass === Some("UNTYPED_SCALA_UDF"))
    assert(e.message ===
      "You're using untyped Scala UDF, which does not have the input type " +
      "information. Spark may blindly pass null to the Scala closure with primitive-type " +
      "argument, and the closure will see the default value of the Java type for the null " +
      "argument, e.g. `udf((x: Int) => x, IntegerType)`, the result is 0 for null input. " +
      "To get rid of this error, you could:\n" +
      "1. use typed Scala UDF APIs(without return type parameter), e.g. `udf((x: Int) => x)`\n" +
      "2. use Java UDF APIs, e.g. `udf(new UDF1[String, Integer] { " +
      "override def call(s: String): Integer = s.length() }, IntegerType)`, " +
      "if input types are all non primitive\n" +
      s"3. set ${SQLConf.LEGACY_ALLOW_UNTYPED_SCALA_UDF.key} to true and " +
      s"use this API with caution")
  }

  test("UDF_CLASS_NOT_IMPLEMENT_ANY_UDF_INTERFACE: " +
    "java udf class does not implement any udf interface") {
    val className = "org.apache.spark.sql.errors.MyCastToString"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "myCast",
        className,
        StringType)
    )

    assert(e.errorClass === Some("UDF_CLASS_NOT_IMPLEMENT_ANY_UDF_INTERFACE"))
    assert(e.message ===
      s"UDF class $className doesn't implement any UDF interface")
  }

  test("UDF_CLASS_IMPLEMENT_MULTI_UDF_INTERFACE: java udf implement multi UDF interface") {
    val className = "org.apache.spark.sql.errors.MySum"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "mySum",
        className,
        StringType)
    )

    assert(e.errorClass === Some("UDF_CLASS_IMPLEMENT_MULTI_UDF_INTERFACE"))
    assert(e.message ===
      s"It is invalid to implement multiple UDF interfaces, UDF class $className")
  }

  test("UNSUPPORTED_FEATURE: java udf with too many type arguments") {
    val className = "org.apache.spark.sql.errors.MultiIntSum"
    val e = intercept[AnalysisException](
      spark.udf.registerJava(
        "mySum",
        className,
        StringType)
    )

    assert(e.errorClass === Some("UNSUPPORTED_FEATURE"))
    assert(e.getSqlState === "0A000")
    assert(e.message ===
      s"The feature is not supported: UDF class with 24 type arguments is not supported.")
  }
}

class MyCastToString extends SparkUserDefinedFunction(
  (input: Any) => if (input == null) {
    null
  } else {
    input.toString
  },
  StringType,
  inputEncoders = Seq.fill(1)(None))

class MySum extends UDF1[Int, Int] with UDF2[Int, Int, Int] {
  override def call(t1: Int): Int = t1

  override def call(t1: Int, t2: Int): Int = t1 + t2
}

class MultiIntSum extends
  UDF23Test[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int,
    Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] {
  // scalastyle:off argcount
  override def call(
      t1: Int, t2: Int, t3: Int, t4: Int, t5: Int, t6: Int, t7: Int, t8: Int,
      t9: Int, t10: Int, t11: Int, t12: Int, t13: Int, t14: Int, t15: Int, t16: Int,
      t17: Int, t18: Int, t19: Int, t20: Int, t21: Int, t22: Int, t23: Int): Int = {
    t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 +
      t11 + t12 + t13 + t14 + t15 + t16 + t17 + t18 + t19 + t20 + t21 + t22 + t23
  }
  // scalastyle:on argcount
}
