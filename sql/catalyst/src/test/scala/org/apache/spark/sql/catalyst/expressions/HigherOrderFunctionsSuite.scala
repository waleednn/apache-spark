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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class HigherOrderFunctionsSuite extends SparkFunSuite with ExpressionEvalHelper {
  import org.apache.spark.sql.catalyst.dsl.expressions._

  private def createLambda(
      dt: DataType,
      nullable: Boolean,
      f: Expression => Expression): Expression = {
    val lv = NamedLambdaVariable("arg", dt, nullable)
    val function = f(lv)
    LambdaFunction(function, Seq(lv))
  }

  private def createLambda(
      dt1: DataType,
      nullable1: Boolean,
      dt2: DataType,
      nullable2: Boolean,
      f: (Expression, Expression) => Expression): Expression = {
    val lv1 = NamedLambdaVariable("arg1", dt1, nullable1)
    val lv2 = NamedLambdaVariable("arg2", dt2, nullable2)
    val function = f(lv1, lv2)
    LambdaFunction(function, Seq(lv1, lv2))
  }

  def transform(expr: Expression, f: Expression => Expression): Expression = {
    val at = expr.dataType.asInstanceOf[ArrayType]
    ArrayTransform(expr, createLambda(at.elementType, at.containsNull, f))
  }

  def transform(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val at = expr.dataType.asInstanceOf[ArrayType]
    ArrayTransform(expr, createLambda(at.elementType, at.containsNull, IntegerType, false, f))
  }

  def filter(expr: Expression, f: Expression => Expression): Expression = {
    val at = expr.dataType.asInstanceOf[ArrayType]
    ArrayFilter(expr, createLambda(at.elementType, at.containsNull, f))
  }

  def aggregate(
      expr: Expression,
      zero: Expression,
      merge: (Expression, Expression) => Expression,
      finish: Expression => Expression): Expression = {
    val at = expr.dataType.asInstanceOf[ArrayType]
    val zeroType = zero.dataType
    ArrayAggregate(
      expr,
      zero,
      createLambda(zeroType, true, at.elementType, at.containsNull, merge),
      createLambda(zeroType, true, finish))
  }

  def aggregate(
      expr: Expression,
      zero: Expression,
      merge: (Expression, Expression) => Expression): Expression = {
    aggregate(expr, zero, merge, identity)
  }

  def transformValues(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
    val valueType = expr.dataType.asInstanceOf[MapType].valueType
    val keyType = expr.dataType.asInstanceOf[MapType].keyType
    TransformValues(expr, createLambda(keyType, false, valueType, true, f))
  }

  test("ArrayTransform") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val plusOne: Expression => Expression = x => x + 1
    val plusIndex: (Expression, Expression) => Expression = (x, i) => x + i

    checkEvaluation(transform(ai0, plusOne), Seq(2, 3, 4))
    checkEvaluation(transform(ai0, plusIndex), Seq(1, 3, 5))
    checkEvaluation(transform(transform(ai0, plusIndex), plusOne), Seq(2, 4, 6))
    checkEvaluation(transform(ai1, plusOne), Seq(2, null, 4))
    checkEvaluation(transform(ai1, plusIndex), Seq(1, null, 5))
    checkEvaluation(transform(transform(ai1, plusIndex), plusOne), Seq(2, null, 6))
    checkEvaluation(transform(ain, plusOne), null)

    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val repeatTwice: Expression => Expression = x => Concat(Seq(x, x))
    val repeatIndexTimes: (Expression, Expression) => Expression = (x, i) => StringRepeat(x, i)

    checkEvaluation(transform(as0, repeatTwice), Seq("aa", "bb", "cc"))
    checkEvaluation(transform(as0, repeatIndexTimes), Seq("", "b", "cc"))
    checkEvaluation(transform(transform(as0, repeatIndexTimes), repeatTwice),
      Seq("", "bb", "cccc"))
    checkEvaluation(transform(as1, repeatTwice), Seq("aa", null, "cc"))
    checkEvaluation(transform(as1, repeatIndexTimes), Seq("", null, "cc"))
    checkEvaluation(transform(transform(as1, repeatIndexTimes), repeatTwice),
      Seq("", null, "cccc"))
    checkEvaluation(transform(asn, repeatTwice), null)

    val aai = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(transform(aai, array => Cast(transform(array, plusOne), StringType)),
      Seq("[2, 3, 4]", null, "[5, 6]"))
    checkEvaluation(transform(aai, array => Cast(transform(array, plusIndex), StringType)),
      Seq("[1, 3, 5]", null, "[4, 6]"))
  }

  test("MapFilter") {
    def mapFilter(expr: Expression, f: (Expression, Expression) => Expression): Expression = {
      val mt = expr.dataType.asInstanceOf[MapType]
      MapFilter(expr, createLambda(mt.keyType, false, mt.valueType, mt.valueContainsNull, f))
    }
    val mii0 = Literal.create(Map(1 -> 0, 2 -> 10, 3 -> -1),
      MapType(IntegerType, IntegerType, valueContainsNull = false))
    val mii1 = Literal.create(Map(1 -> null, 2 -> 10, 3 -> null),
      MapType(IntegerType, IntegerType, valueContainsNull = true))
    val miin = Literal.create(null, MapType(IntegerType, IntegerType, valueContainsNull = false))

    val kGreaterThanV: (Expression, Expression) => Expression = (k, v) => k > v

    checkEvaluation(mapFilter(mii0, kGreaterThanV), Map(1 -> 0, 3 -> -1))
    checkEvaluation(mapFilter(mii1, kGreaterThanV), Map())
    checkEvaluation(mapFilter(miin, kGreaterThanV), null)

    val valueIsNull: (Expression, Expression) => Expression = (_, v) => v.isNull

    checkEvaluation(mapFilter(mii0, valueIsNull), Map())
    checkEvaluation(mapFilter(mii1, valueIsNull), Map(1 -> null, 3 -> null))
    checkEvaluation(mapFilter(miin, valueIsNull), null)

    val msi0 = Literal.create(Map("abcdf" -> 5, "abc" -> 10, "" -> 0),
      MapType(StringType, IntegerType, valueContainsNull = false))
    val msi1 = Literal.create(Map("abcdf" -> 5, "abc" -> 10, "" -> null),
      MapType(StringType, IntegerType, valueContainsNull = true))
    val msin = Literal.create(null, MapType(StringType, IntegerType, valueContainsNull = false))

    val isLengthOfKey: (Expression, Expression) => Expression = (k, v) => Length(k) === v

    checkEvaluation(mapFilter(msi0, isLengthOfKey), Map("abcdf" -> 5, "" -> 0))
    checkEvaluation(mapFilter(msi1, isLengthOfKey), Map("abcdf" -> 5))
    checkEvaluation(mapFilter(msin, isLengthOfKey), null)

    val mia0 = Literal.create(Map(1 -> Seq(0, 1, 2), 2 -> Seq(10), -3 -> Seq(-1, 0, -2, 3)),
      MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = false))
    val mia1 = Literal.create(Map(1 -> Seq(0, 1, 2), 2 -> null, -3 -> Seq(-1, 0, -2, 3)),
      MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = true))
    val mian = Literal.create(
      null, MapType(IntegerType, ArrayType(IntegerType), valueContainsNull = false))

    val customFunc: (Expression, Expression) => Expression = (k, v) => Size(v) + k > 3

    checkEvaluation(mapFilter(mia0, customFunc), Map(1 -> Seq(0, 1, 2)))
    checkEvaluation(mapFilter(mia1, customFunc), Map(1 -> Seq(0, 1, 2)))
    checkEvaluation(mapFilter(mian, customFunc), null)
  }

  test("ArrayFilter") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    val isEven: Expression => Expression = x => x % 2 === 0
    val isNullOrOdd: Expression => Expression = x => x.isNull || x % 2 === 1

    checkEvaluation(filter(ai0, isEven), Seq(2))
    checkEvaluation(filter(ai0, isNullOrOdd), Seq(1, 3))
    checkEvaluation(filter(ai1, isEven), Seq.empty)
    checkEvaluation(filter(ai1, isNullOrOdd), Seq(1, null, 3))
    checkEvaluation(filter(ain, isEven), null)
    checkEvaluation(filter(ain, isNullOrOdd), null)

    val as0 =
      Literal.create(Seq("a0", "b1", "a2", "c3"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    val startsWithA: Expression => Expression = x => x.startsWith("a")

    checkEvaluation(filter(as0, startsWithA), Seq("a0", "a2"))
    checkEvaluation(filter(as1, startsWithA), Seq("a"))
    checkEvaluation(filter(asn, startsWithA), null)

    val aai = Literal.create(Seq(Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(transform(aai, ix => filter(ix, isNullOrOdd)),
      Seq(Seq(1, 3), null, Seq(5)))
  }

  test("ArrayAggregate") {
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val ai1 = Literal.create(Seq[Integer](1, null, 3), ArrayType(IntegerType, containsNull = true))
    val ai2 = Literal.create(Seq.empty[Int], ArrayType(IntegerType, containsNull = false))
    val ain = Literal.create(null, ArrayType(IntegerType, containsNull = false))

    checkEvaluation(aggregate(ai0, 0, (acc, elem) => acc + elem, acc => acc * 10), 60)
    checkEvaluation(aggregate(ai1, 0, (acc, elem) => acc + coalesce(elem, 0), acc => acc * 10), 40)
    checkEvaluation(aggregate(ai2, 0, (acc, elem) => acc + elem, acc => acc * 10), 0)
    checkEvaluation(aggregate(ain, 0, (acc, elem) => acc + elem, acc => acc * 10), null)

    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType, containsNull = false))
    val as1 = Literal.create(Seq("a", null, "c"), ArrayType(StringType, containsNull = true))
    val as2 = Literal.create(Seq.empty[String], ArrayType(StringType, containsNull = false))
    val asn = Literal.create(null, ArrayType(StringType, containsNull = false))

    checkEvaluation(aggregate(as0, "", (acc, elem) => Concat(Seq(acc, elem))), "abc")
    checkEvaluation(aggregate(as1, "", (acc, elem) => Concat(Seq(acc, coalesce(elem, "x")))), "axc")
    checkEvaluation(aggregate(as2, "", (acc, elem) => Concat(Seq(acc, elem))), "")
    checkEvaluation(aggregate(asn, "", (acc, elem) => Concat(Seq(acc, elem))), null)

    val aai = Literal.create(Seq[Seq[Integer]](Seq(1, 2, 3), null, Seq(4, 5)),
      ArrayType(ArrayType(IntegerType, containsNull = false), containsNull = true))
    checkEvaluation(
      aggregate(aai, 0,
        (acc, array) => coalesce(aggregate(array, acc, (acc, elem) => acc + elem), acc)),
      15)
  }

  test("TransformValues") {
    val ai0 = Literal.create(
      Map(1 -> 1, 2 -> 2, 3 -> 3),
      MapType(IntegerType, IntegerType))
    val ai1 = Literal.create(
      Map(1 -> 1, 2 -> null, 3 -> 3),
      MapType(IntegerType, IntegerType))
    val ain = Literal.create(
      Map.empty[Int, Int],
      MapType(IntegerType, IntegerType))

    val plusOne: (Expression, Expression) => Expression = (k, v) => v + 1
    val valueUpdate: (Expression, Expression) => Expression = (k, v) => k * k

    checkEvaluation(transformValues(ai0, plusOne), Map(1 -> 2, 2 -> 3, 3 -> 4))
    checkEvaluation(transformValues(ai0, valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(
      transformValues(transformValues(ai0, plusOne), valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(transformValues(ai1, plusOne), Map(1 -> 2, 2 -> null, 3 -> 4))
    checkEvaluation(transformValues(ai1, valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(
      transformValues(transformValues(ai1, plusOne), valueUpdate), Map(1 -> 1, 2 -> 4, 3 -> 9))
    checkEvaluation(transformValues(ain, plusOne), Map.empty[Int, Int])

    val as0 = Literal.create(
      Map("a" -> "xy", "bb" -> "yz", "ccc" -> "zx"), MapType(StringType, StringType))
    val as1 = Literal.create(
      Map("a" -> "xy", "bb" -> null, "ccc" -> "zx"), MapType(StringType, StringType))
    val asn = Literal.create(Map.empty[StringType, StringType], MapType(StringType, StringType))

    val concatValue: (Expression, Expression) => Expression = (k, v) => Concat(Seq(k, v))
    val valueTypeUpdate: (Expression, Expression) => Expression =
      (k, v) => Length(v) + 1

    checkEvaluation(
      transformValues(as0, concatValue), Map("a" -> "axy", "bb" -> "bbyz", "ccc" -> "ccczx"))
    checkEvaluation(transformValues(as0, valueTypeUpdate),
      Map("a" -> 3, "bb" -> 3, "ccc" -> 3))
    checkEvaluation(
      transformValues(transformValues(as0, concatValue), concatValue),
      Map("a" -> "aaxy", "bb" -> "bbbbyz", "ccc" -> "cccccczx"))
    checkEvaluation(transformValues(as1, concatValue),
      Map("a" -> "axy", "bb" -> null, "ccc" -> "ccczx"))
    checkEvaluation(transformValues(as1, valueTypeUpdate),
      Map("a" -> 3, "bb" -> null, "ccc" -> 3))
    checkEvaluation(
      transformValues(transformValues(as1, concatValue), concatValue),
      Map("a" -> "aaxy", "bb" -> null, "ccc" -> "cccccczx"))
    checkEvaluation(transformValues(asn, concatValue), Map.empty[String, String])
    checkEvaluation(transformValues(asn, valueTypeUpdate), Map.empty[String, Int])
    checkEvaluation(
      transformValues(transformValues(asn, concatValue), valueTypeUpdate),
      Map.empty[String, Int])
  }
}
