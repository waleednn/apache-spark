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

package org.apache.spark.sql.catalyst.analysis

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.types._

class HiveTypeCoercionSuite extends FunSuite {

  test("tightest common bound for types") {
    def widenTest(t1: DataType, t2: DataType, tightestCommon: Option[DataType]) {
      var found = HiveTypeCoercion.findTightestCommonType(t1, t2)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t1 and $t2, found $found")
      // Test both directions to make sure the widening is symmetric.
      found = HiveTypeCoercion.findTightestCommonType(t2, t1)
      assert(found == tightestCommon,
        s"Expected $tightestCommon as tightest common type for $t2 and $t1, found $found")
    }

    // Null
    widenTest(NullType, NullType, Some(NullType))

    // Boolean
    widenTest(NullType, BooleanType, Some(BooleanType))
    widenTest(BooleanType, BooleanType, Some(BooleanType))
    widenTest(IntegerType, BooleanType, None)
    widenTest(LongType, BooleanType, None)

    // Integral
    widenTest(NullType, ByteType, Some(ByteType))
    widenTest(NullType, IntegerType, Some(IntegerType))
    widenTest(NullType, LongType, Some(LongType))
    widenTest(ShortType, IntegerType, Some(IntegerType))
    widenTest(ShortType, LongType, Some(LongType))
    widenTest(IntegerType, LongType, Some(LongType))
    widenTest(LongType, LongType, Some(LongType))

    // Floating point
    widenTest(NullType, FloatType, Some(FloatType))
    widenTest(NullType, DoubleType, Some(DoubleType))
    widenTest(FloatType, DoubleType, Some(DoubleType))
    widenTest(FloatType, FloatType, Some(FloatType))
    widenTest(DoubleType, DoubleType, Some(DoubleType))

    // Integral mixed with floating point.
    widenTest(IntegerType, FloatType, Some(FloatType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(IntegerType, DoubleType, Some(DoubleType))
    widenTest(LongType, FloatType, Some(FloatType))
    widenTest(LongType, DoubleType, Some(DoubleType))

    // StringType
    widenTest(NullType, StringType, Some(StringType))
    widenTest(StringType, StringType, Some(StringType))
    widenTest(IntegerType, StringType, None)
    widenTest(LongType, StringType, None)

    // TimestampType
    widenTest(NullType, TimestampType, Some(TimestampType))
    widenTest(TimestampType, TimestampType, Some(TimestampType))
    widenTest(IntegerType, TimestampType, None)
    widenTest(StringType, TimestampType, None)

    // ComplexType
    widenTest(NullType, MapType(IntegerType, StringType, false), Some(MapType(IntegerType, StringType, false)))
    widenTest(NullType, StructType(Seq()), Some(StructType(Seq())))
    widenTest(StringType, MapType(IntegerType, StringType, true), None)
    widenTest(ArrayType(IntegerType), StructType(Seq()), None)
  }

  test("boolean casts") {
    def ruleTest(initial: Expression, transformed: Expression) {
      val testRelation = LocalRelation(AttributeReference("a", IntegerType)())
      assert(BooleanCasts(Project(Seq(Alias(initial, "a")()), testRelation)) ==
        Project(Seq(Alias(transformed, "a")()), testRelation))      
    }
    // Remove superflous boolean -> boolean casts.
    ruleTest(Cast(Literal(true), BooleanType), Literal(true))
    // Stringify boolean when casting to string.
    ruleTest(Cast(Literal(false), StringType), If(Literal(false), Literal("true"), Literal("false")))
  }
}
