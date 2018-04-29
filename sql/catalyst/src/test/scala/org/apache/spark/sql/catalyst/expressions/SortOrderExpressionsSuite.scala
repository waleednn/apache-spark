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

import java.sql.{Date, Timestamp}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.unsafe.sort.PrefixComparators.{BinaryPrefixComparator, DoublePrefixComparator, StringPrefixComparator}

class SortOrderExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SortPrefix") {
    val i1 = Literal.create(20132983, IntegerType)
    val i2 = Literal.create(-20132983, IntegerType)
    val l1 = Literal.create(20132983, LongType)
    val l2 = Literal.create(-20132983, LongType)
    val millis = 1524954911000L;
    val d1 = Literal.create(new java.sql.Date(millis), DateType)
    val t1 = Literal.create(new Timestamp(millis), TimestampType)
    val f1 = Literal.create(0.7788229f, FloatType)
    val f2 = Literal.create(-0.7788229f, FloatType)
    val db1 = Literal.create(0.7788229d, DoubleType)
    val db2 = Literal.create(-0.7788229d, DoubleType)
    val s1 = Literal.create("T", StringType)
    val s2 = Literal.create("This is longer than 8 characters", StringType)
    val b1 = Literal.create(Array[Byte](12), BinaryType)
    val b2 = Literal.create(Array[Byte](12, 17, 99, 0, 0, 0, 2, 3, 0xf4.asInstanceOf[Byte]),
      BinaryType)

    checkEvaluation(SortPrefix(SortOrder(i1, Ascending)), 20132983L)
    checkEvaluation(SortPrefix(SortOrder(i2, Ascending)), -20132983L)
    checkEvaluation(SortPrefix(SortOrder(l1, Ascending)), 20132983L)
    checkEvaluation(SortPrefix(SortOrder(l2, Ascending)), -20132983L)
    // TODO: Find out why 17649L is the correct number for both eval and codegen paths
    checkEvaluation(SortPrefix(SortOrder(d1, Ascending)), 17649L)
    checkEvaluation(SortPrefix(SortOrder(t1, Ascending)), millis*1000)
    checkEvaluation(SortPrefix(SortOrder(f1, Ascending)),
      DoublePrefixComparator.computePrefix(f1.value.asInstanceOf[Float].toDouble))
    checkEvaluation(SortPrefix(SortOrder(f2, Ascending)),
      DoublePrefixComparator.computePrefix(f2.value.asInstanceOf[Float].toDouble))
    checkEvaluation(SortPrefix(SortOrder(db1, Ascending)),
      DoublePrefixComparator.computePrefix(db1.value.asInstanceOf[Double]))
    checkEvaluation(SortPrefix(SortOrder(db2, Ascending)),
      DoublePrefixComparator.computePrefix(db2.value.asInstanceOf[Double]))
    checkEvaluation(SortPrefix(SortOrder(s1, Ascending)),
      StringPrefixComparator.computePrefix(s1.value.asInstanceOf[UTF8String]))
    checkEvaluation(SortPrefix(SortOrder(s2, Ascending)),
      StringPrefixComparator.computePrefix(s2.value.asInstanceOf[UTF8String]))
    checkEvaluation(SortPrefix(SortOrder(b1, Ascending)),
      BinaryPrefixComparator.computePrefix(b1.value.asInstanceOf[Array[Byte]]))
    checkEvaluation(SortPrefix(SortOrder(b2, Ascending)),
      BinaryPrefixComparator.computePrefix(b2.value.asInstanceOf[Array[Byte]]))
  }
}
