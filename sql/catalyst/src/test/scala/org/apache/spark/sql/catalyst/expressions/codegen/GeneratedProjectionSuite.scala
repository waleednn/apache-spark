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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test suite for generated projections
 */
class GeneratedProjectionSuite extends SparkFunSuite {

  test("generated projections on wider table") {
    val N = 10
    val wideRow1 = new GenericInternalRow((1 to N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      (1 to N).map(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    // test generated UnsafeProjection
    val unsafeProj = UnsafeProjection.create(nestedSchema)
    val unsafe: UnsafeRow = unsafeProj(nested)
    (0 until N).foreach { i =>
      val s = UTF8String.fromString((i + 1).toString)
      assert(i + 1 === unsafe.getInt(i + 2))
      assert(s === unsafe.getUTF8String(i + 2 + N))
      assert(i + 1 === unsafe.getStruct(0, N * 2).getInt(i))
      assert(s === unsafe.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i + 1 === unsafe.getStruct(1, N * 2).getInt(i))
      assert(s === unsafe.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated SafeProjection
    val safeProj = FromUnsafeProjection(nestedSchema)
    val result = safeProj(unsafe)
    // Can't compare GenericInternalRow with JoinedRow directly
    (0 until N).foreach { i =>
      val r = i + 1
      val s = UTF8String.fromString((i + 1).toString)
      assert(r === result.getInt(i + 2))
      assert(s === result.getUTF8String(i + 2 + N))
      assert(r === result.getStruct(0, N * 2).getInt(i))
      assert(s === result.getStruct(0, N * 2).getUTF8String(i + N))
      assert(r === result.getStruct(1, N * 2).getInt(i))
      assert(s === result.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)()
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }
}
