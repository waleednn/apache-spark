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

import java.io.ByteArrayOutputStream

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeRow, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.unsafe.types.UTF8String

class UnsafeRowSuite extends SparkFunSuite {
  test("writeToStream") {
    val row = InternalRow.apply(UTF8String.fromString("hello"), UTF8String.fromString("world"), 123)
    val arrayBackedUnsafeRow: UnsafeRow =
      UnsafeProjection.create(Array[DataType](StringType, StringType, IntegerType)).apply(row)
    assert(arrayBackedUnsafeRow.getBaseObject.isInstanceOf[Array[Byte]])
    val bytesFromArrayBackedRow: Array[Byte] = {
      val baos = new ByteArrayOutputStream()
      arrayBackedUnsafeRow.writeToStream(baos, null)
      baos.toByteArray
    }
    val bytesFromOffheapRow: Array[Byte] = {
      val offheapRowPage = MemoryAllocator.UNSAFE.allocate(arrayBackedUnsafeRow.getSizeInBytes)
      try {
        PlatformDependent.copyMemory(
          arrayBackedUnsafeRow.getBaseObject,
          arrayBackedUnsafeRow.getBaseOffset,
          offheapRowPage.getBaseObject,
          offheapRowPage.getBaseOffset,
          arrayBackedUnsafeRow.getSizeInBytes
        )
        val offheapUnsafeRow: UnsafeRow = new UnsafeRow()
        offheapUnsafeRow.pointTo(
          offheapRowPage.getBaseObject,
          offheapRowPage.getBaseOffset,
          3, // num fields
          arrayBackedUnsafeRow.getSizeInBytes
        )
        assert(offheapUnsafeRow.getBaseObject === null)
        val baos = new ByteArrayOutputStream()
        val writeBuffer = new Array[Byte](1024)
        offheapUnsafeRow.writeToStream(baos, writeBuffer)
        baos.toByteArray
      } finally {
        MemoryAllocator.UNSAFE.free(offheapRowPage)
      }
    }

    assert(bytesFromArrayBackedRow === bytesFromOffheapRow)
  }

  test("calling getDouble() and getFloat() on null columns") {
    val row = InternalRow.apply(null, null)
    val unsafeRow = UnsafeProjection.create(Array[DataType](FloatType, DoubleType)).apply(row)
    assert(unsafeRow.getFloat(0) === row.getFloat(0))
    assert(unsafeRow.getDouble(1) === row.getDouble(1))
  }

  test("calling get(ordinal, datatype) on null columns") {
    val row = InternalRow.apply(null)
    val unsafeRow = UnsafeProjection.create(Array[DataType](NullType)).apply(row)
    for (dataType <- DataTypeTestUtils.atomicTypes) {
      assert(unsafeRow.get(0, dataType) === null)
    }
  }

  test("createFromByteArray and copyFrom") {
    val row = InternalRow(1, UTF8String.fromString("abc"))
    val converter = UnsafeProjection.create(Array[DataType](IntegerType, StringType))
    val unsafeRow = converter.apply(row)

    val emptyRow = UnsafeRow.createFromByteArray(64, 2)
    val buffer = emptyRow.getBaseObject

    emptyRow.copyFrom(unsafeRow)
    assert(emptyRow.getSizeInBytes() == unsafeRow.getSizeInBytes)
    assert(emptyRow.getInt(0) == unsafeRow.getInt(0))
    assert(emptyRow.getUTF8String(1) == unsafeRow.getUTF8String(1))
    // make sure we reuse the buffer.
    assert(emptyRow.getBaseObject == buffer)

    // make sure we really copied the input row.
    unsafeRow.setInt(0, 2)
    assert(emptyRow.getInt(0) == 1)

    val longString = UTF8String.fromString((1 to 100).map(_ => "abc").reduce(_ + _))
    val row2 = InternalRow(3, longString)
    val unsafeRow2 = converter.apply(row2)

    // make sure we can resize.
    emptyRow.copyFrom(unsafeRow2)
    assert(emptyRow.getSizeInBytes() == unsafeRow2.getSizeInBytes)
    assert(emptyRow.getInt(0) == 3)
    assert(emptyRow.getUTF8String(1) == longString)
    // make sure we really resized.
    assert(emptyRow.getBaseObject != buffer)

    // make sure we can still handle small rows after resize.
    emptyRow.copyFrom(unsafeRow)
    assert(emptyRow.getSizeInBytes() == unsafeRow.getSizeInBytes)
    assert(emptyRow.getInt(0) == unsafeRow.getInt(0))
    assert(emptyRow.getUTF8String(1) == unsafeRow.getUTF8String(1))
  }
}
