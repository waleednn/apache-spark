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

package org.apache.spark.sql.catalyst.expressions.codegen;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write data into global row buffer using `UnsafeArrayData` format,
 * used by {@link org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection}.
 */
public class UnsafeArrayWriter {

  private MemoryBlockHolder holder;

  // The offset of the global buffer where we start to write this array.
  private int startingOffset;

  public void initialize(MemoryBlockHolder holder, int numElements, int fixedElementSize) {
    // We need 4 bytes to store numElements and 4 bytes each element to store offset.
    final int fixedSize = 4 + 4 * numElements;

    this.holder = holder;
    this.startingOffset = holder.cursor;

    holder.grow(fixedSize);
    Platform.putInt(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, numElements);
    holder.cursor += fixedSize;

    // Grows the global buffer ahead for fixed size data.
    holder.grow(fixedElementSize * numElements);
  }

  private long getElementOffset(int ordinal) {
    return startingOffset + 4 + 4 * ordinal;
  }

  public void setNullAt(int ordinal) {
    final int relativeOffset = holder.cursor - startingOffset;
    // Writes negative offset value to represent null element.
    Platform.putInt(holder.getBaseObject(), holder.getBaseOffset() + getElementOffset(ordinal), -relativeOffset);
  }

  public void setOffset(int ordinal) {
    final int relativeOffset = holder.cursor - startingOffset;
    Platform.putInt(holder.getBaseObject(), holder.getBaseOffset() + getElementOffset(ordinal), relativeOffset);
  }

  public void write(int ordinal, boolean value) {
    Platform.putBoolean(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 1;
  }

  public void write(int ordinal, byte value) {
    Platform.putByte(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 1;
  }

  public void write(int ordinal, short value) {
    Platform.putShort(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 2;
  }

  public void write(int ordinal, int value) {
    Platform.putInt(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 4;
  }

  public void write(int ordinal, long value) {
    Platform.putLong(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 8;
  }

  public void write(int ordinal, float value) {
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }
    Platform.putFloat(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 4;
  }

  public void write(int ordinal, double value) {
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }
    Platform.putDouble(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, value);
    setOffset(ordinal);
    holder.cursor += 8;
  }

  public void write(int ordinal, Decimal input, int precision, int scale) {
    // make sure Decimal object has the same scale as DecimalType
    if (input.changePrecision(precision, scale)) {
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        Platform.putLong(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, input.toUnscaledLong());
        setOffset(ordinal);
        holder.cursor += 8;
      } else {
        final byte[] bytes = input.toJavaBigDecimal().unscaledValue().toByteArray();
        assert bytes.length <= 16;
        holder.grow(bytes.length);

        // Write the bytes to the variable length portion.
        Platform.copyMemory(
          bytes, Platform.BYTE_ARRAY_OFFSET, holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, bytes.length);
        setOffset(ordinal);
        holder.cursor += bytes.length;
      }
    } else {
      setNullAt(ordinal);
    }
  }

  public void write(int ordinal, UTF8String input) {
    final int numBytes = input.numBytes();

    // grow the global buffer before writing data.
    holder.grow(numBytes);

    // Write the bytes to the variable length portion.
    input.writeToMemory(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor);

    setOffset(ordinal);

    // move the cursor forward.
    holder.cursor += numBytes;
  }

  public void write(int ordinal, byte[] input) {
    // grow the global buffer before writing data.
    holder.grow(input.length);

    // Write the bytes to the variable length portion.
    Platform.copyMemory(
      input, Platform.BYTE_ARRAY_OFFSET, holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, input.length);

    setOffset(ordinal);

    // move the cursor forward.
    holder.cursor += input.length;
  }

  public void write(int ordinal, CalendarInterval input) {
    // grow the global buffer before writing data.
    holder.grow(16);

    // Write the months and microseconds fields of Interval to the variable length portion.
    Platform.putLong(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor, input.months);
    Platform.putLong(holder.getBaseObject(), holder.getBaseOffset() + holder.cursor + 8, input.microseconds);

    setOffset(ordinal);

    // move the cursor forward.
    holder.cursor += 16;
  }
}
