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

package org.apache.spark.unsafe.array;

import static org.apache.spark.unsafe.PlatformDependent.*;

public class ByteArrayMethods {

  private ByteArrayMethods() {
    // Private constructor, since this class only contains static methods.
  }

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    int remainder = numBytes & 0x07;  // This is equivalent to `numBytes % 8`
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }

  /**
   * Optimized byte array equality check for 8-byte-word-aligned byte arrays.
   * @return true if the arrays are equal, false otherwise
   */
  public static boolean arrayEquals(
      Object leftBase,
      long leftOffset,
      Object rightBase,
      long rightOffset,
      final long length) {
    int i = 0;
    while (i <= length - 8) {
      if (UNSAFE.getLong(leftBase, leftOffset + i) != UNSAFE.getLong(rightBase, rightOffset + i)) {
        return false;
      }
      i += 8;
    }
    while (i < length) {
      if (UNSAFE.getByte(leftBase, leftOffset + i) != UNSAFE.getByte(rightBase, rightOffset + i)) {
        return false;
      }
      i += 1;
    }
    return true;
  }

  /**
   * Optimized hashCode of byte array, calculating word-by-word.
   *
   * Note: Returned hashCode is different than java.utils.Arrays.hashCode().
   */
  public static int arrayHashCode(Object base, long offset, long length) {
    long result = 1;
    final long last = offset + length;
    while (offset <= last - 8) {
      result += (result << 5) + UNSAFE.getLong(base, offset);
      offset += 8;
    }
    while (offset < last) {
      result += (result << 5) + UNSAFE.getLong(base, offset);
      offset += 1;
    }
    return (int) ((result >> 32) ^ result);
  }
}
