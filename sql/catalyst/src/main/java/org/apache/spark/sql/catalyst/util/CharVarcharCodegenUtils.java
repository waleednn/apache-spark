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

package org.apache.spark.sql.catalyst.util;

import org.apache.spark.unsafe.types.UTF8String;

public class CharVarcharCodegenUtils {
  private static final UTF8String SPACE = UTF8String.fromString(" ");

  public static UTF8String lengthCheck(UTF8String inputStr, int limit) {
    if (inputStr != null && inputStr.numChars() > limit) {
      throw new RuntimeException("Exceeds varchar type length limitation: " + limit);
    }
    return inputStr;
  }

  public static UTF8String paddingWithLengthCheck(UTF8String inputStr, int limit) {
    if (inputStr == null) return null;
    if (inputStr.numChars() > limit) {
      throw new RuntimeException("Exceeds char type length limitation: " + limit);
    }
    return inputStr.rpad(limit, SPACE);
  }
}
