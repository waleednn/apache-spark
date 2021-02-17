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

package org.apache.spark.util

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.internal.Logging

object TestObjectLyftUtils {
  var testVar = 0L
  def setVal() = {
    testVar = 1L
  }
}

class LyftUtilsSuite extends SparkFunSuite with ResetSystemProperties with Logging {

  test("callObjectMethodNoArguments") {
    // Test calling the method using reflection 1
    val v = LyftUtils.callObjectMethodNoArguments("org.apache.spark.util.TestObjectLyftUtils$", "setVal")
    assert(v === true)
    assert(TestObjectLyftUtils.testVar === 1)
    assert(false ==
      LyftUtils.callObjectMethodNoArguments("org.apache.spark.util.TestObjectLyftUtils$", "setVal1"))
  }
}
