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

package org.apache.spark.sql.kafka010

import java.util.{HashMap => JavaMap}

import org.scalatest.PrivateMethodTester

import org.apache.spark.SparkFunSuite

class CachedKafkaConsumerSuite extends SparkFunSuite with PrivateMethodTester {

  test("SPARK-19886: Report error cause correctly in reportDataLoss") {
    val cause = new Exception("D'oh!")
    val consumer = CachedKafkaConsumer.getOrCreate("topic", 1, new JavaMap[String, Object]())
    try {
      val reportDataLoss = PrivateMethod[Unit]('reportDataLoss)
      val e = intercept[IllegalStateException] {
        consumer.invokePrivate(reportDataLoss(true, "message", cause))
      }
      assert(e.getCause === cause)
    } finally {
      CachedKafkaConsumer.removeKafkaConsumer("topic", 1, new JavaMap[String, Object]())
    }
  }
}
