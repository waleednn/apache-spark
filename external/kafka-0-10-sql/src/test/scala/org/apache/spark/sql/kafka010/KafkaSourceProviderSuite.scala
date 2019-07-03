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

import java.util.Locale

import scala.collection.JavaConverters._

import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterEach, PrivateMethodTester}

import org.apache.spark.{SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.sql.sources.v2.reader.Scan
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class KafkaSourceProviderSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with PrivateMethodTester {

  private val pollTimeoutMsMethod = PrivateMethod[Long]('pollTimeoutMs)
  private val maxOffsetsPerTriggerMethod = PrivateMethod[Option[Long]]('maxOffsetsPerTrigger)

  override protected def afterEach(): Unit = {
    SparkEnv.set(null)
  }

  test("micro-batch mode - options should be handled as case-insensitive") {
    def verifyFieldsInMicroBatchStream(
        options: CaseInsensitiveStringMap,
        expectedPollTimeoutMs: Long,
        expectedMaxOffsetsPerTrigger: Option[Long]): Unit = {
      // KafkaMicroBatchStream reads Spark conf from SparkEnv for default value
      // hence we set mock SparkEnv here before creating KafkaMicroBatchStream
      val sparkEnv = mock(classOf[SparkEnv])
      when(sparkEnv.conf).thenReturn(new SparkConf())
      SparkEnv.set(sparkEnv)

      val scan = getKafkaDataSourceScan(options)
      val stream = scan.toMicroBatchStream("dummy").asInstanceOf[KafkaMicroBatchStream]

      assert(expectedPollTimeoutMs === getField(stream, pollTimeoutMsMethod))
      assert(expectedMaxOffsetsPerTrigger === getField(stream, maxOffsetsPerTriggerMethod))
    }

    // upper-case
    val expectedValue = 1000L
    val mapWithUppercase = buildKafkaSourceCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toUpperCase(Locale.ROOT) -> expectedValue.toString,
      KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER.toUpperCase(Locale.ROOT) -> expectedValue.toString
    )
    verifyFieldsInMicroBatchStream(mapWithUppercase, expectedValue, Some(expectedValue))

    // lower-case
    val mapWithLowercase = buildKafkaSourceCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toLowerCase(Locale.ROOT) -> expectedValue.toString,
      KafkaSourceProvider.MAX_OFFSET_PER_TRIGGER.toLowerCase(Locale.ROOT) -> expectedValue.toString
    )
    verifyFieldsInMicroBatchStream(mapWithLowercase, expectedValue, Some(expectedValue))
  }

  test("SPARK-28142 - continuous mode - options should be handled as case-insensitive") {
    def verifyFieldsInContinuousStream(
        options: CaseInsensitiveStringMap,
        expectedPollTimeoutMs: Long): Unit = {
      val scan = getKafkaDataSourceScan(options)
      val stream = scan.toContinuousStream("dummy").asInstanceOf[KafkaContinuousStream]
      assert(expectedPollTimeoutMs === getField(stream, pollTimeoutMsMethod))
    }

    // upper-case
    val expectedValue = 1000
    val mapWithUppercase = buildKafkaSourceCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toUpperCase(Locale.ROOT) -> expectedValue.toString)
    verifyFieldsInContinuousStream(mapWithUppercase, expectedValue)

    // lower-case
    val mapWithLowercase = buildKafkaSourceCaseInsensitiveStringMap(
      KafkaSourceProvider.CONSUMER_POLL_TIMEOUT.toLowerCase(Locale.ROOT) -> expectedValue.toString)
    verifyFieldsInContinuousStream(mapWithLowercase, expectedValue)
  }

  private def buildKafkaSourceCaseInsensitiveStringMap(
      options: (String, String)*): CaseInsensitiveStringMap = {
    val requiredOptions = Map("kafka.bootstrap.servers" -> "dummy", "subscribe" -> "dummy")
    new CaseInsensitiveStringMap((options.toMap ++ requiredOptions).asJava)
  }

  private def getKafkaDataSourceScan(options: CaseInsensitiveStringMap): Scan = {
    val provider = new KafkaSourceProvider()
    provider.getTable(options).newScanBuilder(options).build()
  }

  private def getField[T](obj: AnyRef, method: PrivateMethod[T]): T = {
    obj.invokePrivate(method())
  }
}
