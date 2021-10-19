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

package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.streaming.{StreamTest, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock

class RatePerEpochProviderSuite extends StreamTest {

  import testImplicits._

  test("RatePerEpochProvider in registry") {
    val ds = DataSource.lookupDataSource("rate-epoch", spark.sqlContext.conf).newInstance()
    assert(ds.isInstanceOf[RatePerEpochProvider], "Could not find rate-epoch source")
  }

  test("microbatch - basic") {
    val input = spark.readStream
      .format("rate-epoch")
      .option("rowsPerEpoch", "10")
      .option("startTimestamp", "1000")
      .option("advanceMillisPerEpoch", "50")
      .load()
    val clock = new StreamManualClock
    testStream(input)(
      StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
      AdvanceManualClock(10),
      CheckLastBatch((0 until 10).map(v => new java.sql.Timestamp(1000L) -> v): _*),
      AdvanceManualClock(10),
      CheckLastBatch((10 until 20).map(v => new java.sql.Timestamp(1050L) -> v): _*),
      AdvanceManualClock(10),
      CheckLastBatch((20 until 30).map(v => new java.sql.Timestamp(1100L) -> v): _*)
    )
  }

  test("microbatch - restart") {
    withTempDir { dir =>
      val input = spark.readStream
        .format("rate-epoch")
        .option("rowsPerEpoch", "10")
        .load()
        .select('value)

      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        Execute(_.awaitOffset(0, RatePerEpochStreamOffset(20, 2000), streamingTimeout.toMillis)),
        CheckAnswer(0 until 20: _*),
        StopStream
      )

      testStream(input)(
        StartStream(checkpointLocation = dir.getAbsolutePath),
        Execute(_.awaitOffset(0, RatePerEpochStreamOffset(40, 4000), streamingTimeout.toMillis)),
        CheckAnswer(20 until 40: _*)
      )
    }
  }

  test("numPartitions") {
    val input = spark.readStream
      .format("rate-epoch")
      .option("rowsPerEpoch", "10")
      .option("numPartitions", "6")
      .load()
      .select(spark_partition_id())
      .distinct()
    val clock = new StreamManualClock
    testStream(input)(
      StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
      AdvanceManualClock(10),
      CheckLastBatch(0 until 6: _*)
    )
  }

  testQuietly("illegal option values") {
    def testIllegalOptionValue(
        option: String,
        value: String,
        expectedMessages: Seq[String]): Unit = {
      val e = intercept[IllegalArgumentException] {
        var stream = spark.readStream
          .format("rate-epoch")
          .option(option, value)

        if (option != "rowsPerEpoch") {
          stream = stream.option("rowsPerEpoch", "1")
        }

        stream.load()
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }
      for (msg <- expectedMessages) {
        assert(e.getMessage.contains(msg))
      }
    }

    testIllegalOptionValue("rowsPerEpoch", "-1", Seq("-1", "rowsPerEpoch", "positive"))
    testIllegalOptionValue("rowsPerEpoch", "0", Seq("0", "rowsPerEpoch", "positive"))
    testIllegalOptionValue("numPartitions", "-1", Seq("-1", "numPartitions", "positive"))
    testIllegalOptionValue("numPartitions", "0", Seq("0", "numPartitions", "positive"))

    // RatePerEpochProvider allows setting below options to 0
    testIllegalOptionValue("advanceMillisPerEpoch", "-1",
      Seq("-1", "advanceMillisPerEpoch", "non-negative"))
    testIllegalOptionValue("startTimestamp", "-1", Seq("-1", "startTimestamp", "non-negative"))
  }

  test("user-specified schema given") {
    val exception = intercept[UnsupportedOperationException] {
      spark.readStream
        .format("rate-epoch")
        .option("rowsPerEpoch", "10")
        .schema(spark.range(1).schema)
        .load()
    }
    assert(exception.getMessage.contains(
      "RatePerEpochProvider source does not support user-specified schema"))
  }
}
