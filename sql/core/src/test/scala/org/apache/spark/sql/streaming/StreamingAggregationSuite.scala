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

package org.apache.spark.sql.streaming

import java.util.{Locale, TimeZone}
import java.util.concurrent.CountDownLatch

import org.scalatest.Assertions
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.rdd.BlockRDD
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.{SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.util.{MockSourceProvider, StreamManualClock}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockId, StorageLevel, TestBlockId}

object FailureSingleton {
  var firstTime = true
}

class StreamingAggregationSuite extends StateStoreMetricsTest
    with BeforeAndAfterAll with Assertions {

  override def afterAll(): Unit = {
    super.afterAll()
    StateStore.stop()
  }

  import testImplicits._

  test("simple count, update mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 3, 2),
      CheckLastBatch((3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 3), (2, 2), (1, 1)),
      // By default we run in new tuple mode.
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4))
    )
  }

  test("count distinct") {
    val inputData = MemoryStream[(Int, Seq[Int])]

    val aggregated =
      inputData.toDF()
        .select($"*", explode($"_2") as 'value)
        .groupBy($"_1")
        .agg(size(collect_set($"value")))
        .as[(Int, Int)]

    testStream(aggregated, Update)(
      AddData(inputData, (1, Seq(1, 2))),
      CheckLastBatch((1, 2))
    )
  }

  test("simple count, complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch((3, 1)),
      AddData(inputData, 2),
      CheckLastBatch((3, 1), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch((3, 2), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch((4, 4), (3, 2), (2, 2), (1, 1))
    )
  }

  test("simple count, append mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    val e = intercept[AnalysisException] {
      testStream(aggregated, Append)()
    }
    Seq("append", "not supported").foreach { m =>
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
    }
  }

  test("sort after aggregate in complete mode") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .toDF("value", "count")
        .orderBy($"count".desc)
        .as[(Int, Long)]

    testStream(aggregated, Complete)(
      AddData(inputData, 3),
      CheckLastBatch(isSorted = true, (3, 1)),
      AddData(inputData, 2, 3),
      CheckLastBatch(isSorted = true, (3, 2), (2, 1)),
      StopStream,
      StartStream(),
      AddData(inputData, 3, 2, 1),
      CheckLastBatch(isSorted = true, (3, 3), (2, 2), (1, 1)),
      AddData(inputData, 4, 4, 4, 4),
      CheckLastBatch(isSorted = true, (4, 4), (3, 3), (2, 2), (1, 1))
    )
  }

  test("state metrics") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDS()
        .flatMap(x => Seq(x, x + 1))
        .toDF("value")
        .groupBy($"value")
        .agg(count("*"))
        .as[(Int, Long)]

    implicit class RichStreamExecution(query: StreamExecution) {
      def stateNodes: Seq[SparkPlan] = {
        query.lastExecution.executedPlan.collect {
          case p if p.isInstanceOf[StateStoreSaveExec] => p
        }
      }
    }

    // Test with Update mode
    testStream(aggregated, Update)(
      AddData(inputData, 1),
      CheckLastBatch((1, 1), (2, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 2 },
      AddData(inputData, 2, 3),
      CheckLastBatch((2, 2), (3, 2), (4, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 4 }
    )

    // Test with Complete mode
    inputData.reset()
    testStream(aggregated, Complete)(
      AddData(inputData, 1),
      CheckLastBatch((1, 1), (2, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 2 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 2 },
      AddData(inputData, 2, 3),
      CheckLastBatch((1, 1), (2, 2), (3, 2), (4, 1)),
      AssertOnQuery { _.stateNodes.size === 1 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numOutputRows").get.value === 4 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numUpdatedStateRows").get.value === 3 },
      AssertOnQuery { _.stateNodes.head.metrics.get("numTotalStateRows").get.value === 4 }
    )
  }

  test("multiple keys") {
    val inputData = MemoryStream[Int]

    val aggregated =
      inputData.toDF()
        .groupBy($"value", $"value" + 1)
        .agg(count("*"))
        .as[(Int, Int, Long)]

    testStream(aggregated, Update)(
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 1), (2, 3, 1)),
      AddData(inputData, 1, 2),
      CheckLastBatch((1, 2, 2), (2, 3, 2))
    )
  }

  testQuietly("midbatch failure") {
    val inputData = MemoryStream[Int]
    FailureSingleton.firstTime = true
    val aggregated =
      inputData.toDS()
          .map { i =>
            if (i == 4 && FailureSingleton.firstTime) {
              FailureSingleton.firstTime = false
              sys.error("injected failure")
            }

            i
          }
          .groupBy($"value")
          .agg(count("*"))
          .as[(Int, Long)]

    testStream(aggregated, Update)(
      StartStream(),
      AddData(inputData, 1, 2, 3, 4),
      ExpectFailure[SparkException](),
      StartStream(),
      CheckLastBatch((1, 1), (2, 1), (3, 1), (4, 1))
    )
  }

  test("typed aggregators") {
    val inputData = MemoryStream[(String, Int)]
    val aggregated = inputData.toDS().groupByKey(_._1).agg(typed.sumLong(_._2))

    testStream(aggregated, Update)(
      AddData(inputData, ("a", 10), ("a", 20), ("b", 1), ("b", 2), ("c", 1)),
      CheckLastBatch(("a", 30), ("b", 3), ("c", 1))
    )
  }

  test("prune results by current_time, complete mode") {
    import testImplicits._
    val clock = new StreamManualClock
    val inputData = MemoryStream[Long]
    val aggregated =
      inputData.toDF()
        .groupBy($"value")
        .agg(count("*"))
        .where('value >= current_timestamp().cast("long") - 10L)

    testStream(aggregated, Complete)(
      StartStream(Trigger.ProcessingTime("10 seconds"), triggerClock = clock),

      // advance clock to 10 seconds, all keys retained
      AddData(inputData, 0L, 5L, 5L, 10L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((0L, 1), (5L, 2), (10L, 1)),

      // advance clock to 20 seconds, should retain keys >= 10
      AddData(inputData, 15L, 15L, 20L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((10L, 1), (15L, 2), (20L, 1)),

      // advance clock to 30 seconds, should retain keys >= 20
      AddData(inputData, 0L, 85L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((20L, 1), (85L, 1)),

      // bounce stream and ensure correct batch timestamp is used
      // i.e., we don't take it from the clock, which is at 90 seconds.
      StopStream,
      AssertOnQuery { q => // clear the sink
        q.sink.asInstanceOf[MemorySink].clear()
        q.batchCommitLog.purge(3)
        // advance by a minute i.e., 90 seconds total
        clock.advance(60 * 1000L)
        true
      },
      StartStream(Trigger.ProcessingTime("10 seconds"), triggerClock = clock),
      // The commit log blown, causing the last batch to re-run
      CheckLastBatch((20L, 1), (85L, 1)),
      AssertOnQuery { q =>
        clock.getTimeMillis() == 90000L
      },

      // advance clock to 100 seconds, should retain keys >= 90
      AddData(inputData, 85L, 90L, 100L, 105L),
      AdvanceManualClock(10 * 1000),
      CheckLastBatch((90L, 1), (100L, 1), (105L, 1))
    )
  }

  test("prune results by current_date, complete mode") {
    import testImplicits._
    val clock = new StreamManualClock
    val tz = TimeZone.getDefault.getID
    val inputData = MemoryStream[Long]
    val aggregated =
      inputData.toDF()
        .select(to_utc_timestamp(from_unixtime('value * DateTimeUtils.SECONDS_PER_DAY), tz))
        .toDF("value")
        .groupBy($"value")
        .agg(count("*"))
        .where($"value".cast("date") >= date_sub(current_date(), 10))
        .select(($"value".cast("long") / DateTimeUtils.SECONDS_PER_DAY).cast("long"), $"count(1)")
    testStream(aggregated, Complete)(
      StartStream(Trigger.ProcessingTime("10 day"), triggerClock = clock),
      // advance clock to 10 days, should retain all keys
      AddData(inputData, 0L, 5L, 5L, 10L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((0L, 1), (5L, 2), (10L, 1)),
      // advance clock to 20 days, should retain keys >= 10
      AddData(inputData, 15L, 15L, 20L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((10L, 1), (15L, 2), (20L, 1)),
      // advance clock to 30 days, should retain keys >= 20
      AddData(inputData, 85L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((20L, 1), (85L, 1)),

      // bounce stream and ensure correct batch timestamp is used
      // i.e., we don't take it from the clock, which is at 90 days.
      StopStream,
      AssertOnQuery { q => // clear the sink
        q.sink.asInstanceOf[MemorySink].clear()
        q.batchCommitLog.purge(3)
        // advance by 60 days i.e., 90 days total
        clock.advance(DateTimeUtils.MILLIS_PER_DAY * 60)
        true
      },
      StartStream(Trigger.ProcessingTime("10 day"), triggerClock = clock),
      // Commit log blown, causing a re-run of the last batch
      CheckLastBatch((20L, 1), (85L, 1)),

      // advance clock to 100 days, should retain keys >= 90
      AddData(inputData, 85L, 90L, 100L, 105L),
      AdvanceManualClock(DateTimeUtils.MILLIS_PER_DAY * 10),
      CheckLastBatch((90L, 1), (100L, 1), (105L, 1))
    )
  }

  test("SPARK-19690: do not convert batch aggregation in streaming query to streaming") {
    val streamInput = MemoryStream[Int]
    val batchDF = Seq(1, 2, 3, 4, 5)
        .toDF("value")
        .withColumn("parity", 'value % 2)
        .groupBy('parity)
        .agg(count("*") as 'joinValue)
    val joinDF = streamInput
        .toDF()
        .join(batchDF, 'value === 'parity)

    // make sure we're planning an aggregate in the first place
    assert(batchDF.queryExecution.optimizedPlan match { case _: Aggregate => true })

    testStream(joinDF, Append)(
      AddData(streamInput, 0, 1, 2, 3),
      CheckLastBatch((0, 0, 2), (1, 1, 3)),
      AddData(streamInput, 0, 1, 2, 3),
      CheckLastBatch((0, 0, 2), (1, 1, 3)))
  }

  test("SPARK-21977: coalesce(1) with 0 partition RDD should be repartitioned accordingly") {
    val inputSource = new NonLocalRelationSource(spark)
    MockSourceProvider.withMockSources(inputSource) {
      withTempDir { tempDir =>
        val aggregated: Dataset[Long] =
          spark.readStream
            .format((new MockSourceProvider).getClass.getCanonicalName)
            .load()
            .coalesce(1)
            .groupBy()
            .count()
            .as[Long]

        val sq = aggregated.writeStream
          .format("memory")
          .outputMode("complete")
          .queryName("agg_test")
          .option("checkpointLocation", tempDir.getAbsolutePath)
          .start()

        try {

          inputSource.addData(1)
          inputSource.releaseLock()
          sq.processAllAvailable()

          checkDataset(
            spark.table("agg_test").as[Long],
            1L)

          val restore1 = sq.asInstanceOf[StreamingQueryWrapper].streamingQuery
            .lastExecution.executedPlan
            .collect { case ss: StateStoreRestoreExec => ss }
            .head
          restore1.child match {
            case wscg: WholeStageCodegenExec =>
              assert(wscg.outputPartitioning.numPartitions === 1)
              assert(wscg.child.isInstanceOf[HashAggregateExec], "Shouldn't require shuffling")
            case _ => fail("Expected no shuffling")
          }

          inputSource.addData()
          inputSource.releaseLock()
          sq.processAllAvailable()

          val restore2 = sq.asInstanceOf[StreamingQueryWrapper].streamingQuery
            .lastExecution.executedPlan
            .collect { case ss: StateStoreRestoreExec => ss }
            .head
          restore2.child match {
            case shuffle: ShuffleExchange => assert(shuffle.newPartitioning.numPartitions === 1)
            case _ => fail("Expected shuffling when there was no data")
          }

          checkDataset(
            spark.table("agg_test").as[Long],
            1L)

          inputSource.addData(2, 3)
          inputSource.releaseLock()
          sq.processAllAvailable()

          checkDataset(
            spark.table("agg_test").as[Long],
            3L)

          inputSource.addData()
          inputSource.releaseLock()
          sq.processAllAvailable()

          checkDataset(
            spark.table("agg_test").as[Long],
            3L)
        } finally {
          sq.stop()
        }
      }
    }
  }

  test("SPARK-21977: coalesce(1) should still be repartitioned when it has keyExpressions") {
    val inputSource = new NonLocalRelationSource(spark)
    MockSourceProvider.withMockSources(inputSource) {
      withTempDir { tempDir =>

        val sq = spark.readStream
          .format((new MockSourceProvider).getClass.getCanonicalName)
          .load()
          .coalesce(1)
          .groupBy('a % 1) // just to give it a fake key
          .count()
          .as[(Long, Long)]
          .writeStream
          .format("memory")
          .outputMode("complete")
          .queryName("agg_test")
          .option("checkpointLocation", tempDir.getAbsolutePath)
          .start()

        try {

          inputSource.addData(1)
          inputSource.releaseLock()
          sq.processAllAvailable()

          val restore1 = sq.asInstanceOf[StreamingQueryWrapper].streamingQuery
            .lastExecution.executedPlan
            .collect { case ss: StateStoreRestoreExec => ss }
            .head
          restore1.child match {
            case shuffle: ShuffleExchange =>
              assert(shuffle.newPartitioning.numPartitions ===
                spark.sessionState.conf.numShufflePartitions)
            case _ => fail(s"Expected shuffling but got: ${restore1.child}")
          }

          checkDataset(
            spark.table("agg_test").as[(Long, Long)],
            (0L, 1L))

        } finally {
          sq.stop()
        }

        val sq2 = spark.readStream
          .format((new MockSourceProvider).getClass.getCanonicalName)
          .load()
          .coalesce(2)
          .groupBy('a % 1) // just to give it a fake key
          .count()
          .as[(Long, Long)]
          .writeStream
          .format("memory")
          .outputMode("complete")
          .queryName("agg_test")
          .option("checkpointLocation", tempDir.getAbsolutePath)
          .start()

        try {
          sq2.processAllAvailable()
          inputSource.addData(2)
          inputSource.addData(3)
          inputSource.addData(4)
          inputSource.releaseLock()
          sq2.processAllAvailable()

          val restore2 = sq2.asInstanceOf[StreamingQueryWrapper].streamingQuery
            .lastExecution.executedPlan
            .collect { case ss: StateStoreRestoreExec => ss }
            .head
          restore2.child match {
            case wscg: WholeStageCodegenExec =>
              assert(wscg.outputPartitioning.numPartitions ===
                spark.sessionState.conf.numShufflePartitions)
            case _ =>
              fail("Shouldn't require shuffling as HashAggregateExec should have asked for a " +
                s"shuffle. But got: ${restore2.child}")
          }

          checkDataset(
            spark.table("agg_test").as[(Long, Long)],
            (0L, 4L))

          inputSource.addData()
          inputSource.releaseLock()
          sq2.processAllAvailable()

          checkDataset(
            spark.table("agg_test").as[(Long, Long)],
            (0L, 4L))
        } finally {
          sq2.stop()
        }
      }
    }
  }
}

/**
 * LocalRelation has some optimized properties during Spark planning. In order for the bugs in
 * SPARK-21977 to occur, we need to create a logical relation from an existing RDD. We use a
 * BlockRDD since it accepts 0 partitions. One requirement for the one of the bugs is the use of
 * `coalesce(1)`, which has several optimizations regarding [[SinglePartition]], and a 0 partition
 * parentRDD.
 */
class NonLocalRelationSource(spark: SparkSession) extends Source {
  private var counter = 0L
  private val blockMgr = SparkEnv.get.blockManager
  private var blocks: Seq[BlockId] = Seq.empty

  private var streamLock: CountDownLatch = new CountDownLatch(1)

  def addData(data: Int*): Unit = {
    if (streamLock.getCount == 0) {
      streamLock = new CountDownLatch(1)
    }
    synchronized {
      if (data.nonEmpty) {
        counter += data.length
        val id = TestBlockId(counter.toString)
        blockMgr.putIterator(id, data.iterator, StorageLevel.MEMORY_ONLY)
        blocks ++= id :: Nil
      } else {
        counter += 1
      }
    }
  }

  def releaseLock(): Unit = streamLock.countDown()

  override def getOffset: Option[Offset] = {
    streamLock.await()
    synchronized {
      if (counter == 0) None else Some(LongOffset(counter))
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = synchronized {
    val rdd = new BlockRDD[Int](spark.sparkContext, blocks.toArray)
      .map(i => InternalRow(i)) // we don't really care about the values in this test
    blocks = Seq.empty
    spark.internalCreateDataFrame(rdd, schema, isStreaming = true).toDF()
  }
  override def schema: StructType = MockSourceProvider.fakeSchema
  override def stop(): Unit = {
    blockMgr.getMatchingBlockIds(_.isInstanceOf[TestBlockId]).foreach(blockMgr.removeBlock(_))
  }
}
