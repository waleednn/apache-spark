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

package org.apache.spark.sql.execution.streaming

import java.io.Serializable
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.mutable

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, StreamTest, Trigger}
import org.apache.spark.sql.test.SharedSQLContext

class ForeachSinkSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("foreach() with `append` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(2).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Append)
        .foreach(new TestForeachWriter())
        .start()

      def verifyOutput(expectedVersion: Int, expectedData: Seq[Int]): Unit = {
        import ForeachSinkSuite._

        val events = ForeachSinkSuite.allEvents()
        assert(events.size === 2) // one seq of events for each of the 2 partitions

        // Verify both seq of events have an Open event as the first event
        assert(events.map(_.head).toSet === Set(0, 1).map(p => Open(p, expectedVersion)))

        // Verify all the Process event correspond to the expected data
        val allProcessEvents = events.flatMap(_.filter(_.isInstanceOf[Process[_]]))
        assert(allProcessEvents.toSet === expectedData.map { data => Process(data) }.toSet)

        // Verify both seq of events have a Close event as the last event
        assert(events.map(_.last).toSet === Set(Close(None), Close(None)))
      }

      // -- batch 0 ---------------------------------------
      ForeachSinkSuite.clear()
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()
      verifyOutput(expectedVersion = 0, expectedData = 1 to 4)

      // -- batch 1 ---------------------------------------
      ForeachSinkSuite.clear()
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()
      verifyOutput(expectedVersion = 1, expectedData = 5 to 8)

      query.stop()
    }
  }

  test("foreach() with `complete` output mode") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]

      val query = input.toDS()
        .groupBy().count().as[Long].map(_.toInt)
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .outputMode(OutputMode.Complete)
        .foreach(new TestForeachWriter())
        .start()

      // -- batch 0 ---------------------------------------
      input.addData(1, 2, 3, 4)
      query.processAllAvailable()

      var allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      var expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 4),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      ForeachSinkSuite.clear()

      // -- batch 1 ---------------------------------------
      input.addData(5, 6, 7, 8)
      query.processAllAvailable()

      allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 1),
        ForeachSinkSuite.Process(value = 8),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))

      query.stop()
    }
  }

  testQuietly("foreach with error") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(value)
            throw new RuntimeException("error")
          }
        }).start()
      input.addData(1, 2, 3, 4)

      // Error in `process` should fail the Spark job
      val e = intercept[StreamingQueryException] {
        query.processAllAvailable()
      }
      assert(e.getCause.isInstanceOf[SparkException])
      assert(e.getCause.getCause.getCause.getMessage === "error")
      assert(query.isActive === false)

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      assert(allEvents(0)(0) === ForeachSinkSuite.Open(partition = 0, version = 0))
      assert(allEvents(0)(1) === ForeachSinkSuite.Process(value = 1))

      // `close` should be called with the error
      val errorEvent = allEvents(0)(2).asInstanceOf[ForeachSinkSuite.Close]
      assert(errorEvent.error.get.isInstanceOf[RuntimeException])
      assert(errorEvent.error.get.getMessage === "error")
    }
  }

  test("foreach with watermark: complete") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      .outputMode(OutputMode.Complete)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 1)
      val expectedEvents = Seq(
        ForeachSinkSuite.Open(partition = 0, version = 0),
        ForeachSinkSuite.Process(value = 3),
        ForeachSinkSuite.Close(None)
      )
      assert(allEvents === Seq(expectedEvents))
    } finally {
      query.stop()
    }
  }

  test("foreach with watermark: append") {
    val inputData = MemoryStream[Int]

    val windowedAggregation = inputData.toDF()
      .withColumn("eventTime", $"value".cast("timestamp"))
      .withWatermark("eventTime", "10 seconds")
      .groupBy(window($"eventTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"count".as[Long])
      .map(_.toInt)
      .repartition(1)

    val query = windowedAggregation
      .writeStream
      .outputMode(OutputMode.Append)
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      inputData.addData(25) // Advance watermark to 15 seconds
      query.processAllAvailable()
      inputData.addData(25) // Evict items less than previous watermark
      query.processAllAvailable()

      // There should be 3 batches and only does the last batch contain a value.
      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 3)
      val expectedEvents = Seq(
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 0),
          ForeachSinkSuite.Close(None)
        ),
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 1),
          ForeachSinkSuite.Close(None)
        ),
        Seq(
          ForeachSinkSuite.Open(partition = 0, version = 2),
          ForeachSinkSuite.Process(value = 3),
          ForeachSinkSuite.Close(None)
        )
      )
      assert(allEvents === expectedEvents)
    } finally {
      query.stop()
    }
  }

  test("foreach sink should support metrics") {
    val inputData = MemoryStream[Int]
    val query = inputData.toDS()
      .writeStream
      .foreach(new TestForeachWriter())
      .start()
    try {
      inputData.addData(10, 11, 12)
      query.processAllAvailable()
      val recentProgress = query.recentProgress.filter(_.numInputRows != 0).headOption
      assert(recentProgress.isDefined && recentProgress.get.numInputRows === 3,
        s"recentProgress[${query.recentProgress.toList}] doesn't contain correct metrics")
    } finally {
      query.stop()
    }
  }

  testQuietly("foreach does not reuse writers") {
    withTempDir { checkpointDir =>
      val input = MemoryStream[Int]
      val query = input.toDS().repartition(1).writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .foreach(new TestForeachWriter() {
          override def process(value: Int): Unit = {
            super.process(this.hashCode())
          }
        }).start()
      input.addData(0)
      query.processAllAvailable()
      input.addData(0)
      query.processAllAvailable()

      val allEvents = ForeachSinkSuite.allEvents()
      assert(allEvents.size === 2)
      assert(allEvents(0)(1).isInstanceOf[ForeachSinkSuite.Process[Int]])
      val firstWriterId = allEvents(0)(1).asInstanceOf[ForeachSinkSuite.Process[Int]].value
      assert(allEvents(1)(1).isInstanceOf[ForeachSinkSuite.Process[Int]])
      assert(
        allEvents(1)(1).asInstanceOf[ForeachSinkSuite.Process[Int]].value != firstWriterId,
        "writer was reused!")
    }
  }

  testQuietly("foreach sink for continuous query") {
    withTempDir { checkpointDir =>
      val query = spark.readStream
        .format("rate")
        .option("numPartitions", "1")
        .option("rowsPerSecond", "5")
        .load()
        .select('value.cast("INT"))
        .map(r => r.getInt(0))
        .writeStream
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .trigger(Trigger.Continuous(500))
        .foreach(new TestForeachWriter with Serializable {
          override def process(value: Int): Unit = {
            super.process(this.hashCode())
          }
        }).start()
      try {
        // Wait until we get 3 epochs with at least 3 events in them. This means we'll see
        // open, close, and at least 1 process.
        eventually(timeout(streamingTimeout)) {
          // Check
          assert(ForeachSinkSuite.allEvents().count(_.size >= 3) === 3)
        }

        val allEvents = ForeachSinkSuite.allEvents().filter(_.size >= 3)
        // Check open and close events.
        allEvents(0).head match {
          case ForeachSinkSuite.Open(0, _) =>
          case e => assert(false, s"unexpected event $e")
        }
        allEvents(1).head match {
          case ForeachSinkSuite.Open(0, _) =>
          case e => assert(false, s"unexpected event $e")
        }
        allEvents(2).head match {
          case ForeachSinkSuite.Open(0, _) =>
          case e => assert(false, s"unexpected event $e")
        }
        assert(allEvents(0).last == ForeachSinkSuite.Close(None))
        assert(allEvents(1).last == ForeachSinkSuite.Close(None))
        assert(allEvents(2).last == ForeachSinkSuite.Close(None))

        // Check the first Process event in each epoch, and also check the writer IDs
        // we packed in to make sure none got reused.
        val writerIds = (0 to 2).map { i =>
          allEvents(i)(1).asInstanceOf[ForeachSinkSuite.Process[Int]].value
        }
        assert(
          writerIds.toSet.size == 3,
          s"writer was reused! expected 3 unique writers but saw $writerIds")
      } finally {
        query.stop()
      }
    }
  }
}

/** A global object to collect events in the executor */
object ForeachSinkSuite {

  trait Event

  case class Open(partition: Long, version: Long) extends Event

  case class Process[T](value: T) extends Event

  case class Close(error: Option[Throwable]) extends Event

  private val _allEvents = new ConcurrentLinkedQueue[Seq[Event]]()

  def addEvents(events: Seq[Event]): Unit = {
    _allEvents.add(events)
  }

  def allEvents(): Seq[Seq[Event]] = {
    _allEvents.toArray(new Array[Seq[Event]](_allEvents.size()))
  }

  def clear(): Unit = {
    _allEvents.clear()
  }
}

/** A [[ForeachWriter]] that writes collected events to ForeachSinkSuite */
class TestForeachWriter extends ForeachWriter[Int] {
  ForeachSinkSuite.clear()

  private val events = mutable.ArrayBuffer[ForeachSinkSuite.Event]()

  override def open(partitionId: Long, version: Long): Boolean = {
    events += ForeachSinkSuite.Open(partition = partitionId, version = version)
    true
  }

  override def process(value: Int): Unit = {
    events += ForeachSinkSuite.Process(value)
  }

  override def close(errorOrNull: Throwable): Unit = {
    events += ForeachSinkSuite.Close(error = Option(errorOrNull))
    ForeachSinkSuite.addEvents(events)
  }
}
