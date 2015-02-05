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

package org.apache.spark.streaming.kafka

import java.io.File

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import kafka.serializer.StringDecoder
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.concurrent.{Eventually, Timeouts}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.util.Utils

class DirectKafkaStreamSuite extends KafkaStreamSuiteBase
  with BeforeAndAfter with BeforeAndAfterAll with Eventually {
  val sparkConf = new SparkConf()
    .setMaster("local[4]")
    .setAppName(this.getClass.getSimpleName)

  val brokerHost = "localhost"

  val kafkaParams = Map(
    "metadata.broker.list" -> s"$brokerHost:$brokerPort",
    "auto.offset.reset" -> "smallest"
  )

  var ssc: StreamingContext = _
  var testDir: File = _

  override def beforeAll {
    setupKafka()
  }

  override def afterAll {
    tearDownKafka()
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
    if (testDir != null) {
      Utils.deleteRecursively(testDir)
    }
  }

  test("basic receiving with multiple topics") {
    val topics = Set("newA", "newB")
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      createTopic(t)
      produceAndSendMessage(t, data)
    }
    ssc = new StreamingContext(sparkConf, Milliseconds(200))
    val stream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)
    }
    var total = 0L

    stream.foreachRDD { rdd =>
    // Get the offset ranges in the RDD
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val collected = rdd.mapPartitionsWithIndex { (i, iter) =>
      // For each partition, get size of the range in the partition,
      // and the number of items in the partition
        val off = offsets(i)
        val all = iter.toSeq
        val partSize = all.size
        val rangeSize = off.untilOffset - off.fromOffset
        Iterator((partSize, rangeSize))
      }.collect

      // Verify whether number of elements in each partition
      // matches with the corresponding offset range
      collected.foreach { case (partSize, rangeSize) =>
        assert(partSize === rangeSize, "offset ranges are wrong")
      }
      total += collected.size  // Add up all the collected items
    }
    ssc.start()
    eventually(timeout(20000.milliseconds), interval(200.milliseconds)) {
      assert(total === data.values.sum * topics.size, "didn't get all messages")
    }
    ssc.stop()
  }

  // Test to verify the offset ranges can be recovered from the checkpoints
  test("offset recovery") {
    val topic = "recovery"
    createTopic(topic)
    testDir = Utils.createTempDir()

    // Send data to Kafka and wait for it to be received
    def sendDataAndWaitForReceive(data: Seq[Int]) {
      val strings = data.map { _.toString}
      produceAndSendMessage(topic, strings.map { _ -> 1}.toMap)
      eventually(timeout(10 seconds), interval(50 milliseconds)) {
        assert(strings.forall { DirectKafkaStreamSuite.collectedData.contains })
      }
    }

    // Setup the streaming context
    ssc = new StreamingContext(sparkConf, Milliseconds(100))
    val kafkaStream = withClue("Error creating direct stream") {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))
    }
    val keyedStream = kafkaStream.map { v => "key" -> v._2.toInt }
    val stateStream = keyedStream.updateStateByKey { (values: Seq[Int], state: Option[Int]) =>
      Some(values.sum + state.getOrElse(0))
    }
    ssc.checkpoint(testDir.getAbsolutePath)

    // This is to collect the raw data received from Kafka
    kafkaStream.foreachRDD { (rdd: RDD[(String, String)], time: Time) =>
      val data = rdd.map { _._2 }.collect()
      DirectKafkaStreamSuite.collectedData.appendAll(data)
    }

    // This is ensure all the data is eventually receiving only once
    stateStream.foreachRDD { (rdd: RDD[(String, Int)]) =>
      rdd.collect().headOption.foreach { x => DirectKafkaStreamSuite.total = x._2 }
    }
    ssc.start()

    // Send some data and wait for them to be received
    for (i <- (1 to 10).grouped(4)) {
      sendDataAndWaitForReceive(i)
    }

    // Verify that offset ranges were generated
    val offsetRangesBeforeStop = getOffsetRanges(kafkaStream)
    assert(offsetRangesBeforeStop.size >= 1, "No offset ranges generated")
    assert(
      offsetRangesBeforeStop.head._2.forall { _.fromOffset === 0 },
      "starting offset not zero"
    )
    ssc.stop()
    logInfo("====== RESTARTING ========")

    // Recover context from checkpoints
    ssc = new StreamingContext(testDir.getAbsolutePath)
    val recoveredStream = ssc.graph.getInputStreams().head.asInstanceOf[DStream[(String, String)]]

    // Verify offset ranges have been recovered
    val recoveredOffsetRanges = getOffsetRanges(recoveredStream)
    assert(recoveredOffsetRanges.size > 0, "No offset ranges recovered")
    val earlierOffsetRangesAsSets = offsetRangesBeforeStop.map { x => (x._1, x._2.toSet) }
    assert(
      recoveredOffsetRanges.forall { or =>
        earlierOffsetRangesAsSets.contains((or._1, or._2.toSet))
      },
      "Recovered ranges are not the same as the ones generated"
    )

    // Restart context, give more data and verify the total at the end
    // If the total is write that means each records has been received only once
    ssc.start()
    sendDataAndWaitForReceive(11 to 20)
    eventually(timeout(10 seconds), interval(50 milliseconds)) {
      assert(DirectKafkaStreamSuite.total === (1 to 20).sum)
    }
    ssc.stop()
  }

  /** Get the generated offset ranges from the DirectKafkaStream */
  private def getOffsetRanges[K, V](
      kafkaStream: DStream[(K, V)]): Seq[(Time, Array[OffsetRange])] = {
    kafkaStream.generatedRDDs.mapValues { rdd =>
      rdd.asInstanceOf[KafkaRDD[K, V, _, _, (K, V)]].offsetRanges
    }.toSeq.sortBy { _._1 }
  }
}

object DirectKafkaStreamSuite {
  val collectedData = new mutable.ArrayBuffer[String]()
  var total = -1L
}
