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

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.kafka.common.TopicPartition
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

/**
 * Utilities for converting Kafka related objects to and from json.
 */
private object JsonUtils {
  private implicit val formats = Serialization.formats(NoTypeHints)
  implicit val ordering = new Ordering[TopicPartition] {
    override def compare(x: TopicPartition, y: TopicPartition): Int = {
      Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
    }
  }

  /**
   * Read TopicPartitions from json string
   */
  def partitions(str: String): Array[TopicPartition] = {
    try {
      Serialization.read[Map[String, Seq[Int]]](str).flatMap {  case (topic, parts) =>
          parts.map { part =>
            new TopicPartition(topic, part)
          }
      }.toArray
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":[0,1],"topicB":[0,1]}, got $str""")
    }
  }

  /**
   * Write TopicPartitions as json string
   */
  def partitions(partitions: Iterable[TopicPartition]): String = {
    val result = HashMap.empty[String, List[Int]]
    partitions.foreach { tp =>
      val parts: List[Int] = result.getOrElse(tp.topic, Nil)
      result += tp.topic -> (tp.partition::parts)
    }
    Serialization.write(result)
  }

  /**
   * Read per-TopicPartition offsets from json string
   */
  def partitionOffsets(str: String): Map[TopicPartition, Long] = {
    try {
      Serialization.read[Map[String, Map[Int, Long]]](str).flatMap { case (topic, partOffsets) =>
          partOffsets.map { case (part, offset) =>
              new TopicPartition(topic, part) -> offset
          }
      }.toMap
    } catch {
      case NonFatal(x) =>
        throw new IllegalArgumentException(
          s"""Expected e.g. {"topicA":{"0":23,"1":-1},"topicB":{"0":-2}}, got $str""")
    }
  }

  /**
   * Write per-TopicPartition offsets as json string
   */
  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = HashMap.empty[String, HashMap[Int, Long]]
    val partitions = partitionOffsets.keySet.toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
        val off = partitionOffsets(tp)
        val parts = result.getOrElse(tp.topic, HashMap.empty[Int, Long])
        parts += tp.partition -> off
        result += tp.topic -> parts
    }
    Serialization.write(result)
  }

  /**
   * Write per-topic partition lag as json string
   */
  def partitionLags(
      latestOffsets: Map[TopicPartition, Long],
      processedOffsets: Map[TopicPartition, Long]): String = {
    val result = HashMap.empty[String, HashMap[Int, Long]]
    val partitions = latestOffsets.keySet.toSeq.sorted
    partitions.foreach { tp =>
      val lag = latestOffsets(tp) - processedOffsets.getOrElse(tp, 0L)
      val parts = result.getOrElse(tp.topic, HashMap.empty[Int, Long])
      parts += tp.partition -> lag
      result += tp.topic -> parts
    }
    Serialization.write(Map("lag" -> result))
  }
}
