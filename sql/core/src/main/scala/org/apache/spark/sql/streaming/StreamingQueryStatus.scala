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

import java.{util => ju}

import scala.collection.JavaConverters._

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.execution.streaming.{LongOffset, OffsetSeq}
import org.apache.spark.util.JsonProtocol

/**
 * :: Experimental ::
 * A class used to report information about the progress of a [[StreamingQuery]].
 *
 * @param name Name of the query. This name is unique across all active queries.
 * @param id Id of the query. This id is unique across
 *          all queries that have been started in the current process.
 * @param timestamp Timestamp (ms) of when this query was generated.
 * @param inputRate Current rate (rows/sec) at which data is being generated by all the sources.
 * @param processingRate Current rate (rows/sec) at which the query is processing data from
 *                       all the sources.
 * @param latency  Current average latency between the data being available in source and the sink
 *                   writing the corresponding output.
 * @param sourceStatuses Current statuses of the sources.
 * @param sinkStatus Current status of the sink.
 * @param triggerDetails Low-level details of the currently active trigger (e.g. number of
 *                      rows processed in trigger, latency of intermediate steps, etc.).
 *                      If no trigger is active, then it will have details of the last completed
 *                      trigger.
 * @since 2.0.0
 */
@Experimental
class StreamingQueryStatus private(
  val name: String,
  val id: Long,
  val timestamp: Long,
  val inputRate: Double,
  val processingRate: Double,
  val latency: Option[Double],
  val sourceStatuses: Array[SourceStatus],
  val sinkStatus: SinkStatus,
  val triggerDetails: ju.Map[String, String]) {

  import StreamingQueryStatus._

  /** The compact JSON representation of this status. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = {
    val sourceStatusLines = sourceStatuses.zipWithIndex.map { case (s, i) =>
      s"Source ${i + 1} - " + indent(s.prettyString).trim
    }
    val sinkStatusLines = sinkStatus.prettyString.trim
    val triggerDetailsLines = triggerDetails.asScala.map { case (k, v) => s"$k: $v" }.toSeq.sorted
    val numSources = sourceStatuses.length
    val numSourcesString = s"$numSources source" + { if (numSources > 1) "s" else "" }

    val allLines =
      s"""|Query id: $id
          |Status timestamp: $timestamp
          |Input rate: $inputRate rows/sec
          |Processing rate $processingRate rows/sec
          |Latency: ${latency.getOrElse("-")} ms
          |Trigger details:
          |${indent(triggerDetailsLines)}
          |Source statuses [$numSourcesString]:
          |${indent(sourceStatusLines)}
          |Sink status - ${indent(sinkStatusLines).trim}""".stripMargin

    s"Status of query '$name'\n${indent(allLines)}"
  }

  private[sql] def jsonValue: JValue = {
    ("name" -> JString(name)) ~
    ("id" -> JInt(id)) ~
    ("timestamp" -> JInt(timestamp)) ~
    ("inputRate" -> JDouble(inputRate)) ~
    ("processingRate" -> JDouble(processingRate)) ~
    ("latency" -> latency.map(JDouble).getOrElse(JNothing)) ~
    ("triggerDetails" -> JsonProtocol.mapToJson(triggerDetails.asScala))
    ("sourceStatuses" -> JArray(sourceStatuses.map(_.jsonValue).toList)) ~
    ("sinkStatus" -> sinkStatus.jsonValue)
  }
}

/** Companion object, primarily for creating StreamingQueryInfo instances internally */
private[sql] object StreamingQueryStatus {
  def apply(
      name: String,
      id: Long,
      timestamp: Long,
      inputRate: Double,
      processingRate: Double,
      latency: Option[Double],
      sourceStatuses: Array[SourceStatus],
      sinkStatus: SinkStatus,
      triggerDetails: Map[String, String]): StreamingQueryStatus = {
    new StreamingQueryStatus(name, id, timestamp, inputRate, processingRate,
      latency, sourceStatuses, sinkStatus, triggerDetails.asJava)
  }

  def indent(strings: Iterable[String]): String = strings.map(indent).mkString("\n")
  def indent(string: String): String = string.split("\n").map("    " + _).mkString("\n")

  /** Create an instance of status for python testing */
  def testStatus(): StreamingQueryStatus = {
    import org.apache.spark.sql.execution.streaming.StreamMetrics._
    StreamingQueryStatus(
      name = "query",
      id = 1,
      timestamp = 123,
      inputRate = 15.5,
      processingRate = 23.5,
      latency = Some(345),
      sourceStatuses = Array(
        SourceStatus(
          desc = "MySource1",
          offsetDesc = LongOffset(0).json,
          inputRate = 15.5,
          processingRate = 23.5,
          triggerDetails = Map(
            NUM_SOURCE_INPUT_ROWS -> "100",
            SOURCE_GET_OFFSET_LATENCY -> "10",
            SOURCE_GET_BATCH_LATENCY -> "20"))),
      sinkStatus = SinkStatus(
        desc = "MySink",
        offsetDesc = OffsetSeq(Some(LongOffset(1)) :: None :: Nil).toString),
      triggerDetails = Map(
        BATCH_ID -> "5",
        IS_TRIGGER_ACTIVE -> "true",
        IS_DATA_PRESENT_IN_TRIGGER -> "true",
        GET_OFFSET_LATENCY -> "10",
        GET_BATCH_LATENCY -> "20",
        NUM_INPUT_ROWS -> "100"
      ))
  }
}
