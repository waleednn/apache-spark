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

package org.apache.spark.sql.connect.service

import java.util.concurrent.Future

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{Request, Response}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.connect.command.SparkConnectCommandPlanner
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AdaptiveSparkPlanHelper, QueryStageExec}
import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.ThreadUtils

class SparkConnectStreamHandler(responseObserver: StreamObserver[Response]) extends Logging {

  // The maximum batch size in bytes for a single batch of data to be returned via proto.
  val MAX_BATCH_SIZE: Long = 10 * 1024 * 1024

  def handle(v: Request): Unit = {
    val session =
      SparkConnectService.getOrCreateIsolatedSession(v.getUserContext.getUserId).session
    v.getPlan.getOpTypeCase match {
      case proto.Plan.OpTypeCase.COMMAND => handleCommand(session, v)
      case proto.Plan.OpTypeCase.ROOT => handlePlan(session, v)
      case _ =>
        throw new UnsupportedOperationException(s"${v.getPlan.getOpTypeCase} not supported.")
    }
  }

  def handlePlan(session: SparkSession, request: Request): Unit = {
    // Extract the plan from the request and convert it to a logical plan
    val planner = new SparkConnectPlanner(request.getPlan.getRoot, session)
    val dataframe = Dataset.ofRows(session, planner.transform())
    // check whether all data types are supported
    if (Try {
        ArrowUtils.toArrowSchema(dataframe.schema, session.sessionState.conf.sessionLocalTimeZone)
      }.isSuccess) {
      processRowsAsArrowBatches(request.getClientId, dataframe)
    } else {
      processRowsAsJsonBatches(request.getClientId, dataframe)
    }
  }

  def processRowsAsJsonBatches(clientId: String, dataframe: DataFrame): Unit = {
    // Only process up to 10MB of data.
    val sb = new StringBuilder
    var batchId = 0L
    var rowCount = 0
    dataframe.toJSON
      .collect()
      .foreach(row => {

        // There are a few cases to cover here.
        // 1. The aggregated buffer size is larger than the MAX_BATCH_SIZE
        //     -> send the current batch and reset.
        // 2. The aggregated buffer size is smaller than the MAX_BATCH_SIZE
        //     -> append the row to the buffer.
        // 3. The row in question is larger than the MAX_BATCH_SIZE
        //     -> fail the query.

        // Case 3. - Fail
        if (row.size > MAX_BATCH_SIZE) {
          throw SparkException.internalError(
            s"Serialized row is larger than MAX_BATCH_SIZE: ${row.size} > ${MAX_BATCH_SIZE}")
        }

        // Case 1 - FLush and send.
        if (sb.size + row.size > MAX_BATCH_SIZE) {
          val response = proto.Response.newBuilder().setClientId(clientId)
          val batch = proto.Response.JSONBatch
            .newBuilder()
            .setBatchId(batchId)
            .setData(ByteString.copyFromUtf8(sb.toString()))
            .setRowCount(rowCount)
            .build()
          response.setJsonBatch(batch)
          responseObserver.onNext(response.build())
          sb.clear()
          batchId += 1
          sb.append(row)
          rowCount = 1
        } else {
          // Case 2 - Append.
          // Make sure to put the newline delimiters only between items and not at the end.
          if (rowCount > 0) {
            sb.append("\n")
          }
          sb.append(row)
          rowCount += 1
        }
      })

    // If the last batch is not empty, send out the data to the client.
    if (sb.size > 0) {
      val response = proto.Response.newBuilder().setClientId(clientId)
      val batch = proto.Response.JSONBatch
        .newBuilder()
        .setData(ByteString.copyFromUtf8(sb.toString()))
        .setRowCount(rowCount)
        .build()
      response.setJsonBatch(batch)
      responseObserver.onNext(response.build())
    }

    responseObserver.onNext(sendMetricsToResponse(clientId, dataframe))
    responseObserver.onCompleted()
  }

  def processRowsAsArrowBatches(clientId: String, dataframe: DataFrame): Unit = {
    val spark = dataframe.sparkSession
    val schema = dataframe.schema
    // TODO: control the batch size instead of max records
    val maxRecordsPerBatch = spark.sessionState.conf.arrowMaxRecordsPerBatch
    val timeZoneId = spark.sessionState.conf.sessionLocalTimeZone

    SQLExecution.withNewExecutionId(dataframe.queryExecution, Some("collectArrow")) {
      val pool = ThreadUtils.newDaemonSingleThreadExecutor("connect-collect-arrow")
      val tasks = collection.mutable.ArrayBuffer.empty[Future[_]]
      val rows = dataframe.queryExecution.executedPlan.execute()

      if (rows.getNumPartitions > 0) {
        val batches = rows.mapPartitionsInternal { iter =>
          ArrowConverters
            .toArrowBatchIterator(iter, schema, maxRecordsPerBatch, timeZoneId)
        }

        val processPartition = (iter: Iterator[(Array[Byte], Long, Long)]) => iter.toArray

        val resultHandler = (partitionId: Int, taskResult: Array[(Array[Byte], Long, Long)]) => {
          if (taskResult.exists(_._1.nonEmpty)) {
            // only send non-empty partitions
            val task = pool.submit(new Runnable {
              override def run(): Unit = {
                var batchId = partitionId.toLong << 33
                taskResult.foreach { case (bytes, count, size) =>
                  val response = proto.Response.newBuilder().setClientId(clientId)
                  val batch = proto.Response.ArrowBatch
                    .newBuilder()
                    .setBatchId(batchId)
                    .setRowCount(count)
                    .setUncompressedBytes(size)
                    .setCompressedBytes(bytes.length)
                    .setData(ByteString.copyFrom(bytes))
                    .build()
                  response.setArrowBatch(batch)
                  responseObserver.onNext(response.build())
                  batchId += 1
                }
              }
            })
            tasks.synchronized {
              tasks.append(task)
            }
          }
          val i = 0 // Unit
        }

        spark.sparkContext.runJob(batches, processPartition, resultHandler)
      }

      // make sure at least 1 batch will be sent
      if (tasks.isEmpty) {
        val task = pool.submit(new Runnable {
          override def run(): Unit = {
            val (bytes, count, size) = ArrowConverters.createEmptyArrowBatch(schema, timeZoneId)
            val response = proto.Response.newBuilder().setClientId(clientId)
            val batch = proto.Response.ArrowBatch
              .newBuilder()
              .setBatchId(0L)
              .setRowCount(count)
              .setUncompressedBytes(size)
              .setCompressedBytes(bytes.length)
              .setData(ByteString.copyFrom(bytes))
              .build()
            response.setArrowBatch(batch)
            responseObserver.onNext(response.build())
          }
        })
        tasks.append(task)
      }

      tasks.foreach(_.get())
      pool.shutdown()

      responseObserver.onNext(sendMetricsToResponse(clientId, dataframe))
      responseObserver.onCompleted()
    }
  }

  def sendMetricsToResponse(clientId: String, rows: DataFrame): Response = {
    // Send a last batch with the metrics
    Response
      .newBuilder()
      .setClientId(clientId)
      .setMetrics(MetricGenerator.buildMetrics(rows.queryExecution.executedPlan))
      .build()
  }

  def handleCommand(session: SparkSession, request: Request): Unit = {
    val command = request.getPlan.getCommand
    val planner = new SparkConnectCommandPlanner(session, command)
    planner.process()
    responseObserver.onCompleted()
  }
}

object MetricGenerator extends AdaptiveSparkPlanHelper {
  def buildMetrics(p: SparkPlan): Response.Metrics = {
    val b = Response.Metrics.newBuilder
    b.addAllMetrics(transformPlan(p, p.id).asJava)
    b.build()
  }

  def transformChildren(p: SparkPlan): Seq[Response.Metrics.MetricObject] = {
    allChildren(p).flatMap(c => transformPlan(c, p.id))
  }

  def allChildren(p: SparkPlan): Seq[SparkPlan] = p match {
    case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
    case s: QueryStageExec => Seq(s.plan)
    case _ => p.children
  }

  def transformPlan(p: SparkPlan, parentId: Int): Seq[Response.Metrics.MetricObject] = {
    val mv = p.metrics.map(m =>
      m._1 -> Response.Metrics.MetricValue.newBuilder
        .setName(m._2.name.getOrElse(""))
        .setValue(m._2.value)
        .setMetricType(m._2.metricType)
        .build())
    val mo = Response.Metrics.MetricObject
      .newBuilder()
      .setName(p.nodeName)
      .setPlanId(p.id)
      .putAllExecutionMetrics(mv.asJava)
      .build()
    Seq(mo) ++ transformChildren(p)
  }

}
