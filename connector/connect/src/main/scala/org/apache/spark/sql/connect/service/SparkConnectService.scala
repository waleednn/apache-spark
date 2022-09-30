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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder
import io.grpc.{Server, Status}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.stub.StreamObserver

import org.apache.spark.SparkEnv
import org.apache.spark.annotation.{Since, Unstable}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.{AnalyzeResponse, Request, Response, SparkConnectServiceGrpc}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.connect.config.Connect.CONNECT_GRPC_BINDING_PORT
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.execution.ExtendedMode

/**
 * The SparkConnectService implementation.
 *
 * This class implements the service stub from the generated code of GRPC.
 *
 * @param debug
 *   delegates debug behavior to the handlers.
 */
@Unstable
@Since("3.4.0")
class SparkConnectService(debug: Boolean)
  extends SparkConnectServiceGrpc.SparkConnectServiceImplBase with Logging {

  /**
   * This is the main entry method for Spark Connect and all calls to execute a plan.
   *
   * The plan execution is delegated to the [[SparkConnectStreamHandler]]. All error handling
   * should be directly implemented in the deferred implementation. But this method catches
   * generic errors.
   *
   * @param request
   * @param responseObserver
   */
  override def executePlan(request: Request, responseObserver: StreamObserver[Response]): Unit = {
    try {
      new SparkConnectStreamHandler(responseObserver).handle(request)
    } catch {
      case e: Throwable =>
        log.error("Error executing plan.", e)
        responseObserver.onError(
          Status.UNKNOWN.withCause(e).withDescription(e.getLocalizedMessage).asRuntimeException())
    }
  }

  /**
   * Analyze a plan to provide metadata and debugging information.
   *
   * This method is called to generate the explain plan for a SparkConnect plan. In its simplest
   * implementation, the plan that is generated by the [[SparkConnectPlanner]] is used to build a
   * [[Dataset]] and derive the explain string from the query execution details.
   *
   * Errors during planning are returned via the [[StreamObserver]] interface.
   *
   * @param request
   * @param responseObserver
   */
  override def analyzePlan(
      request: Request,
      responseObserver: StreamObserver[AnalyzeResponse]): Unit = {
    try {
      val session =
        SparkConnectService.getOrCreateIsolatedSession(request.getUserContext.getUserId).session

      val logicalPlan = request.getPlan.getOpTypeCase match {
        case proto.Plan.OpTypeCase.ROOT =>
          new SparkConnectPlanner(request.getPlan.getRoot, session).transform()
        case _ =>
          responseObserver.onError(
            new UnsupportedOperationException(
              s"${request.getPlan.getOpTypeCase} not supported for analysis."))
          return
      }
      val ds = Dataset.ofRows(session, logicalPlan)
      val explainString = ds.queryExecution.explainString(ExtendedMode)

      val resp = proto.AnalyzeResponse
        .newBuilder()
        .setExplainString(explainString)
        .setClientId(request.getClientId)

      resp.addAllColumnTypes(ds.schema.fields.map(_.dataType.sql).toSeq.asJava)
      resp.addAllColumnNames(ds.schema.fields.map(_.name).toSeq.asJava)
      responseObserver.onNext(resp.build())
      responseObserver.onCompleted()
    } catch {
      case e: Throwable =>
        log.error("Error analyzing plan.", e)
        responseObserver.onError(
          Status.UNKNOWN.withCause(e).withDescription(e.getLocalizedMessage).asRuntimeException())
    }
  }
}

/**
 * Object used for referring to SparkSessions in the SessionCache.
 *
 * @param userId
 * @param session
 */
@Unstable
@Since("3.4.0")
private[connect] case class SessionHolder(userId: String, session: SparkSession)

/**
 * Static instance of the SparkConnectService.
 *
 * Used to start the overall SparkConnect service and provides global state to manage the
 * different SparkSession from different users connecting to the cluster.
 */
@Unstable
@Since("3.4.0")
object SparkConnectService {

  private val CACHE_SIZE = 100

  private val CACHE_TIMEOUT_SECONDS = 3600

  // Type alias for the SessionCacheKey. Right now this is a String but allows us to switch to a
  // different or complex type easily.
  private type SessionCacheKey = String;

  private var server: Server = _

  private val userSessionMapping =
    cacheBuilder(CACHE_SIZE, CACHE_TIMEOUT_SECONDS).build[SessionCacheKey, SessionHolder]()

  // Simple builder for creating the cache of Sessions.
  private def cacheBuilder(cacheSize: Int, timeoutSeconds: Int): CacheBuilder[Object, Object] = {
    var cacheBuilder = CacheBuilder.newBuilder().ticker(Ticker.systemTicker())
    if (cacheSize >= 0) {
      cacheBuilder = cacheBuilder.maximumSize(cacheSize)
    }
    if (timeoutSeconds >= 0) {
      cacheBuilder.expireAfterAccess(timeoutSeconds, TimeUnit.SECONDS)
    }
    cacheBuilder
  }

  /**
   * Based on the `key` find or create a new SparkSession.
   */
  private[connect] def getOrCreateIsolatedSession(key: SessionCacheKey): SessionHolder = {
    userSessionMapping.get(
      key,
      () => {
        SessionHolder(key, newIsolatedSession())
      })
  }

  private def newIsolatedSession(): SparkSession = {
    SparkSession.active.newSession()
  }

  /**
   * Starts the GRPC Serivce.
   *
   */
  def startGRPCService(): Unit = {
    val debugMode = SparkEnv.get.conf.getBoolean("spark.connect.grpc.debug.enabled", true)
    val port = SparkEnv.get.conf.get(CONNECT_GRPC_BINDING_PORT)
    val sb = NettyServerBuilder
      .forPort(port)
      .addService(new SparkConnectService(debugMode))

    // If debug mode is configured, load the ProtoReflection service so that tools like
    // grpcurl can introspect the API for debugging.
    if (debugMode) {
      sb.addService(ProtoReflectionService.newInstance())
    }
    server = sb.build
    server.start()
  }

  // Starts the service
  def start(): Unit = {
    startGRPCService()
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdownNow()
    }
  }
}

