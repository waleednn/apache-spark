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

package org.apache.spark.deploy.worker

import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.SendHeartbeat
import org.apache.spark.rpc._

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(override val rpcEnv: RpcEnv, workerUrl: String)
  extends NetworkRpcEndpoint with Logging {

  override def onStart() {
    logInfo(s"Connecting to worker $workerUrl")
    if (!isTesting) {
      val worker = rpcEnv.setupEndpointRefByUrl(workerUrl)
      worker.send(SendHeartbeat) // need to send a message here to initiate connection
    }
  }

  // Used to avoid shutting down JVM during tests
  // In the normal case, exitNonZero will call `System.exit(-1)` to shutdown the JVM. In the unit
  // test, the user should call `setTesting(true)` so that `exitNonZero` will set `isShutDown` to
  // true rather than calling `System.exit`. The user can check `isShutDown` to know if
  // `exitNonZero` is called.
  private[deploy] var isShutDown = false
  private[deploy] def setTesting(testing: Boolean) = isTesting = testing
  private var isTesting = false

  // Lets us filter events only from the worker's actor system
  private val expectedHostPort = new java.net.URI(workerUrl)
  private def isWorker(address: RpcAddress) = {
    expectedHostPort.getHost == address.host && expectedHostPort.getPort == address.port
  }

  def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receive(sender: RpcEndpointRef) = {
    case e => logWarning(s"Received unexpected actor system event: $e")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      logInfo(s"Successfully connected to $workerUrl")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // This log message will never be seen
      logError(s"Lost connection to worker actor $workerUrl. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isWorker(remoteAddress)) {
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()
    }
  }
}
