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

package org.apache.spark.storage

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

import akka.actor._

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.{Logging, SparkConf, SparkException}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.AkkaUtils

private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {

  val timeout = AkkaUtils.askTimeout(conf)

  /** Remove a dead executor from the driver actor. This is only called on the driver side. */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /** Register the BlockManager's id with the driver. */
  def registerBlockManager(blockManagerId: BlockManagerId, maxMemSize: Long, slaveActor: ActorRef) {
    logInfo("Trying to register BlockManager")
    tell(RegisterBlockManager(blockManagerId, maxMemSize, slaveActor))
    logInfo("Registered BlockManager")
  }

  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      tachyonSize: Long): Boolean = {
    val res = driverEndpoint.askWithReply[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize, tachyonSize))
    logDebug(s"Updated info of block $blockId")
    res
  }

  /** Get locations of the blockId from the driver */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    driverEndpoint.askWithReply[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations of multiple blockIds from the driver */
  def getLocations(blockIds: Array[BlockId]): Seq[Seq[BlockManagerId]] = {
    driverEndpoint.askWithReply[Seq[Seq[BlockManagerId]]](GetLocationsMultipleBlockIds(blockIds))
  }

  /**
   * Check if block manager master has a block. Note that this can be used to check for only
   * those blocks that are reported to block manager master.
   */
  def contains(blockId: BlockId): Boolean = {
    !getLocations(blockId).isEmpty
  }

  /** Get ids of other nodes in the cluster from the driver */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    driverEndpoint.askWithReply[Seq[BlockManagerId]](GetPeers(blockManagerId))
  }

  def getActorSystemHostPortForExecutor(executorId: String): Option[(String, Int)] = {
    driverEndpoint.askWithReply[Option[(String, Int)]](GetActorSystemHostPortForExecutor(executorId))
  }

  /**
   * Remove a block from the slaves that have it. This can only be used to remove
   * blocks that the driver knows about.
   */
  def removeBlock(blockId: BlockId) {
    driverEndpoint.askWithReply[Boolean](RemoveBlock(blockId))
  }

  /** Remove all blocks belonging to the given RDD. */
  def removeRdd(rddId: Int, blocking: Boolean) {
    val future = driverEndpoint.askWithReply[Future[Seq[Int]]](RemoveRdd(rddId))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove RDD $rddId - ${e.getMessage}}")
    }
    if (blocking) {
      Await.result(future, timeout)
    }
  }

  /** Remove all blocks belonging to the given shuffle. */
  def removeShuffle(shuffleId: Int, blocking: Boolean) {
    val future = driverEndpoint.askWithReply[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove shuffle $shuffleId - ${e.getMessage}}")
    }
    if (blocking) {
      Await.result(future, timeout)
    }
  }

  /** Remove all blocks belonging to the given broadcast. */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean) {
    val future = driverEndpoint.askWithReply[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.onFailure {
      case e: Exception =>
        logWarning(s"Failed to remove broadcast $broadcastId" +
          s" with removeFromMaster = $removeFromMaster - ${e.getMessage}}")
    }
    if (blocking) {
      Await.result(future, timeout)
    }
  }

  /**
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    driverEndpoint.askWithReply[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    driverEndpoint.askWithReply[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * Return the block's status on all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getBlockStatus(
      blockId: BlockId,
      askSlaves: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askSlaves)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master actor
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master actor for a response to a prior message.
     */
    val response = driverEndpoint.askWithReply[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    val result = Await.result(Future.sequence(futures), timeout)
    if (result == null) {
      throw new SparkException("BlockManager returned null for BlockStatus query: " + blockId)
    }
    val blockStatus = result.asInstanceOf[Iterable[Option[BlockStatus]]]
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * Return a list of ids of existing blocks such that the ids match the given filter. NOTE: This
   * is a potentially expensive operation and should only be used for testing.
   *
   * If askSlaves is true, this invokes the master to query each block manager for the most
   * updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askSlaves: Boolean): Seq[BlockId] = {
    val msg = GetMatchingBlockIds(filter, askSlaves)
    val future = driverEndpoint.askWithReply[Future[Seq[BlockId]]](msg)
    Await.result(future, timeout)
  }

  /** Stop the driver actor, called only on the Spark driver node */
  def stop() {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  /** Send a one-way message to the master actor, to which we expect it to reply with true. */
  private def tell(message: Any) {
    if (!driverEndpoint.askWithReply[Boolean](message)) {
      throw new SparkException("BlockManagerMasterActor returned false, expected true.")
    }
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_AKKA_ACTOR_NAME = "BlockManagerMaster"
}
