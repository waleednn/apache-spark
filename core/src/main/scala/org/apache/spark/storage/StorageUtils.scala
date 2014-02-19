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

import org.apache.spark.SparkContext
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.JsonSerializable

import net.liftweb.json.JsonDSL._
import net.liftweb.json.JsonAST._
import net.liftweb.json.DefaultFormats

import BlockManagerMasterActor.BlockStatus

private[spark]
case class StorageStatus(blockManagerId: BlockManagerId, maxMem: Long,
  blocks: Map[BlockId, BlockStatus]) extends JsonSerializable {

  def memUsed() = blocks.values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def memUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsed() = blocks.values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def memRemaining : Long = maxMem - memUsed()

  def rddBlocks = blocks.flatMap {
    case (rdd: RDDBlockId, status) => Some(rdd, status)
    case _ => None
  }

  override def toJson = {
    val blocksJson = JArray(
      blocks.toList.map { case (id, status) =>
        ("Block ID" -> id.toJson) ~
        ("Status" -> status.toJson)
      })
    ("Block Manager ID" -> blockManagerId.toJson) ~
    ("Maximum Memory" -> maxMem) ~
    ("Blocks" -> blocksJson)
  }
}

private[spark]
case object StorageStatus {
  def fromJson(json: JValue): StorageStatus = {
    implicit val format = DefaultFormats
    val blocks = (json \ "Blocks").extract[List[JValue]].map { block =>
      val id = BlockId.fromJson(block \ "Block ID")
      val status = BlockStatus.fromJson(block \ "Status")
      (id, status)
    }.toMap
    new StorageStatus(
      BlockManagerId.fromJson(json \ "Block Manager ID"),
      (json \ "Maximum Memory").extract[Long],
      blocks)
  }
}

case class RDDInfo(
    id: Int,
    name: String,
    storageLevel: StorageLevel,
    numCachedPartitions: Int,
    numPartitions: Int,
    memSize: Long,
    diskSize: Long)
  extends JsonSerializable with Ordered[RDDInfo] {
  override def toString = {
    ("RDD \"%s\" (%d) Storage: %s; CachedPartitions: %d; TotalPartitions: %d; MemorySize: %s; " +
       "DiskSize: %s").format(name, id, storageLevel.toString, numCachedPartitions,
         numPartitions, Utils.bytesToString(memSize), Utils.bytesToString(diskSize))
  }

  override def compare(that: RDDInfo) = {
    this.id - that.id
  }

  override def toJson = {
    ("RDD ID" -> id) ~
    ("Name" -> name) ~
    ("Storage Level" -> storageLevel.toJson) ~
    ("Number of Cached Partitions" -> numCachedPartitions) ~
    ("Number of Partitions" -> numPartitions) ~
    ("Memory Size" -> memSize) ~
    ("Disk Size" -> diskSize)
  }
}

case object RDDInfo {
  def fromJson(json: JValue): RDDInfo = {
    implicit val format = DefaultFormats
    new RDDInfo(
      (json \ "RDD ID").extract[Int],
      (json \ "Name").extract[String],
      StorageLevel.fromJson(json \ "Storage Level"),
      (json \ "Number of Cached Partitions").extract[Int],
      (json \ "Number of Partitions").extract[Int],
      (json \ "Memory Size").extract[Long],
      (json \ "Disk Size").extract[Long])
  }
}

/* Helper methods for storage-related objects */
private[spark]
object StorageUtils {

  /* Returns RDD-level information, compiled from a list of StorageStatus objects */
  def rddInfoFromStorageStatus(storageStatusList: Seq[StorageStatus],
    sc: SparkContext) : Array[RDDInfo] = {
    rddInfoFromBlockStatusList(
      storageStatusList.flatMap(_.rddBlocks).toMap[RDDBlockId, BlockStatus], sc)
  }

  /* Returns a map of blocks to their locations, compiled from a list of StorageStatus objects */
  def blockLocationsFromStorageStatus(storageStatusList: Seq[StorageStatus]) = {
    val blockLocationPairs = storageStatusList
      .flatMap(s => s.blocks.map(b => (b._1, s.blockManagerId.hostPort)))
    blockLocationPairs.groupBy(_._1).map{case (k, v) => (k, v.unzip._2)}.toMap
  }

  /* Given a list of BlockStatus objects, returns information for each RDD */
  def rddInfoFromBlockStatusList(infos: Map[RDDBlockId, BlockStatus],
    sc: SparkContext) : Array[RDDInfo] = {

    // Group by rddId, ignore the partition name
    val groupedRddBlocks = infos.groupBy { case (k, v) => k.rddId }.mapValues(_.values.toArray)

    // For each RDD, generate an RDDInfo object
    val rddInfos = groupedRddBlocks.map { case (rddId, rddBlocks) =>
      // Add up memory and disk sizes
      val memSize = rddBlocks.map(_.memSize).reduce(_ + _)
      val diskSize = rddBlocks.map(_.diskSize).reduce(_ + _)

      // Get the friendly name and storage level for the RDD, if available
      sc.persistentRdds.get(rddId).map { r =>
        val rddName = Option(r.name).getOrElse(rddId.toString)
        val rddStorageLevel = r.getStorageLevel
        RDDInfo(rddId, rddName, rddStorageLevel, rddBlocks.length, r.partitions.size,
          memSize, diskSize)
      }
    }.flatten.toArray

    scala.util.Sorting.quickSort(rddInfos)

    rddInfos
  }

  /* Filters storage status by a given RDD id. */
  def filterStorageStatusByRDD(storageStatusList: Array[StorageStatus], rddId: Int)
    : Array[StorageStatus] = {

    storageStatusList.map { status =>
      val newBlocks = status.rddBlocks.filterKeys(_.rddId == rddId).toMap[BlockId, BlockStatus]
      //val newRemainingMem = status.maxMem - newBlocks.values.map(_.memSize).reduce(_ + _)
      StorageStatus(status.blockManagerId, status.maxMem, newBlocks)
    }
  }
}
