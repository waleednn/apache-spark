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

package org.apache.spark.ui.storage

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.storage.{BlockId, StorageStatus, StorageUtils}
import org.apache.spark.storage.BlockManagerMasterActor.BlockStatus
import org.apache.spark.ui.UIUtils._
import org.apache.spark.ui.Page._
import org.apache.spark.util.Utils


/** Page showing storage details for a given RDD */
private[spark] class RDDPage(parent: BlockManagerUI, fromDisk: Boolean = false) {
  private val sc = parent.sc
  private def listener = parent.listener

  def render(request: HttpServletRequest): Seq[Node] = {
    if (!fromDisk) {
      listener.fetchStorageStatus()
    }
    val storageStatusList = listener.storageStatusList
    val id = request.getParameter("id").toInt
    val filteredStorageStatusList =
      StorageUtils.filterStorageStatusByRDD(storageStatusList.toArray, id)
    val rddInfo = StorageUtils.rddInfoFromStorageStatus(filteredStorageStatusList, sc).head

    // Worker table
    val workers = filteredStorageStatusList.map((id, _))
    val workerTable = listingTable(workerHeader, workerRow, workers)

    // Block table
    val blockStatuses = filteredStorageStatusList.flatMap(_.blocks).toArray.
      sortWith(_._1.name < _._1.name)
    val blockLocations = StorageUtils.blockLocationsFromStorageStatus(filteredStorageStatusList)
    val blocks = blockStatuses.map {
      case(id, status) => (id, status, blockLocations.get(id).getOrElse(Seq("Unknown")))
    }
    val blockTable = listingTable(blockHeader, blockRow, blocks)

    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            <li>
              <strong>Storage Level:</strong>
              {rddInfo.storageLevel.description}
            </li>
            <li>
              <strong>Cached Partitions:</strong>
              {rddInfo.numCachedPartitions}
            </li>
            <li>
              <strong>Total Partitions:</strong>
              {rddInfo.numPartitions}
            </li>
            <li>
              <strong>Memory Size:</strong>
              {Utils.bytesToString(rddInfo.memSize)}
            </li>
            <li>
              <strong>Disk Size:</strong>
              {Utils.bytesToString(rddInfo.diskSize)}
            </li>
          </ul>
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> Data Distribution on {workers.size} Executors </h4>
          {workerTable}
        </div>
      </div>

      <div class="row-fluid">
        <div class="span12">
          <h4> {blocks.size} Partitions </h4>
          {blockTable}
        </div>
      </div>;

    headerSparkPage(content, sc, "RDD Storage Info for " + rddInfo.name, Storage)
  }

  /** Header fields for the worker table */
  private def workerHeader = Seq(
    "Host",
    "Memory Usage",
    "Disk Usage")

  /** Header fields for the block table */
  private def blockHeader = Seq(
    "Block Name",
    "Storage Level",
    "Size in Memory",
    "Size on Disk",
    "Executors")

  /** Render an HTML row representing a worker */
  private def workerRow(worker: (Int, StorageStatus)): Seq[Node] = {
    val (rddId, status) = worker
    <tr>
      <td>{status.blockManagerId.host + ":" + status.blockManagerId.port}</td>
      <td>
        {Utils.bytesToString(status.memUsedByRDD(rddId))}
        ({Utils.bytesToString(status.memRemaining)} Remaining)
      </td>
      <td>{Utils.bytesToString(status.diskUsedByRDD(rddId))}</td>
    </tr>
  }

  /** Render an HTML row representing a block */
  private def blockRow(row: (BlockId, BlockStatus, Seq[String])): Seq[Node] = {
    val (id, block, locations) = row
    <tr>
      <td>{id}</td>
      <td>
        {block.storageLevel.description}
      </td>
      <td sorttable_customkey={block.memSize.toString}>
        {Utils.bytesToString(block.memSize)}
      </td>
      <td sorttable_customkey={block.diskSize.toString}>
        {Utils.bytesToString(block.diskSize)}
      </td>
      <td>
        {locations.map(l => <span>{l}<br/></span>)}
      </td>
    </tr>
  }
}
