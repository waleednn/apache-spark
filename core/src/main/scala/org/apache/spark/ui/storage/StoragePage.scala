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

import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.storage.RDDInfo
import org.apache.spark.ui.{WebUIPage, UIUtils}
import org.apache.spark.util.{JsonProtocol, Utils}

/** Page showing list of RDD's currently stored in the cluster */
private[ui] class StoragePage(parent: StorageTab) extends WebUIPage("") {
  private val listener = parent.listener

  override def renderJson(request: HttpServletRequest): JValue = {
    val rddJsonList =
      listener.rddInfoList.map { info => JsonProtocol.rddInfoToJson(info) }

    rddJsonList
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val rdds = listener.rddInfoList
    val content = UIUtils.listingTable(rddHeader, rddRow, rdds, id = Some("storage-by-rdd-table"))
    UIUtils.headerSparkPage("Storage", content, parent)
  }

  /** Header fields for the RDD table */
  private def rddHeader = Seq(
    "RDD Name",
    "Storage Level",
    "Cached Partitions",
    "Fraction Cached",
    "Size in Memory",
    "Size in Tachyon",
    "Size on Disk")

  /** Render an HTML row representing an RDD */
  private def rddRow(rdd: RDDInfo): Seq[Node] = {
    // scalastyle:off
    <tr>
      <td>
        <a href={"%s/storage/rdd?id=%s".format(UIUtils.prependBaseUri(parent.basePath), rdd.id)}>
          {rdd.name}
        </a>
      </td>
      <td>{rdd.storageLevel.description}
      </td>
      <td>{rdd.numCachedPartitions}</td>
      <td>{"%.0f%%".format(rdd.numCachedPartitions * 100.0 / rdd.numPartitions)}</td>
      <td sorttable_customkey={rdd.memSize.toString}>{Utils.bytesToString(rdd.memSize)}</td>
      <td sorttable_customkey={rdd.tachyonSize.toString}>{Utils.bytesToString(rdd.tachyonSize)}</td>
      <td sorttable_customkey={rdd.diskSize.toString} >{Utils.bytesToString(rdd.diskSize)}</td>
    </tr>
    // scalastyle:on
  }
}
