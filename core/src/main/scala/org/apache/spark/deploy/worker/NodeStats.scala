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

/**
 * @author: Dennis J. McWherter, Jr. (dmcwherter@yahoo-inc.com)
 */

package org.apache.spark.deploy.worker

import scala.io.Source._

/**
 * Data type to hold all statistics
 *
 * @param cpuspeed    List of CPU speeds (in MHz)
 * @param diskspeed   List of disk speeds (in milliseconds per access)
 * @param maxMem      Max memory provided by JVM (in bytes)
 * @param availMem    Available memory (in bytes)
 */
case class Statistics(cpuspeed: List[Float], diskspeed: List[Float], maxMem: Long, availMem: Long)

/**
 * Node stats
 */
class NodeStats {
  /**
   * Retrieve all statistics about the node
   *
   * @return  A statistics data type with current node information
   */
  def getAllStats: Statistics = {
    val cpu = getCPUInfo
    val disks = getDiskInfo
    val (maxMem, availMem) = getMemInfo
    new Statistics(cpu, disks, maxMem, availMem)
  }

  /**
   * Memory information
   *
   * @return  Tuple of (maxMem, availMem)
   */
  def getMemInfo: (Long,Long) = {
    val rt = Runtime.getRuntime
    (rt.maxMemory, rt.freeMemory)
  }

  /**
   * Disk information as reported by /proc/diskstats
   *
   * @return  List of milliseconds/access values. Each entry corresponds to an active disk.
   */
  def getDiskInfo: List[Float] = {
    // diskstats documentation:
    // https://www.kernel.org/doc/Documentation/ABI/testing/procfs-diskstats
    val src   = fromFile("/proc/diskstats")
    val lines = src.mkString.split("\n")
    src.close()
    lines.map(_.trim.split("\\s+")).filter {
      // Heuristic to determine active disk(s)
      e => "0".equals(e(1)) && !"0".equals(e(6)) && !"0".equals(e(10))
    }.map {
      e =>
        val completedReads = e(3).toFloat
        val completedWrites = e(7).toFloat
        val completedReqs = completedReads + completedWrites
        val ioTime = e(12).toFloat
        // (Requests * ms) / Requests = ms
        ioTime / completedReqs
    }.toList
  }

  /**
   * CPU information as reported by /proc/cpuinfo
   *
   * @return  List of CPU speeds in MHz. One entry per core
   */
  def getCPUInfo: List[Float] = {
    val src   = fromFile("/proc/cpuinfo")
    val lines = src.mkString.split("\n")
    src.close
    lines.filter(_.startsWith("cpu MHz")).map(_.split(":")(1).trim.toFloat).toList
  }
}

// NOTE: This is just here for an example usage. Should remove.
object StatsExample {
  def main(args:Array[String]) {
    val x = new NodeStats
    val s = x.getAllStats
    println("Linux box statistics (based on procfs and JVM runtime)")
    println("======================================================")
    println("CPU speeds: " + (s.cpuspeed mkString " MHz,") + " MHz")
    println("Disk speeds: " + (s.diskspeed mkString " ms/access, ") + " ms/access")
    println("Max memory: " + s.maxMem + " B")
    println("Available memory: " + s.availMem + " B")
  }
}
