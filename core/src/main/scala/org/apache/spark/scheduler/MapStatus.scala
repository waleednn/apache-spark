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

package org.apache.spark.scheduler

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.Utils

/**
 * Result returned by a ShuffleMapTask to a scheduler. Includes the block manager address that the
 * task ran on, the sizes of outputs for each reducer, and the number of records of the map task,
 * for passing on to the reduce tasks.
 */
private[spark] sealed trait MapStatus {
  /** Location where this task was run. */
  def location: BlockManagerId

  /**
   * Estimated size for the reduce block, in bytes.
   *
   * If a block is non-empty, then this method MUST return a non-zero size.  This invariant is
   * necessary for correctness, since block fetchers are allowed to skip zero-size blocks.
   */
  def getSizeForBlock(reduceId: Int): Long

  /**
   * The number of records for the map task. We use this information downstream to optimize
   * execution. For example, SPARK-19355 uses it to improve limit performance.
   */
  def numRecords: Long
}


private[spark] object MapStatus {

  def apply(loc: BlockManagerId, uncompressedSizes: Array[Long], numRecords: Long): MapStatus = {
    if (uncompressedSizes.length >  Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS))
      .getOrElse(config.SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS.defaultValue.get)) {
      HighlyCompressedMapStatus(loc, uncompressedSizes, numRecords)
    } else {
      new CompressedMapStatus(loc, uncompressedSizes, numRecords)
    }
  }

  private[this] val LOG_BASE = 1.1

  /**
   * Compress a size in bytes to 8 bits for efficient reporting of map output sizes.
   * We do this by encoding the log base 1.1 of the size as an integer, which can support
   * sizes up to 35 GB with at most 10% error.
   */
  def compressSize(size: Long): Byte = {
    if (size == 0) {
      0
    } else if (size <= 1L) {
      1
    } else {
      math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    }
  }

  /**
   * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
   */
  def decompressSize(compressedSize: Byte): Long = {
    if (compressedSize == 0) {
      0
    } else {
      math.pow(LOG_BASE, compressedSize & 0xFF).toLong
    }
  }
}


/**
 * A [[MapStatus]] implementation that tracks the size of each block. Size for each block is
 * represented using a single byte.
 *
 * @param _location location where the task is being executed.
 * @param _compressedSizes size of the blocks, indexed by reduce partition id.
 */
private[spark] class CompressedMapStatus(
    private[this] var _location: BlockManagerId,
    private[this] var _compressedSizes: Array[Byte],
    private[this] var _numRecords: Long)
  extends MapStatus with Externalizable {

  protected def this() = this(null, null.asInstanceOf[Array[Byte]], -1)  // For deserialization only

  def this(loc: BlockManagerId, uncompressedSizes: Array[Long], numRecords: Long) {
    this(loc, uncompressedSizes.map(MapStatus.compressSize), numRecords)
  }

  override def location: BlockManagerId = _location

  override def numRecords: Long = _numRecords

  override def getSizeForBlock(reduceId: Int): Long = {
    MapStatus.decompressSize(_compressedSizes(reduceId))
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    _location.writeExternal(out)
    out.writeLong(_numRecords)
    out.writeInt(_compressedSizes.length)
    out.write(_compressedSizes)
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    _location = BlockManagerId(in)
    _numRecords = in.readLong()
    val len = in.readInt()
    _compressedSizes = new Array[Byte](len)
    in.readFully(_compressedSizes)
  }
}

/**
 * A [[MapStatus]] implementation that stores the accurate size of huge blocks, which are larger
 * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
 * plus a bitmap for tracking which blocks are empty.
 *
 * @param _location location where the task is being executed
 * @param _numNonEmptyBlocks the number of non-empty blocks
 * @param _emptyBlocks a bitmap tracking which blocks are empty
 * @param _avgSize average size of the non-empty and non-huge blocks
 * @param _hugeBlockSizes sizes of huge blocks by their reduceId.
 */
private[spark] class HighlyCompressedMapStatus private (
    private[this] var _location: BlockManagerId,
    private[this] var _numNonEmptyBlocks: Int,
    private[this] var _emptyBlocks: RoaringBitmap,
    private[this] var _avgSize: Long,
    private[this] var _hugeBlockSizes: Map[Int, Byte],
    private[this] var _numRecords: Long)
  extends MapStatus with Externalizable {

  // _location could be null when the default constructor is called during deserialization
  require(_location == null || _avgSize > 0 || _hugeBlockSizes.size > 0 || _numNonEmptyBlocks == 0,
    "Average size can only be zero for map stages that produced no output")

  protected def this() = this(null, -1, null, -1, null, -1)  // For deserialization only

  override def location: BlockManagerId = _location

  override def numRecords: Long = _numRecords

  override def getSizeForBlock(reduceId: Int): Long = {
    assert(_hugeBlockSizes != null)
    if (_emptyBlocks.contains(reduceId)) {
      0
    } else {
      _hugeBlockSizes.get(reduceId) match {
        case Some(size) => MapStatus.decompressSize(size)
        case None => _avgSize
      }
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    _location.writeExternal(out)
    out.writeLong(_numRecords)
    _emptyBlocks.writeExternal(out)
    out.writeLong(_avgSize)
    out.writeInt(_hugeBlockSizes.size)
    _hugeBlockSizes.foreach { kv =>
      out.writeInt(kv._1)
      out.writeByte(kv._2)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    _location = BlockManagerId(in)
    _numRecords = in.readLong()
    _emptyBlocks = new RoaringBitmap()
    _emptyBlocks.readExternal(in)
    _avgSize = in.readLong()
    val count = in.readInt()
    val hugeBlockSizesArray = mutable.ArrayBuffer[Tuple2[Int, Byte]]()
    (0 until count).foreach { _ =>
      val block = in.readInt()
      val size = in.readByte()
      hugeBlockSizesArray += Tuple2(block, size)
    }
    _hugeBlockSizes = hugeBlockSizesArray.toMap
  }
}

private[spark] object HighlyCompressedMapStatus {
  def apply(
      loc: BlockManagerId,
      uncompressedSizes: Array[Long],
      numRecords: Long): HighlyCompressedMapStatus = {
    // We must keep track of which blocks are empty so that we don't report a zero-sized
    // block as being non-empty (or vice-versa) when using the average block size.
    var i = 0
    var numNonEmptyBlocks: Int = 0
    var numSmallBlocks: Int = 0
    var totalSmallBlockSize: Long = 0
    // From a compression standpoint, it shouldn't matter whether we track empty or non-empty
    // blocks. From a performance standpoint, we benefit from tracking empty blocks because
    // we expect that there will be far fewer of them, so we will perform fewer bitmap insertions.
    val emptyBlocks = new RoaringBitmap()
    val totalNumBlocks = uncompressedSizes.length
    val threshold = Option(SparkEnv.get)
      .map(_.conf.get(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD))
      .getOrElse(config.SHUFFLE_ACCURATE_BLOCK_THRESHOLD.defaultValue.get)
    val hugeBlockSizesArray = ArrayBuffer[Tuple2[Int, Byte]]()
    while (i < totalNumBlocks) {
      val size = uncompressedSizes(i)
      if (size > 0) {
        numNonEmptyBlocks += 1
        // Huge blocks are not included in the calculation for average size, thus size for smaller
        // blocks is more accurate.
        if (size < threshold) {
          totalSmallBlockSize += size
          numSmallBlocks += 1
        } else {
          hugeBlockSizesArray += Tuple2(i, MapStatus.compressSize(uncompressedSizes(i)))
        }
      } else {
        emptyBlocks.add(i)
      }
      i += 1
    }
    val avgSize = if (numSmallBlocks > 0) {
      totalSmallBlockSize / numSmallBlocks
    } else {
      0
    }
    emptyBlocks.trim()
    emptyBlocks.runOptimize()
    new HighlyCompressedMapStatus(loc, numNonEmptyBlocks, emptyBlocks, avgSize,
      hugeBlockSizesArray.toMap, numRecords)
  }
}
