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

package org.apache.spark.shuffle.sort

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark._
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle._

/**
 * Subclass of [[BaseShuffleHandle]], used to identify when we've chosen to use the
 * serialized shuffle.
 */
private[spark] class SerializedShuffleHandle[K, V](
    shuffleId: Int,
    numMaps: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, numMaps, dependency) {
}

private[spark] object SortShuffleManager extends Logging {

  /**
   * The maximum number of shuffle output partitions that SortShuffleManager supports when
   *
   */
  val MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE =
    PackedRecordPointer.MAXIMUM_PARTITION_ID + 1

  /**
   * Helper method for determining whether a shuffle should use an optimized serialized shuffle
   * path or whether it should fall back to the original path that operates on deserialized objects.
   */
  def canUseSerializedShuffle(dependency: ShuffleDependency[_, _, _]): Boolean = {
    val shufId = dependency.shuffleId
    val numPartitions = dependency.partitioner.numPartitions
    val serializer = Serializer.getSerializer(dependency.serializer)
    if (!serializer.supportsRelocationOfSerializedObjects) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because the serializer, " +
        s"${serializer.getClass.getName}, does not support object relocation")
      false
    } else if (dependency.aggregator.isDefined) {
      log.debug(
        s"Can't use serialized shuffle for shuffle $shufId because an aggregator is defined")
      false
    } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
      log.debug(s"Can't use serialized shuffle for shuffle $shufId because it has more than " +
        s"$MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE partitions")
      false
    } else {
      log.debug(s"Can use serialized shuffle for shuffle $shufId")
      true
    }
  }
}

/**
 * A shuffle implementation that uses directly-managed memory to implement several performance
 * optimizations for certain types of shuffles. In cases where the new performance optimizations
 * cannot be applied, this shuffle manager delegates to [[SortShuffleManager]] to handle those
 * shuffles.
 *
 * UnsafeShuffleManager's optimizations will apply when _all_ of the following conditions hold:
 *
 *  - The shuffle dependency specifies no aggregation or output ordering.
 *  - The shuffle serializer supports relocation of serialized values (this is currently supported
 *    by KryoSerializer and Spark SQL's custom serializers).
 *  - The shuffle produces fewer than 16777216 output partitions.
 *
 * In addition, extra spill-merging optimizations are automatically applied when the shuffle
 * compression codec supports concatenation of serialized streams. This is currently supported by
 * Spark's LZF serializer.
 *
 * At a high-level, UnsafeShuffleManager's design is similar to Spark's existing SortShuffleManager.
 * In sort-based shuffle, incoming records are sorted according to their target partition ids, then
 * written to a single map output file. Reducers fetch contiguous regions of this file in order to
 * read their portion of the map output. In cases where the map output data is too large to fit in
 * memory, sorted subsets of the output can are spilled to disk and those on-disk files are merged
 * to produce the final output file.
 *
 * UnsafeShuffleManager optimizes this process in several ways:
 *
 *  - Its sort operates on serialized binary data rather than Java objects, which reduces memory
 *    consumption and GC overheads. This optimization requires the record serializer to have certain
 *    properties to allow serialized records to be re-ordered without requiring deserialization.
 *    See SPARK-4550, where this optimization was first proposed and implemented, for more details.
 *
 *  - It uses a specialized cache-efficient sorter ([[ShuffleExternalSorter]]) that sorts
 *    arrays of compressed record pointers and partition ids. By using only 8 bytes of space per
 *    record in the sorting array, this fits more of the array into cache.
 *
 *  - The spill merging procedure operates on blocks of serialized records that belong to the same
 *    partition and does not need to deserialize records during the merge.
 *
 *  - When the spill compression codec supports concatenation of compressed data, the spill merge
 *    simply concatenates the serialized and compressed spill partitions to produce the final output
 *    partition.  This allows efficient data copying methods, like NIO's `transferTo`, to be used
 *    and avoids the need to allocate decompression or copying buffers during the merge.
 *
 * For more details on UnsafeShuffleManager's design, see SPARK-7081.
 */
private[spark] class SortShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  if (!conf.getBoolean("spark.shuffle.spill", true)) {
    logWarning(
      "spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
        " Shuffle will continue to spill to disk when necessary.")
  }

  private[this] val numMapsForShuffle = new ConcurrentHashMap[Int, Int]()
  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /**
   * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      numMaps: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (SortShuffleManager.canUseSerializedShuffle(dependency)) {
      new SerializedShuffleHandle[K, V](
        shuffleId, numMaps, dependency.asInstanceOf[ShuffleDependency[K, V, V]])
    } else {
      new BaseShuffleHandle(shuffleId, numMaps, dependency)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext): ShuffleReader[K, C] = {
    new BlockStoreShuffleReader(
      handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Int,
      context: TaskContext): ShuffleWriter[K, V] = {
    numMapsForShuffle.putIfAbsent(
      handle.shuffleId, handle.asInstanceOf[BaseShuffleHandle[_, _, _]].numMaps)
    handle match {
      case unsafeShuffleHandle: SerializedShuffleHandle[K @unchecked, V @unchecked] =>
        val env = SparkEnv.get
        new UnsafeShuffleWriter(
          env.blockManager,
          shuffleBlockResolver.asInstanceOf[IndexShuffleBlockResolver],
          context.taskMemoryManager(),
          env.shuffleMemoryManager,
          unsafeShuffleHandle,
          mapId,
          context,
          env.conf)
      case other: BaseShuffleHandle[K @unchecked, V @unchecked, _] =>
        new SortShuffleWriter(shuffleBlockResolver, other, mapId, context)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    Option(numMapsForShuffle.remove(shuffleId)).foreach { numMaps =>
      (0 until numMaps).foreach { mapId =>
        shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
      }
    }
    true
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    shuffleBlockResolver.stop()
  }
}
