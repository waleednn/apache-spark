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

package org.apache.spark.internal

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.metrics.GarbageCollectionMetrics
import org.apache.spark.network.shuffle.Constants
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.{EventLoggingListener, SchedulingMode}
import org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO
import org.apache.spark.storage.{DefaultTopologyMapper, RandomBlockReplicationPolicy}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader.MAX_BUFFER_SIZE_BYTES

package object config {

  private[spark] val SPARK_DRIVER_PREFIX = "spark.driver"
  private[spark] val SPARK_EXECUTOR_PREFIX = "spark.executor"
  private[spark] val SPARK_TASK_PREFIX = "spark.task"
  private[spark] val LISTENER_BUS_EVENT_QUEUE_PREFIX = "spark.scheduler.listenerbus.eventqueue"

  private[spark] val RESOURCES_DISCOVERY_PLUGIN =
    ConfigBuilder("spark.resources.discoveryPlugin")
      .doc("Comma-separated list of class names implementing" +
        "org.apache.spark.api.resource.ResourceDiscoveryPlugin to load into the application." +
        "This is for advanced users to replace the resource discovery class with a " +
        "custom implementation. Spark will try each class specified until one of them " +
        "returns the resource information for that resource. It tries the discovery " +
        "script last if none of the plugins return information for that resource.")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  private[spark] val DRIVER_RESOURCES_FILE =
    ConfigBuilder("spark.driver.resourcesFile")
      .internal()
      .doc("Path to a file containing the resources allocated to the driver. " +
        "The file should be formatted as a JSON array of ResourceAllocation objects. " +
        "Only used internally in standalone mode.")
      .version("3.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_CLASS_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)
      .withPrepended(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.driver.userClassPathFirst")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DRIVER_CORES = ConfigBuilder("spark.driver.cores")
    .doc("Number of cores to use for the driver process, only in cluster mode.")
    .version("1.3.0")
    .intConf
    .createWithDefault(1)

  private[spark] val DRIVER_MEMORY = ConfigBuilder(SparkLauncher.DRIVER_MEMORY)
    .doc("Amount of memory to use for the driver process, in MiB unless otherwise specified.")
    .version("1.1.1")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val DRIVER_MEMORY_OVERHEAD = ConfigBuilder("spark.driver.memoryOverhead")
    .doc("The amount of non-heap memory to be allocated per driver in cluster mode, " +
      "in MiB unless otherwise specified.")
    .version("2.3.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val DRIVER_LOG_DFS_DIR =
    ConfigBuilder("spark.driver.log.dfsDir").version("3.0.0").stringConf.createOptional

  private[spark] val DRIVER_LOG_LAYOUT =
    ConfigBuilder("spark.driver.log.layout")
      .version("3.0.0")
      .stringConf
      .createOptional

  private[spark] val DRIVER_LOG_PERSISTTODFS =
    ConfigBuilder("spark.driver.log.persistToDfs.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DRIVER_LOG_ALLOW_EC =
    ConfigBuilder("spark.driver.log.allowErasureCoding")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_ENABLED = ConfigBuilder("spark.eventLog.enabled")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val EVENT_LOG_DIR = ConfigBuilder("spark.eventLog.dir")
    .version("1.0.0")
    .stringConf
    .createWithDefault(EventLoggingListener.DEFAULT_LOG_DIR)

  private[spark] val EVENT_LOG_COMPRESS =
    ConfigBuilder("spark.eventLog.compress")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_BLOCK_UPDATES =
    ConfigBuilder("spark.eventLog.logBlockUpdates.enabled")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_ALLOW_EC =
    ConfigBuilder("spark.eventLog.erasureCoding.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_TESTING =
    ConfigBuilder("spark.eventLog.testing")
      .internal()
      .version("1.0.1")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_OUTPUT_BUFFER_SIZE = ConfigBuilder("spark.eventLog.buffer.kb")
    .doc("Buffer size to use when writing to output streams, in KiB unless otherwise specified.")
    .version("1.0.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("100k")

  private[spark] val EVENT_LOG_STAGE_EXECUTOR_METRICS =
    ConfigBuilder("spark.eventLog.logStageExecutorMetrics")
      .doc("Whether to write per-stage peaks of executor metrics (for each executor) " +
        "to the event log.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_GC_METRICS_YOUNG_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors")
      .doc("Names of supported young generation garbage collector. A name usually is " +
        " the return of GarbageCollectorMXBean.getName. The built-in young generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.YOUNG_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  private[spark] val EVENT_LOG_GC_METRICS_OLD_GENERATION_GARBAGE_COLLECTORS =
    ConfigBuilder("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors")
      .doc("Names of supported old generation garbage collector. A name usually is " +
        "the return of GarbageCollectorMXBean.getName. The built-in old generation garbage " +
        s"collectors are ${GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS}")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(GarbageCollectionMetrics.OLD_GENERATION_BUILTIN_GARBAGE_COLLECTORS)

  private[spark] val EVENT_LOG_OVERWRITE =
    ConfigBuilder("spark.eventLog.overwrite")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_CALLSITE_LONG_FORM =
    ConfigBuilder("spark.eventLog.longForm.enabled")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_ENABLE_ROLLING =
    ConfigBuilder("spark.eventLog.rolling.enabled")
      .doc("Whether rolling over event log files is enabled. If set to true, it cuts down " +
        "each event log file to the configured size.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EVENT_LOG_ROLLING_MAX_FILE_SIZE =
    ConfigBuilder("spark.eventLog.rolling.maxFileSize")
      .doc(s"When ${EVENT_LOG_ENABLE_ROLLING.key}=true, specifies the max size of event log file" +
        " to be rolled over.")
      .version("3.0.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ >= ByteUnit.MiB.toBytes(10), "Max file size of event log should be " +
        "configured to be at least 10 MiB.")
      .createWithDefaultString("128m")

  private[spark] val EXECUTOR_ID =
    ConfigBuilder("spark.executor.id").version("1.2.0").stringConf.createOptional

  private[spark] val EXECUTOR_CLASS_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_HEARTBEAT_DROP_ZERO_ACCUMULATOR_UPDATES =
    ConfigBuilder("spark.executor.heartbeat.dropZeroAccumulatorUpdates")
      .internal()
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val EXECUTOR_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.executor.heartbeatInterval")
      .version("1.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  private[spark] val EXECUTOR_HEARTBEAT_MAX_FAILURES =
    ConfigBuilder("spark.executor.heartbeat.maxFailures")
      .internal()
      .version("1.6.2")
      .intConf
      .createWithDefault(60)

  private[spark] val EXECUTOR_PROCESS_TREE_METRICS_ENABLED =
    ConfigBuilder("spark.executor.processTreeMetrics.enabled")
      .doc("Whether to collect process tree metrics (from the /proc filesystem) when collecting " +
        "executor metrics.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_METRICS_POLLING_INTERVAL =
    ConfigBuilder("spark.executor.metrics.pollingInterval")
      .doc("How often to collect executor metrics (in milliseconds). " +
        "If 0, the polling is done on executor heartbeats. " +
        "If positive, the polling is done at this interval.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("0")

  private[spark] val EXECUTOR_METRICS_FILESYSTEM_SCHEMES =
    ConfigBuilder("spark.executor.metrics.fileSystemSchemes")
      .doc("The file system schemes to report in executor metrics.")
      .version("3.1.0")
      .stringConf
      .createWithDefaultString("file,hdfs")

  private[spark] val EXECUTOR_JAVA_OPTIONS =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS)
      .withPrepended(SparkLauncher.EXECUTOR_DEFAULT_JAVA_OPTIONS)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_LIBRARY_PATH =
    ConfigBuilder(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH)
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val EXECUTOR_USER_CLASS_PATH_FIRST =
    ConfigBuilder("spark.executor.userClassPathFirst")
      .version("1.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_CORES = ConfigBuilder(SparkLauncher.EXECUTOR_CORES)
    .version("1.0.0")
    .intConf
    .createWithDefault(1)

  private[spark] val EXECUTOR_MEMORY = ConfigBuilder(SparkLauncher.EXECUTOR_MEMORY)
    .doc("Amount of memory to use per executor process, in MiB unless otherwise specified.")
    .version("0.7.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("1g")

  private[spark] val EXECUTOR_MEMORY_OVERHEAD = ConfigBuilder("spark.executor.memoryOverhead")
    .doc("The amount of non-heap memory to be allocated per executor, in MiB unless otherwise" +
      " specified.")
    .version("2.3.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val CORES_MAX = ConfigBuilder("spark.cores.max")
    .doc("When running on a standalone deploy cluster or a Mesos cluster in coarse-grained " +
      "sharing mode, the maximum amount of CPU cores to request for the application from across " +
      "the cluster (not from each machine). If not set, the default will be " +
      "`spark.deploy.defaultCores` on Spark's standalone cluster manager, or infinite " +
      "(all available cores) on Mesos.")
    .version("0.6.0")
    .intConf
    .createOptional

  private[spark] val MEMORY_OFFHEAP_ENABLED = ConfigBuilder("spark.memory.offHeap.enabled")
    .doc("If true, Spark will attempt to use off-heap memory for certain operations. " +
      "If off-heap memory use is enabled, then spark.memory.offHeap.size must be positive.")
    .version("1.6.0")
    .withAlternative("spark.unsafe.offHeap")
    .booleanConf
    .createWithDefault(false)

  private[spark] val MEMORY_OFFHEAP_SIZE = ConfigBuilder("spark.memory.offHeap.size")
    .doc("The absolute amount of memory which can be used for off-heap allocation, " +
      " in bytes unless otherwise specified. " +
      "This setting has no impact on heap memory usage, so if your executors' total memory " +
      "consumption must fit within some hard limit then be sure to shrink your JVM heap size " +
      "accordingly. This must be set to a positive value when spark.memory.offHeap.enabled=true.")
    .version("1.6.0")
    .bytesConf(ByteUnit.BYTE)
    .checkValue(_ >= 0, "The off-heap memory size must not be negative")
    .createWithDefault(0)

  private[spark] val MEMORY_STORAGE_FRACTION = ConfigBuilder("spark.memory.storageFraction")
    .doc("Amount of storage memory immune to eviction, expressed as a fraction of the " +
      "size of the region set aside by spark.memory.fraction. The higher this is, the " +
      "less working memory may be available to execution and tasks may spill to disk more " +
      "often. Leaving this at the default value is recommended. ")
    .version("1.6.0")
    .doubleConf
    .checkValue(v => v >= 0.0 && v < 1.0, "Storage fraction must be in [0,1)")
    .createWithDefault(0.5)

  private[spark] val MEMORY_FRACTION = ConfigBuilder("spark.memory.fraction")
    .doc("Fraction of (heap space - 300MB) used for execution and storage. The " +
      "lower this is, the more frequently spills and cached data eviction occur. " +
      "The purpose of this config is to set aside memory for internal metadata, " +
      "user data structures, and imprecise size estimation in the case of sparse, " +
      "unusually large records. Leaving this at the default value is recommended.  ")
    .version("1.6.0")
    .doubleConf
    .createWithDefault(0.6)

  private[spark] val STORAGE_SAFETY_FRACTION = ConfigBuilder("spark.storage.safetyFraction")
    .version("1.1.0")
    .doubleConf
    .createWithDefault(0.9)

  private[spark] val STORAGE_UNROLL_MEMORY_THRESHOLD =
    ConfigBuilder("spark.storage.unrollMemoryThreshold")
      .doc("Initial memory to request before unrolling any block")
      .version("1.1.0")
      .longConf
      .createWithDefault(1024 * 1024)

  private[spark] val STORAGE_REPLICATION_PROACTIVE =
    ConfigBuilder("spark.storage.replication.proactive")
      .doc("Enables proactive block replication for RDD blocks. " +
        "Cached RDD block replicas lost due to executor failures are replenished " +
        "if there are any existing available replicas. This tries to " +
        "get the replication level of the block to the initial number")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STORAGE_MEMORY_MAP_THRESHOLD =
    ConfigBuilder("spark.storage.memoryMapThreshold")
      .doc("Size in bytes of a block above which Spark memory maps when " +
        "reading a block from disk. " +
        "This prevents Spark from memory mapping very small blocks. " +
        "In general, memory mapping has high overhead for blocks close to or below " +
        "the page size of the operating system.")
      .version("0.9.2")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("2m")

  private[spark] val STORAGE_REPLICATION_POLICY =
    ConfigBuilder("spark.storage.replication.policy")
      .version("2.1.0")
      .stringConf
      .createWithDefaultString(classOf[RandomBlockReplicationPolicy].getName)

  private[spark] val STORAGE_REPLICATION_TOPOLOGY_MAPPER =
    ConfigBuilder("spark.storage.replication.topologyMapper")
      .version("2.1.0")
      .stringConf
      .createWithDefaultString(classOf[DefaultTopologyMapper].getName)

  private[spark] val STORAGE_CACHED_PEERS_TTL = ConfigBuilder("spark.storage.cachedPeersTtl")
    .version("1.1.1")
    .intConf
    .createWithDefault(60 * 1000)

  private[spark] val STORAGE_MAX_REPLICATION_FAILURE =
    ConfigBuilder("spark.storage.maxReplicationFailures")
      .version("1.1.1")
      .intConf
      .createWithDefault(1)

  private[spark] val STORAGE_DECOMMISSION_ENABLED =
    ConfigBuilder("spark.storage.decommission.enabled")
      .doc("Whether to decommission the block manager when decommissioning executor")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED =
    ConfigBuilder("spark.storage.decommission.shuffleBlocks.enabled")
      .doc("Whether to transfer shuffle blocks during block manager decommissioning. Requires " +
        "a migratable shuffle resolver (like sort based shuffle)")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS =
    ConfigBuilder("spark.storage.decommission.shuffleBlocks.maxThreads")
      .doc("Maximum number of threads to use in migrating shuffle files.")
      .version("3.1.0")
      .intConf
      .checkValue(_ > 0, "The maximum number of threads should be positive")
      .createWithDefault(8)

  private[spark] val STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED =
    ConfigBuilder("spark.storage.decommission.rddBlocks.enabled")
      .doc("Whether to transfer RDD blocks during block manager decommissioning.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK =
    ConfigBuilder("spark.storage.decommission.maxReplicationFailuresPerBlock")
      .internal()
      .doc("Maximum number of failures which can be handled for the replication of " +
        "one RDD block when block manager is decommissioning and trying to move its " +
        "existing blocks.")
      .version("3.1.0")
      .intConf
      .createWithDefault(3)

  private[spark] val STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL =
    ConfigBuilder("spark.storage.decommission.replicationReattemptInterval")
      .internal()
      .doc("The interval of time between consecutive cache block replication reattempts " +
        "happening on each decommissioning executor (due to storage decommissioning).")
      .version("3.1.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ > 0, "Time interval between two consecutive attempts of " +
        "cache block replication should be positive.")
      .createWithDefaultString("30s")

  private[spark] val STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH =
    ConfigBuilder("spark.storage.decommission.fallbackStorage.path")
      .doc("The location for fallback storage during block manager decommissioning. " +
        "For example, `s3a://spark-storage/`. In case of empty, fallback storage is disabled. " +
        "The storage should be managed by TTL because Spark will not clean it up.")
      .version("3.1.0")
      .stringConf
      .checkValue(_.endsWith(java.io.File.separator), "Path should end with separator.")
      .createOptional

  private[spark] val STORAGE_REPLICATION_TOPOLOGY_FILE =
    ConfigBuilder("spark.storage.replication.topologyFile")
      .version("2.1.0")
      .stringConf
      .createOptional

  private[spark] val STORAGE_EXCEPTION_PIN_LEAK =
    ConfigBuilder("spark.storage.exceptionOnPinLeak")
      .version("1.6.2")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STORAGE_BLOCKMANAGER_TIMEOUTINTERVAL =
    ConfigBuilder("spark.storage.blockManagerTimeoutIntervalMs")
      .version("0.7.3")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("60s")

  private[spark] val STORAGE_BLOCKMANAGER_HEARTBEAT_TIMEOUT =
    ConfigBuilder("spark.storage.blockManagerHeartbeatTimeoutMs")
      .version("0.7.0")
      .withAlternative("spark.storage.blockManagerSlaveTimeoutMs")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString(Network.NETWORK_TIMEOUT.defaultValueString)

  private[spark] val STORAGE_CLEANUP_FILES_AFTER_EXECUTOR_EXIT =
    ConfigBuilder("spark.storage.cleanupFilesAfterExecutorExit")
      .doc("Whether or not cleanup the files not served by the external shuffle service " +
        "on executor exits.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val DISKSTORE_SUB_DIRECTORIES =
    ConfigBuilder("spark.diskStore.subDirectories")
      .doc("Number of subdirectories inside each path listed in spark.local.dir for " +
        "hashing Block files into.")
      .version("0.6.0")
      .intConf
      .checkValue(_ > 0, "The number of subdirectories must be positive.")
      .createWithDefault(64)

  private[spark] val BLOCK_FAILURES_BEFORE_LOCATION_REFRESH =
    ConfigBuilder("spark.block.failures.beforeLocationRefresh")
      .doc("Max number of failures before this block manager refreshes " +
        "the block locations from the driver.")
      .version("2.0.0")
      .intConf
      .createWithDefault(5)

  private[spark] val IS_PYTHON_APP =
    ConfigBuilder("spark.yarn.isPython")
      .internal()
      .version("1.5.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val CPUS_PER_TASK =
    ConfigBuilder("spark.task.cpus").version("0.5.0").intConf.createWithDefault(1)

  private[spark] val DYN_ALLOCATION_ENABLED =
    ConfigBuilder("spark.dynamicAllocation.enabled")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DYN_ALLOCATION_TESTING =
    ConfigBuilder("spark.dynamicAllocation.testing")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DYN_ALLOCATION_MIN_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.minExecutors")
      .version("1.2.0")
      .intConf
      .createWithDefault(0)

  private[spark] val DYN_ALLOCATION_INITIAL_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.initialExecutors")
      .version("1.3.0")
      .fallbackConf(DYN_ALLOCATION_MIN_EXECUTORS)

  private[spark] val DYN_ALLOCATION_MAX_EXECUTORS =
    ConfigBuilder("spark.dynamicAllocation.maxExecutors")
      .version("1.2.0")
      .intConf
      .createWithDefault(Int.MaxValue)

  private[spark] val DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO =
    ConfigBuilder("spark.dynamicAllocation.executorAllocationRatio")
      .version("2.4.0")
      .doubleConf
      .createWithDefault(1.0)

  private[spark] val DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT =
    ConfigBuilder("spark.dynamicAllocation.cachedExecutorIdleTimeout")
      .version("1.4.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0L, "Timeout must be >= 0.")
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT =
    ConfigBuilder("spark.dynamicAllocation.executorIdleTimeout")
      .version("1.2.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0L, "Timeout must be >= 0.")
      .createWithDefault(60)

  private[spark] val DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED =
    ConfigBuilder("spark.dynamicAllocation.shuffleTracking.enabled")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT =
    ConfigBuilder("spark.dynamicAllocation.shuffleTracking.timeout")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .checkValue(_ >= 0L, "Timeout must be >= 0.")
      .createWithDefault(Long.MaxValue)

  private[spark] val DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT =
    ConfigBuilder("spark.dynamicAllocation.schedulerBacklogTimeout")
      .version("1.2.0")
      .timeConf(TimeUnit.SECONDS).createWithDefault(1)

  private[spark] val DYN_ALLOCATION_SUSTAINED_SCHEDULER_BACKLOG_TIMEOUT =
    ConfigBuilder("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout")
      .version("1.2.0")
      .fallbackConf(DYN_ALLOCATION_SCHEDULER_BACKLOG_TIMEOUT)

  private[spark] val LEGACY_LOCALITY_WAIT_RESET =
    ConfigBuilder("spark.locality.wait.legacyResetOnTaskLaunch")
    .doc("Whether to use the legacy behavior of locality wait, which resets the delay timer " +
      "anytime a task is scheduled. See Delay Scheduling section of TaskSchedulerImpl's class " +
      "documentation for more details.")
    .internal()
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val LOCALITY_WAIT = ConfigBuilder("spark.locality.wait")
    .version("0.5.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("3s")

  private[spark] val SHUFFLE_SERVICE_ENABLED =
    ConfigBuilder("spark.shuffle.service.enabled")
      .version("1.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_SERVICE_FETCH_RDD_ENABLED =
    ConfigBuilder(Constants.SHUFFLE_SERVICE_FETCH_RDD_ENABLED)
      .doc("Whether to use the ExternalShuffleService for fetching disk persisted RDD blocks. " +
        "In case of dynamic allocation if this feature is enabled executors having only disk " +
        "persisted blocks are considered idle after " +
        "'spark.dynamicAllocation.executorIdleTimeout' and will be released accordingly.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_SERVICE_DB_ENABLED =
    ConfigBuilder("spark.shuffle.service.db.enabled")
      .doc("Whether to use db in ExternalShuffleService. Note that this only affects " +
        "standalone mode.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_SERVICE_PORT =
    ConfigBuilder("spark.shuffle.service.port").version("1.2.0").intConf.createWithDefault(7337)

  private[spark] val KEYTAB = ConfigBuilder("spark.kerberos.keytab")
    .doc("Location of user's keytab.")
    .version("3.0.0")
    .stringConf.createOptional

  private[spark] val PRINCIPAL = ConfigBuilder("spark.kerberos.principal")
    .doc("Name of the Kerberos principal.")
    .version("3.0.0")
    .stringConf
    .createOptional

  private[spark] val KERBEROS_RELOGIN_PERIOD = ConfigBuilder("spark.kerberos.relogin.period")
    .version("3.0.0")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("1m")

  private[spark] val KERBEROS_RENEWAL_CREDENTIALS =
    ConfigBuilder("spark.kerberos.renewal.credentials")
      .doc(
        "Which credentials to use when renewing delegation tokens for executors. Can be either " +
        "'keytab', the default, which requires a keytab to be provided, or 'ccache', which uses " +
        "the local credentials cache.")
      .version("3.0.0")
      .stringConf
      .checkValues(Set("keytab", "ccache"))
      .createWithDefault("keytab")

  private[spark] val KERBEROS_FILESYSTEMS_TO_ACCESS =
    ConfigBuilder("spark.kerberos.access.hadoopFileSystems")
    .doc("Extra Hadoop filesystem URLs for which to request delegation tokens. The filesystem " +
      "that hosts fs.defaultFS does not need to be listed here.")
    .version("3.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val EXECUTOR_INSTANCES = ConfigBuilder("spark.executor.instances")
    .version("1.0.0")
    .intConf
    .createOptional

  private[spark] val PY_FILES = ConfigBuilder("spark.yarn.dist.pyFiles")
    .internal()
    .version("2.2.1")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val TASK_MAX_DIRECT_RESULT_SIZE =
    ConfigBuilder("spark.task.maxDirectResultSize")
      .version("2.0.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1L << 20)

  private[spark] val TASK_MAX_FAILURES =
    ConfigBuilder("spark.task.maxFailures")
      .version("0.8.0")
      .intConf
      .createWithDefault(4)

  private[spark] val TASK_REAPER_ENABLED =
    ConfigBuilder("spark.task.reaper.enabled")
      .version("2.0.3")
      .booleanConf
      .createWithDefault(false)

  private[spark] val TASK_REAPER_KILL_TIMEOUT =
    ConfigBuilder("spark.task.reaper.killTimeout")
      .version("2.0.3")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(-1)

  private[spark] val TASK_REAPER_POLLING_INTERVAL =
    ConfigBuilder("spark.task.reaper.pollingInterval")
      .version("2.0.3")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("10s")

  private[spark] val TASK_REAPER_THREAD_DUMP =
    ConfigBuilder("spark.task.reaper.threadDump")
      .version("2.0.3")
      .booleanConf
      .createWithDefault(true)

  private[spark] val EXCLUDE_ON_FAILURE_ENABLED =
    ConfigBuilder("spark.excludeOnFailure.enabled")
      .version("3.1.0")
      .withAlternative("spark.blacklist.enabled")
      .booleanConf
      .createOptional

  private[spark] val MAX_TASK_ATTEMPTS_PER_EXECUTOR =
    ConfigBuilder("spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor")
      .version("3.1.0")
      .withAlternative("spark.blacklist.task.maxTaskAttemptsPerExecutor")
      .intConf
      .createWithDefault(1)

  private[spark] val MAX_TASK_ATTEMPTS_PER_NODE =
    ConfigBuilder("spark.excludeOnFailure.task.maxTaskAttemptsPerNode")
      .version("3.1.0")
      .withAlternative("spark.blacklist.task.maxTaskAttemptsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC =
    ConfigBuilder("spark.excludeOnFailure.application.maxFailedTasksPerExecutor")
      .version("3.1.0")
      .withAlternative("spark.blacklist.application.maxFailedTasksPerExecutor")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILURES_PER_EXEC_STAGE =
    ConfigBuilder("spark.excludeOnFailure.stage.maxFailedTasksPerExecutor")
      .version("3.1.0")
      .withAlternative("spark.blacklist.stage.maxFailedTasksPerExecutor")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE =
    ConfigBuilder("spark.excludeOnFailure.application.maxFailedExecutorsPerNode")
      .version("3.1.0")
      .withAlternative("spark.blacklist.application.maxFailedExecutorsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val MAX_FAILED_EXEC_PER_NODE_STAGE =
    ConfigBuilder("spark.excludeOnFailure.stage.maxFailedExecutorsPerNode")
      .version("3.1.0")
      .withAlternative("spark.blacklist.stage.maxFailedExecutorsPerNode")
      .intConf
      .createWithDefault(2)

  private[spark] val EXCLUDE_ON_FAILURE_TIMEOUT_CONF =
    ConfigBuilder("spark.excludeOnFailure.timeout")
      .version("3.1.0")
      .withAlternative("spark.blacklist.timeout")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val EXCLUDE_ON_FAILURE_KILL_ENABLED =
    ConfigBuilder("spark.excludeOnFailure.killExcludedExecutors")
      .version("3.1.0")
      .withAlternative("spark.blacklist.killBlacklistedExecutors")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXCLUDE_ON_FAILURE_LEGACY_TIMEOUT_CONF =
    ConfigBuilder("spark.scheduler.executorTaskExcludeOnFailureTime")
      .internal()
      .version("3.1.0")
      .withAlternative("spark.scheduler.executorTaskBlacklistTime")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val EXCLUDE_ON_FAILURE_FETCH_FAILURE_ENABLED =
    ConfigBuilder("spark.excludeOnFailure.application.fetchFailure.enabled")
      .version("3.1.0")
      .withAlternative("spark.blacklist.application.fetchFailure.enabled")
      .booleanConf
      .createWithDefault(false)

  private[spark] val UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE =
    ConfigBuilder("spark.files.fetchFailure.unRegisterOutputOnHost")
      .doc("Whether to un-register all the outputs on the host in condition that we receive " +
        " a FetchFailure. This is set default to false, which means, we only un-register the " +
        " outputs related to the exact executor(instead of the host) on a FetchFailure.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val LISTENER_BUS_EVENT_QUEUE_CAPACITY =
    ConfigBuilder("spark.scheduler.listenerbus.eventqueue.capacity")
      .doc("The default capacity for event queues. Spark will try to initialize " +
        "an event queue using capacity specified by `spark.scheduler.listenerbus" +
        ".eventqueue.queueName.capacity` first. If it's not configured, Spark will " +
        "use the default capacity specified by this config.")
      .version("2.3.0")
      .intConf
      .checkValue(_ > 0, "The capacity of listener bus event queue must be positive")
      .createWithDefault(10000)

  private[spark] val LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED =
    ConfigBuilder("spark.scheduler.listenerbus.metrics.maxListenerClassesTimed")
      .internal()
      .version("2.3.0")
      .intConf
      .createWithDefault(128)

  private[spark] val LISTENER_BUS_LOG_SLOW_EVENT_ENABLED =
    ConfigBuilder("spark.scheduler.listenerbus.logSlowEvent")
      .internal()
      .doc("When enabled, log the event that takes too much time to process. This helps us " +
        "discover the event types that cause performance bottlenecks. The time threshold is " +
        "controlled by spark.scheduler.listenerbus.logSlowEvent.threshold.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val LISTENER_BUS_LOG_SLOW_EVENT_TIME_THRESHOLD =
    ConfigBuilder("spark.scheduler.listenerbus.logSlowEvent.threshold")
      .internal()
      .doc("The time threshold of whether a event is considered to be taking too much time to " +
        s"process. Log the event if ${LISTENER_BUS_LOG_SLOW_EVENT_ENABLED.key} is true.")
      .version("3.0.0")
      .timeConf(TimeUnit.NANOSECONDS)
      .createWithDefaultString("1s")

  // This property sets the root namespace for metrics reporting
  private[spark] val METRICS_NAMESPACE = ConfigBuilder("spark.metrics.namespace")
    .version("2.1.0")
    .stringConf
    .createOptional

  private[spark] val METRICS_CONF = ConfigBuilder("spark.metrics.conf")
    .version("0.8.0")
    .stringConf
    .createOptional

  private[spark] val METRICS_EXECUTORMETRICS_SOURCE_ENABLED =
    ConfigBuilder("spark.metrics.executorMetricsSource.enabled")
      .doc("Whether to register the ExecutorMetrics source with the metrics system.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val METRICS_STATIC_SOURCES_ENABLED =
    ConfigBuilder("spark.metrics.staticSources.enabled")
      .doc("Whether to register static sources with the metrics system.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val PYSPARK_DRIVER_PYTHON = ConfigBuilder("spark.pyspark.driver.python")
    .version("2.1.0")
    .stringConf
    .createOptional

  private[spark] val PYSPARK_PYTHON = ConfigBuilder("spark.pyspark.python")
    .version("2.1.0")
    .stringConf
    .createOptional

  // To limit how many applications are shown in the History Server summary ui
  private[spark] val HISTORY_UI_MAX_APPS =
    ConfigBuilder("spark.history.ui.maxApplications")
      .version("2.0.1")
      .intConf
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val IO_ENCRYPTION_ENABLED = ConfigBuilder("spark.io.encryption.enabled")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val IO_ENCRYPTION_KEYGEN_ALGORITHM =
    ConfigBuilder("spark.io.encryption.keygen.algorithm")
      .version("2.1.0")
      .stringConf
      .createWithDefault("HmacSHA1")

  private[spark] val IO_ENCRYPTION_KEY_SIZE_BITS = ConfigBuilder("spark.io.encryption.keySizeBits")
    .version("2.1.0")
    .intConf
    .checkValues(Set(128, 192, 256))
    .createWithDefault(128)

  private[spark] val IO_CRYPTO_CIPHER_TRANSFORMATION =
    ConfigBuilder("spark.io.crypto.cipher.transformation")
      .internal()
      .version("2.1.0")
      .stringConf
      .createWithDefaultString("AES/CTR/NoPadding")

  private[spark] val DRIVER_HOST_ADDRESS = ConfigBuilder("spark.driver.host")
    .doc("Address of driver endpoints.")
    .version("0.7.0")
    .stringConf
    .createWithDefault(Utils.localCanonicalHostName())

  private[spark] val DRIVER_PORT = ConfigBuilder("spark.driver.port")
    .doc("Port of driver endpoints.")
    .version("0.7.0")
    .intConf
    .createWithDefault(0)

  private[spark] val DRIVER_SUPERVISE = ConfigBuilder("spark.driver.supervise")
    .doc("If true, restarts the driver automatically if it fails with a non-zero exit status. " +
      "Only has effect in Spark standalone mode or Mesos cluster deploy mode.")
    .version("1.3.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val DRIVER_BIND_ADDRESS = ConfigBuilder("spark.driver.bindAddress")
    .doc("Address where to bind network listen sockets on the driver.")
    .version("2.1.0")
    .fallbackConf(DRIVER_HOST_ADDRESS)

  private[spark] val BLOCK_MANAGER_PORT = ConfigBuilder("spark.blockManager.port")
    .doc("Port to use for the block manager when a more specific setting is not provided.")
    .version("1.1.0")
    .intConf
    .createWithDefault(0)

  private[spark] val DRIVER_BLOCK_MANAGER_PORT = ConfigBuilder("spark.driver.blockManager.port")
    .doc("Port to use for the block manager on the driver.")
    .version("2.1.0")
    .fallbackConf(BLOCK_MANAGER_PORT)

  private[spark] val IGNORE_CORRUPT_FILES = ConfigBuilder("spark.files.ignoreCorruptFiles")
    .doc("Whether to ignore corrupt files. If true, the Spark jobs will continue to run when " +
      "encountering corrupted or non-existing files and contents that have been read will still " +
      "be returned.")
    .version("2.1.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val IGNORE_MISSING_FILES = ConfigBuilder("spark.files.ignoreMissingFiles")
    .doc("Whether to ignore missing files. If true, the Spark jobs will continue to run when " +
      "encountering missing files and the contents that have been read will still be returned.")
    .version("2.4.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val APP_CALLER_CONTEXT = ConfigBuilder("spark.log.callerContext")
    .version("2.2.0")
    .stringConf
    .createOptional

  private[spark] val FILES_MAX_PARTITION_BYTES = ConfigBuilder("spark.files.maxPartitionBytes")
    .doc("The maximum number of bytes to pack into a single partition when reading files.")
    .version("2.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(128 * 1024 * 1024)

  private[spark] val FILES_OPEN_COST_IN_BYTES = ConfigBuilder("spark.files.openCostInBytes")
    .doc("The estimated cost to open a file, measured by the number of bytes could be scanned in" +
      " the same time. This is used when putting multiple files into a partition. It's better to" +
      " over estimate, then the partitions with small files will be faster than partitions with" +
      " bigger files.")
    .version("2.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefault(4 * 1024 * 1024)

  private[spark] val HADOOP_RDD_IGNORE_EMPTY_SPLITS =
    ConfigBuilder("spark.hadoopRDD.ignoreEmptySplits")
      .internal()
      .doc("When true, HadoopRDD/NewHadoopRDD will not create partitions for empty input splits.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SECRET_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.regex")
      .doc("Regex to decide which Spark configuration properties and environment variables in " +
        "driver and executor environments contain sensitive information. When this regex matches " +
        "a property key or value, the value is redacted from the environment UI and various logs " +
        "like YARN and event logs.")
      .version("2.1.2")
      .regexConf
      .createWithDefault("(?i)secret|password|token".r)

  private[spark] val STRING_REDACTION_PATTERN =
    ConfigBuilder("spark.redaction.string.regex")
      .doc("Regex to decide which parts of strings produced by Spark contain sensitive " +
        "information. When this regex matches a string part, that string part is replaced by a " +
        "dummy value. This is currently used to redact the output of SQL explain commands.")
      .version("2.2.0")
      .regexConf
      .createOptional

  private[spark] val AUTH_SECRET =
    ConfigBuilder("spark.authenticate.secret")
      .version("1.0.0")
      .stringConf
      .createOptional

  private[spark] val AUTH_SECRET_BIT_LENGTH =
    ConfigBuilder("spark.authenticate.secretBitLength")
      .version("1.6.0")
      .intConf
      .createWithDefault(256)

  private[spark] val NETWORK_AUTH_ENABLED =
    ConfigBuilder("spark.authenticate")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SASL_ENCRYPTION_ENABLED =
    ConfigBuilder("spark.authenticate.enableSaslEncryption")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val AUTH_SECRET_FILE =
    ConfigBuilder("spark.authenticate.secret.file")
      .doc("Path to a file that contains the authentication secret to use. The secret key is " +
        "loaded from this path on both the driver and the executors if overrides are not set for " +
        "either entity (see below). File-based secret keys are only allowed when using " +
        "Kubernetes.")
      .version("3.0.0")
      .stringConf
      .createOptional

  private[spark] val AUTH_SECRET_FILE_DRIVER =
    ConfigBuilder("spark.authenticate.secret.driver.file")
      .doc("Path to a file that contains the authentication secret to use. Loaded by the " +
        "driver. In Kubernetes client mode it is often useful to set a different secret " +
        "path for the driver vs. the executors, since the driver may not be running in " +
        "a pod unlike the executors. If this is set, an accompanying secret file must " +
        "be specified for the executors. The fallback configuration allows the same path to be " +
        "used for both the driver and the executors when running in cluster mode. File-based " +
        "secret keys are only allowed when using Kubernetes.")
      .version("3.0.0")
      .fallbackConf(AUTH_SECRET_FILE)

  private[spark] val AUTH_SECRET_FILE_EXECUTOR =
    ConfigBuilder("spark.authenticate.secret.executor.file")
      .doc("Path to a file that contains the authentication secret to use. Loaded by the " +
        "executors only. In Kubernetes client mode it is often useful to set a different " +
        "secret path for the driver vs. the executors, since the driver may not be running " +
        "in a pod unlike the executors. If this is set, an accompanying secret file must be " +
        "specified for the executors. The fallback configuration allows the same path to be " +
        "used for both the driver and the executors when running in cluster mode. File-based " +
        "secret keys are only allowed when using Kubernetes.")
      .version("3.0.0")
      .fallbackConf(AUTH_SECRET_FILE)

  private[spark] val BUFFER_WRITE_CHUNK_SIZE =
    ConfigBuilder("spark.buffer.write.chunkSize")
      .internal()
      .doc("The chunk size in bytes during writing out the bytes of ChunkedByteBuffer.")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(_ <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
        "The chunk size during writing out the bytes of ChunkedByteBuffer should" +
          s" be less than or equal to ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
      .createWithDefault(64 * 1024 * 1024)

  private[spark] val CHECKPOINT_COMPRESS =
    ConfigBuilder("spark.checkpoint.compress")
      .doc("Whether to compress RDD checkpoints. Generally a good idea. Compression will use " +
        "spark.io.compression.codec.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val CACHE_CHECKPOINT_PREFERRED_LOCS_EXPIRE_TIME =
    ConfigBuilder("spark.rdd.checkpoint.cachePreferredLocsExpireTime")
      .internal()
      .doc("Expire time in minutes for caching preferred locations of checkpointed RDD." +
        "Caching preferred locations can relieve query loading to DFS and save the query " +
        "time. The drawback is that the cached locations can be possibly outdated and " +
        "lose data locality. If this config is not specified, it will not cache.")
      .version("3.0.0")
      .timeConf(TimeUnit.MINUTES)
      .checkValue(_ > 0, "The expire time for caching preferred locations cannot be non-positive.")
      .createOptional

  private[spark] val SHUFFLE_ACCURATE_BLOCK_THRESHOLD =
    ConfigBuilder("spark.shuffle.accurateBlockThreshold")
      .doc("Threshold in bytes above which the size of shuffle blocks in " +
        "HighlyCompressedMapStatus is accurately recorded. This helps to prevent OOM " +
        "by avoiding underestimating shuffle block size when fetch shuffle blocks.")
      .version("2.2.1")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(100 * 1024 * 1024)

  private[spark] val SHUFFLE_REGISTRATION_TIMEOUT =
    ConfigBuilder("spark.shuffle.registration.timeout")
      .doc("Timeout in milliseconds for registration to the external shuffle service.")
      .version("2.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(5000)

  private[spark] val SHUFFLE_REGISTRATION_MAX_ATTEMPTS =
    ConfigBuilder("spark.shuffle.registration.maxAttempts")
      .doc("When we fail to register to the external shuffle service, we will " +
        "retry for maxAttempts times.")
      .version("2.3.0")
      .intConf
      .createWithDefault(3)

  private[spark] val REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS =
    ConfigBuilder("spark.reducer.maxBlocksInFlightPerAddress")
      .doc("This configuration limits the number of remote blocks being fetched per reduce task " +
        "from a given host port. When a large number of blocks are being requested from a given " +
        "address in a single fetch or simultaneously, this could crash the serving executor or " +
        "Node Manager. This is especially useful to reduce the load on the Node Manager when " +
        "external shuffle is enabled. You can mitigate the issue by setting it to a lower value.")
      .version("2.2.1")
      .intConf
      .checkValue(_ > 0, "The max no. of blocks in flight cannot be non-positive.")
      .createWithDefault(Int.MaxValue)

  private[spark] val MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM =
    ConfigBuilder("spark.network.maxRemoteBlockSizeFetchToMem")
      .doc("Remote block will be fetched to disk when size of the block is above this threshold " +
        "in bytes. This is to avoid a giant request takes too much memory. Note this " +
        "configuration will affect both shuffle fetch and block manager remote block fetch. " +
        "For users who enabled external shuffle service, this feature can only work when " +
        "external shuffle service is at least 2.3.0.")
      .version("3.0.0")
      .bytesConf(ByteUnit.BYTE)
      // fetch-to-mem is guaranteed to fail if the message is bigger than 2 GB, so we might
      // as well use fetch-to-disk in that case.  The message includes some metadata in addition
      // to the block data itself (in particular UploadBlock has a lot of metadata), so we leave
      // extra room.
      .checkValue(
        _ <= Int.MaxValue - 512,
        "maxRemoteBlockSizeFetchToMem cannot be larger than (Int.MaxValue - 512) bytes.")
      .createWithDefaultString("200m")

  private[spark] val TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES =
    ConfigBuilder("spark.taskMetrics.trackUpdatedBlockStatuses")
      .doc("Enable tracking of updatedBlockStatuses in the TaskMetrics. Off by default since " +
        "tracking the block statuses can use a lot of memory and its not used anywhere within " +
        "spark.")
      .version("2.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_IO_PLUGIN_CLASS =
    ConfigBuilder("spark.shuffle.sort.io.plugin.class")
      .doc("Name of the class to use for shuffle IO.")
      .version("3.0.0")
      .stringConf
      .createWithDefault(classOf[LocalDiskShuffleDataIO].getName)

  private[spark] val SHUFFLE_FILE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.file.buffer")
      .doc("Size of the in-memory buffer for each shuffle file output stream, in KiB unless " +
        "otherwise specified. These buffers reduce the number of disk seeks and system calls " +
        "made in creating intermediate shuffle files.")
      .version("1.4.0")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024,
        s"The file buffer size must be positive and less than or equal to" +
          s" ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.unsafe.file.output.buffer")
      .doc("The file system for this buffer size after each partition " +
        "is written in unsafe shuffle writer. In KiB unless otherwise specified.")
      .version("2.3.0")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024,
        s"The buffer size must be positive and less than or equal to" +
          s" ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024}.")
      .createWithDefaultString("32k")

  private[spark] val SHUFFLE_DISK_WRITE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.spill.diskWriteBufferSize")
      .doc("The buffer size, in bytes, to use when writing the sorted records to an on-disk file.")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v > 12 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
        s"The buffer size must be greater than 12 and less than or equal to " +
          s"${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH}.")
      .createWithDefault(1024 * 1024)

  private[spark] val UNROLL_MEMORY_CHECK_PERIOD =
    ConfigBuilder("spark.storage.unrollMemoryCheckPeriod")
      .internal()
      .doc("The memory check period is used to determine how often we should check whether "
        + "there is a need to request more memory when we try to unroll the given block in memory.")
      .version("2.3.0")
      .longConf
      .createWithDefault(16)

  private[spark] val UNROLL_MEMORY_GROWTH_FACTOR =
    ConfigBuilder("spark.storage.unrollMemoryGrowthFactor")
      .internal()
      .doc("Memory to request as a multiple of the size that used to unroll the block.")
      .version("2.3.0")
      .doubleConf
      .createWithDefault(1.5)

  private[spark] val FORCE_DOWNLOAD_SCHEMES =
    ConfigBuilder("spark.yarn.dist.forceDownloadSchemes")
      .doc("Comma-separated list of schemes for which resources will be downloaded to the " +
        "local disk prior to being added to YARN's distributed cache. For use in cases " +
        "where the YARN service does not support schemes that are supported by Spark, like http, " +
        "https and ftp, or jars required to be in the local YARN client's classpath. Wildcard " +
        "'*' is denoted to download resources for all the schemes.")
      .version("2.3.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  private[spark] val EXTRA_LISTENERS = ConfigBuilder("spark.extraListeners")
    .doc("Class names of listeners to add to SparkContext during initialization.")
    .version("1.3.0")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD =
    ConfigBuilder("spark.shuffle.spill.numElementsForceSpillThreshold")
      .internal()
      .doc("The maximum number of elements in memory before forcing the shuffle sorter to spill. " +
        "By default it's Integer.MAX_VALUE, which means we never force the sorter to spill, " +
        "until we reach some limitations, like the max page size limitation for the pointer " +
        "array in the sorter.")
      .version("1.6.0")
      .intConf
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD =
    ConfigBuilder("spark.shuffle.mapOutput.parallelAggregationThreshold")
      .internal()
      .doc("Multi-thread is used when the number of mappers * shuffle partitions is greater than " +
        "or equal to this threshold. Note that the actual parallelism is calculated by number of " +
        "mappers * shuffle partitions / this threshold + 1, so this threshold should be positive.")
      .version("2.3.0")
      .intConf
      .checkValue(v => v > 0, "The threshold should be positive.")
      .createWithDefault(10000000)

  private[spark] val MAX_RESULT_SIZE = ConfigBuilder("spark.driver.maxResultSize")
    .doc("Size limit for results.")
    .version("1.2.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("1g")

  private[spark] val CREDENTIALS_RENEWAL_INTERVAL_RATIO =
    ConfigBuilder("spark.security.credentials.renewalRatio")
      .doc("Ratio of the credential's expiration time when Spark should fetch new credentials.")
      .version("2.4.0")
      .doubleConf
      .createWithDefault(0.75d)

  private[spark] val CREDENTIALS_RENEWAL_RETRY_WAIT =
    ConfigBuilder("spark.security.credentials.retryWait")
      .doc("How long to wait before retrying to fetch new credentials after a failure.")
      .version("2.4.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("1h")

  private[spark] val SHUFFLE_SORT_INIT_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.sort.initialBufferSize")
      .internal()
      .version("2.1.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v > 0 && v <= Int.MaxValue,
        s"The buffer size must be greater than 0 and less than or equal to ${Int.MaxValue}.")
      .createWithDefault(4096)

  private[spark] val SHUFFLE_COMPRESS =
    ConfigBuilder("spark.shuffle.compress")
      .doc("Whether to compress shuffle output. Compression will use " +
        "spark.io.compression.codec.")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_SPILL_COMPRESS =
    ConfigBuilder("spark.shuffle.spill.compress")
      .doc("Whether to compress data spilled during shuffles. Compression will use " +
        "spark.io.compression.codec.")
      .version("0.9.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val MAP_STATUS_COMPRESSION_CODEC =
    ConfigBuilder("spark.shuffle.mapStatus.compression.codec")
      .internal()
      .doc("The codec used to compress MapStatus, which is generated by ShuffleMapTask. " +
        "By default, Spark provides four codecs: lz4, lzf, snappy, and zstd. You can also " +
        "use fully qualified class names to specify the codec.")
      .version("3.0.0")
      .stringConf
      .createWithDefault("zstd")

  private[spark] val SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD =
    ConfigBuilder("spark.shuffle.spill.initialMemoryThreshold")
      .internal()
      .doc("Initial threshold for the size of a collection before we start tracking its " +
        "memory usage.")
      .version("1.1.1")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(5 * 1024 * 1024)

  private[spark] val SHUFFLE_SPILL_BATCH_SIZE =
    ConfigBuilder("spark.shuffle.spill.batchSize")
      .internal()
      .doc("Size of object batches when reading/writing from serializers.")
      .version("0.9.0")
      .longConf
      .createWithDefault(10000)

  private[spark] val SHUFFLE_SORT_BYPASS_MERGE_THRESHOLD =
    ConfigBuilder("spark.shuffle.sort.bypassMergeThreshold")
      .doc("In the sort-based shuffle manager, avoid merge-sorting data if there is no " +
        "map-side aggregation and there are at most this many reduce partitions")
      .version("1.1.1")
      .intConf
      .createWithDefault(200)

  private[spark] val SHUFFLE_MANAGER =
    ConfigBuilder("spark.shuffle.manager")
      .version("1.1.0")
      .stringConf
      .createWithDefault("sort")

  private[spark] val SHUFFLE_REDUCE_LOCALITY_ENABLE =
    ConfigBuilder("spark.shuffle.reduceLocality.enabled")
      .doc("Whether to compute locality preferences for reduce tasks")
      .version("1.5.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_MAPOUTPUT_MIN_SIZE_FOR_BROADCAST =
    ConfigBuilder("spark.shuffle.mapOutput.minSizeForBroadcast")
      .doc("The size at which we use Broadcast to send the map output statuses to the executors.")
      .version("2.0.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("512k")

  private[spark] val SHUFFLE_MAPOUTPUT_DISPATCHER_NUM_THREADS =
    ConfigBuilder("spark.shuffle.mapOutput.dispatcher.numThreads")
      .version("2.0.0")
      .intConf
      .createWithDefault(8)

  private[spark] val SHUFFLE_DETECT_CORRUPT =
    ConfigBuilder("spark.shuffle.detectCorrupt")
      .doc("Whether to detect any corruption in fetched blocks.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_DETECT_CORRUPT_MEMORY =
    ConfigBuilder("spark.shuffle.detectCorrupt.useExtraMemory")
      .doc("If enabled, part of a compressed/encrypted stream will be de-compressed/de-crypted " +
        "by using extra memory to detect early corruption. Any IOException thrown will cause " +
        "the task to be retried once and if it fails again with same exception, then " +
        "FetchFailedException will be thrown to retry previous stage")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_SYNC =
    ConfigBuilder("spark.shuffle.sync")
      .doc("Whether to force outstanding writes to disk.")
      .version("0.8.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_UNSAFE_FAST_MERGE_ENABLE =
    ConfigBuilder("spark.shuffle.unsafe.fastMergeEnabled")
      .doc("Whether to perform a fast spill merge.")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_SORT_USE_RADIXSORT =
    ConfigBuilder("spark.shuffle.sort.useRadixSort")
      .doc("Whether to use radix sort for sorting in-memory partition ids. Radix sort is much " +
        "faster, but requires additional memory to be reserved memory as pointers are added.")
      .version("2.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val SHUFFLE_MIN_NUM_PARTS_TO_HIGHLY_COMPRESS =
    ConfigBuilder("spark.shuffle.minNumPartitionsToHighlyCompress")
      .internal()
      .doc("Number of partitions to determine if MapStatus should use HighlyCompressedMapStatus")
      .version("2.4.0")
      .intConf
      .checkValue(v => v > 0, "The value should be a positive integer.")
      .createWithDefault(2000)

  private[spark] val SHUFFLE_USE_OLD_FETCH_PROTOCOL =
    ConfigBuilder("spark.shuffle.useOldFetchProtocol")
      .doc("Whether to use the old protocol while doing the shuffle block fetching. " +
        "It is only enabled while we need the compatibility in the scenario of new Spark " +
        "version job fetching shuffle blocks from old version external shuffle service.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED =
    ConfigBuilder("spark.shuffle.readHostLocalDisk")
      .doc(s"If enabled (and `${SHUFFLE_USE_OLD_FETCH_PROTOCOL.key}` is disabled, shuffle " +
        "blocks requested from those block managers which are running on the same host are " +
        "read from the disk directly instead of being fetched as remote blocks over the network.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val STORAGE_LOCAL_DISK_BY_EXECUTORS_CACHE_SIZE =
    ConfigBuilder("spark.storage.localDiskByExecutors.cacheSize")
      .doc("The max number of executors for which the local dirs are stored. This size is " +
        "both applied for the driver and both for the executors side to avoid having an " +
        "unbounded store. This cache will be used to avoid the network in case of fetching disk " +
        s"persisted RDD blocks or shuffle blocks " +
        s"(when `${SHUFFLE_HOST_LOCAL_DISK_READING_ENABLED.key}` is set) from the same host.")
      .version("3.0.0")
      .intConf
      .createWithDefault(1000)

  private[spark] val MEMORY_MAP_LIMIT_FOR_TESTS =
    ConfigBuilder("spark.storage.memoryMapLimitForTests")
      .internal()
      .doc("For testing only, controls the size of chunks when memory mapping a file")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)

  private[spark] val BARRIER_SYNC_TIMEOUT =
    ConfigBuilder("spark.barrier.sync.timeout")
      .doc("The timeout in seconds for each barrier() call from a barrier task. If the " +
        "coordinator didn't receive all the sync messages from barrier tasks within the " +
        "configured time, throw a SparkException to fail all the tasks. The default value is set " +
        "to 31536000(3600 * 24 * 365) so the barrier() call shall wait for one year.")
      .version("2.4.0")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(v => v > 0, "The value should be a positive time value.")
      .createWithDefaultString("365d")

  private[spark] val UNSCHEDULABLE_TASKSET_TIMEOUT =
    ConfigBuilder("spark.scheduler.excludeOnFailure.unschedulableTaskSetTimeout")
      .doc("The timeout in seconds to wait to acquire a new executor and schedule a task " +
        "before aborting a TaskSet which is unschedulable because all executors are " +
        "excluded due to failures.")
      .version("3.1.0")
      .withAlternative("spark.scheduler.blacklist.unschedulableTaskSetTimeout")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(v => v >= 0, "The value should be a non negative time value.")
      .createWithDefault(120)

  private[spark] val BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL =
    ConfigBuilder("spark.scheduler.barrier.maxConcurrentTasksCheck.interval")
      .doc("Time in seconds to wait between a max concurrent tasks check failure and the next " +
        "check. A max concurrent tasks check ensures the cluster can launch more concurrent " +
        "tasks than required by a barrier stage on job submitted. The check can fail in case " +
        "a cluster has just started and not enough executors have registered, so we wait for a " +
        "little while and try to perform the check again. If the check fails more than a " +
        "configured max failure times for a job then fail current job submission. Note this " +
        "config only applies to jobs that contain one or more barrier stages, we won't perform " +
        "the check on non-barrier jobs.")
      .version("2.4.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("15s")

  private[spark] val BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES =
    ConfigBuilder("spark.scheduler.barrier.maxConcurrentTasksCheck.maxFailures")
      .doc("Number of max concurrent tasks check failures allowed before fail a job submission. " +
        "A max concurrent tasks check ensures the cluster can launch more concurrent tasks than " +
        "required by a barrier stage on job submitted. The check can fail in case a cluster " +
        "has just started and not enough executors have registered, so we wait for a little " +
        "while and try to perform the check again. If the check fails more than a configured " +
        "max failure times for a job then fail current job submission. Note this config only " +
        "applies to jobs that contain one or more barrier stages, we won't perform the check on " +
        "non-barrier jobs.")
      .version("2.4.0")
      .intConf
      .checkValue(v => v > 0, "The max failures should be a positive value.")
      .createWithDefault(40)

  private[spark] val UNSAFE_EXCEPTION_ON_MEMORY_LEAK =
    ConfigBuilder("spark.unsafe.exceptionOnMemoryLeak")
      .internal()
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val UNSAFE_SORTER_SPILL_READ_AHEAD_ENABLED =
    ConfigBuilder("spark.unsafe.sorter.spill.read.ahead.enabled")
      .internal()
      .version("2.3.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val UNSAFE_SORTER_SPILL_READER_BUFFER_SIZE =
    ConfigBuilder("spark.unsafe.sorter.spill.reader.buffer.size")
      .internal()
      .version("2.1.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => 1024 * 1024 <= v && v <= MAX_BUFFER_SIZE_BYTES,
        s"The value must be in allowed range [1,048,576, ${MAX_BUFFER_SIZE_BYTES}].")
      .createWithDefault(1024 * 1024)

  private[spark] val DEFAULT_PLUGINS_LIST = "spark.plugins.defaultList"

  private[spark] val PLUGINS =
    ConfigBuilder("spark.plugins")
      .withPrepended(DEFAULT_PLUGINS_LIST, separator = ",")
      .doc("Comma-separated list of class names implementing " +
        "org.apache.spark.api.plugin.SparkPlugin to load into the application.")
      .version("3.0.0")
      .stringConf
      .toSequence
      .createWithDefault(Nil)

  private[spark] val CLEANER_PERIODIC_GC_INTERVAL =
    ConfigBuilder("spark.cleaner.periodicGC.interval")
      .version("1.6.0")
      .timeConf(TimeUnit.SECONDS)
      .createWithDefaultString("30min")

  private[spark] val CLEANER_REFERENCE_TRACKING =
    ConfigBuilder("spark.cleaner.referenceTracking")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val CLEANER_REFERENCE_TRACKING_BLOCKING =
    ConfigBuilder("spark.cleaner.referenceTracking.blocking")
      .version("1.0.0")
      .booleanConf
      .createWithDefault(true)

  private[spark] val CLEANER_REFERENCE_TRACKING_BLOCKING_SHUFFLE =
    ConfigBuilder("spark.cleaner.referenceTracking.blocking.shuffle")
      .version("1.1.1")
      .booleanConf
      .createWithDefault(false)

  private[spark] val CLEANER_REFERENCE_TRACKING_CLEAN_CHECKPOINTS =
    ConfigBuilder("spark.cleaner.referenceTracking.cleanCheckpoints")
      .version("1.4.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_LOGS_ROLLING_STRATEGY =
    ConfigBuilder("spark.executor.logs.rolling.strategy")
      .version("1.1.0")
      .stringConf
      .createWithDefault("")

  private[spark] val EXECUTOR_LOGS_ROLLING_TIME_INTERVAL =
    ConfigBuilder("spark.executor.logs.rolling.time.interval")
      .version("1.1.0")
      .stringConf
      .createWithDefault("daily")

  private[spark] val EXECUTOR_LOGS_ROLLING_MAX_SIZE =
    ConfigBuilder("spark.executor.logs.rolling.maxSize")
      .version("1.4.0")
      .stringConf
      .createWithDefault((1024 * 1024).toString)

  private[spark] val EXECUTOR_LOGS_ROLLING_MAX_RETAINED_FILES =
    ConfigBuilder("spark.executor.logs.rolling.maxRetainedFiles")
      .version("1.1.0")
      .intConf
      .createWithDefault(-1)

  private[spark] val EXECUTOR_LOGS_ROLLING_ENABLE_COMPRESSION =
    ConfigBuilder("spark.executor.logs.rolling.enableCompression")
      .version("2.0.2")
      .booleanConf
      .createWithDefault(false)

  private[spark] val MASTER_REST_SERVER_ENABLED = ConfigBuilder("spark.master.rest.enabled")
    .version("1.3.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val MASTER_REST_SERVER_PORT = ConfigBuilder("spark.master.rest.port")
    .version("1.3.0")
    .intConf
    .createWithDefault(6066)

  private[spark] val MASTER_UI_PORT = ConfigBuilder("spark.master.ui.port")
    .version("1.1.0")
    .intConf
    .createWithDefault(8080)

  private[spark] val IO_COMPRESSION_SNAPPY_BLOCKSIZE =
    ConfigBuilder("spark.io.compression.snappy.blockSize")
      .doc("Block size in bytes used in Snappy compression, in the case when " +
        "Snappy compression codec is used. Lowering this block size " +
        "will also lower shuffle memory usage when Snappy is used")
      .version("1.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32k")

  private[spark] val IO_COMPRESSION_LZ4_BLOCKSIZE =
    ConfigBuilder("spark.io.compression.lz4.blockSize")
      .doc("Block size in bytes used in LZ4 compression, in the case when LZ4 compression" +
        "codec is used. Lowering this block size will also lower shuffle memory " +
        "usage when LZ4 is used.")
      .version("1.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32k")

  private[spark] val IO_COMPRESSION_CODEC =
    ConfigBuilder("spark.io.compression.codec")
      .doc("The codec used to compress internal data such as RDD partitions, event log, " +
        "broadcast variables and shuffle outputs. By default, Spark provides four codecs: " +
        "lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify " +
        "the codec")
      .version("0.8.0")
      .stringConf
      .createWithDefaultString("lz4")

  private[spark] val IO_COMPRESSION_ZSTD_BUFFERSIZE =
    ConfigBuilder("spark.io.compression.zstd.bufferSize")
      .doc("Buffer size in bytes used in Zstd compression, in the case when Zstd " +
        "compression codec is used. Lowering this size will lower the shuffle " +
        "memory usage when Zstd is used, but it might increase the compression " +
        "cost because of excessive JNI call overhead")
      .version("2.3.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("32k")

  private[spark] val IO_COMPRESSION_ZSTD_LEVEL =
    ConfigBuilder("spark.io.compression.zstd.level")
      .doc("Compression level for Zstd compression codec. Increasing the compression " +
        "level will result in better compression at the expense of more CPU and memory")
      .version("2.3.0")
      .intConf
      .createWithDefault(1)

  private[spark] val IO_WARNING_LARGEFILETHRESHOLD =
    ConfigBuilder("spark.io.warning.largeFileThreshold")
      .internal()
      .doc("If the size in bytes of a file loaded by Spark exceeds this threshold, " +
        "a warning is logged with the possible reasons.")
      .version("3.0.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefault(1024 * 1024 * 1024)

  private[spark] val EVENT_LOG_COMPRESSION_CODEC =
    ConfigBuilder("spark.eventLog.compression.codec")
      .doc("The codec used to compress event log. By default, Spark provides four codecs: " +
        "lz4, lzf, snappy, and zstd. You can also use fully qualified class names to specify " +
        "the codec. If this is not given, spark.io.compression.codec will be used.")
      .version("3.0.0")
      .fallbackConf(IO_COMPRESSION_CODEC)

  private[spark] val BUFFER_SIZE =
    ConfigBuilder("spark.buffer.size")
      .version("0.5.0")
      .intConf
      .checkValue(_ >= 0, "The buffer size must not be negative")
      .createWithDefault(65536)

  private[spark] val LOCALITY_WAIT_PROCESS = ConfigBuilder("spark.locality.wait.process")
    .version("0.8.0")
    .fallbackConf(LOCALITY_WAIT)

  private[spark] val LOCALITY_WAIT_NODE = ConfigBuilder("spark.locality.wait.node")
    .version("0.8.0")
    .fallbackConf(LOCALITY_WAIT)

  private[spark] val LOCALITY_WAIT_RACK = ConfigBuilder("spark.locality.wait.rack")
    .version("0.8.0")
    .fallbackConf(LOCALITY_WAIT)

  private[spark] val REDUCER_MAX_SIZE_IN_FLIGHT = ConfigBuilder("spark.reducer.maxSizeInFlight")
    .doc("Maximum size of map outputs to fetch simultaneously from each reduce task, " +
      "in MiB unless otherwise specified. Since each output requires us to create a " +
      "buffer to receive it, this represents a fixed memory overhead per reduce task, " +
      "so keep it small unless you have a large amount of memory")
    .version("1.4.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("48m")

  private[spark] val REDUCER_MAX_REQS_IN_FLIGHT = ConfigBuilder("spark.reducer.maxReqsInFlight")
    .doc("This configuration limits the number of remote requests to fetch blocks at " +
      "any given point. When the number of hosts in the cluster increase, " +
      "it might lead to very large number of inbound connections to one or more nodes, " +
      "causing the workers to fail under load. By allowing it to limit the number of " +
      "fetch requests, this scenario can be mitigated")
    .version("2.0.0")
    .intConf
    .createWithDefault(Int.MaxValue)

  private[spark] val BROADCAST_COMPRESS = ConfigBuilder("spark.broadcast.compress")
    .doc("Whether to compress broadcast variables before sending them. " +
      "Generally a good idea. Compression will use spark.io.compression.codec")
    .version("0.6.0")
    .booleanConf.createWithDefault(true)

  private[spark] val BROADCAST_BLOCKSIZE = ConfigBuilder("spark.broadcast.blockSize")
    .doc("Size of each piece of a block for TorrentBroadcastFactory, in " +
      "KiB unless otherwise specified. Too large a value decreases " +
      "parallelism during broadcast (makes it slower); however, " +
      "if it is too small, BlockManager might take a performance hit")
    .version("0.5.0")
    .bytesConf(ByteUnit.KiB)
    .createWithDefaultString("4m")

  private[spark] val BROADCAST_CHECKSUM = ConfigBuilder("spark.broadcast.checksum")
    .doc("Whether to enable checksum for broadcast. If enabled, " +
      "broadcasts will include a checksum, which can help detect " +
      "corrupted blocks, at the cost of computing and sending a little " +
      "more data. It's possible to disable it if the network has other " +
      "mechanisms to guarantee data won't be corrupted during broadcast")
    .version("2.1.1")
    .booleanConf
    .createWithDefault(true)

  private[spark] val BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD =
    ConfigBuilder("spark.broadcast.UDFCompressionThreshold")
      .doc("The threshold at which user-defined functions (UDFs) and Python RDD commands " +
        "are compressed by broadcast in bytes unless otherwise specified")
      .version("3.0.0")
      .bytesConf(ByteUnit.BYTE)
      .checkValue(v => v >= 0, "The threshold should be non-negative.")
      .createWithDefault(1L * 1024 * 1024)

  private[spark] val RDD_COMPRESS = ConfigBuilder("spark.rdd.compress")
    .doc("Whether to compress serialized RDD partitions " +
      "(e.g. for StorageLevel.MEMORY_ONLY_SER in Scala " +
      "or StorageLevel.MEMORY_ONLY in Python). Can save substantial " +
      "space at the cost of some extra CPU time. " +
      "Compression will use spark.io.compression.codec")
    .version("0.6.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val RDD_PARALLEL_LISTING_THRESHOLD =
    ConfigBuilder("spark.rdd.parallelListingThreshold")
      .version("2.0.0")
      .intConf
      .createWithDefault(10)

  private[spark] val RDD_LIMIT_SCALE_UP_FACTOR =
    ConfigBuilder("spark.rdd.limit.scaleUpFactor")
      .version("2.1.0")
      .intConf
      .createWithDefault(4)

  private[spark] val SERIALIZER = ConfigBuilder("spark.serializer")
    .version("0.5.0")
    .stringConf
    .createWithDefault("org.apache.spark.serializer.JavaSerializer")

  private[spark] val SERIALIZER_OBJECT_STREAM_RESET =
    ConfigBuilder("spark.serializer.objectStreamReset")
      .version("1.0.0")
      .intConf
      .createWithDefault(100)

  private[spark] val SERIALIZER_EXTRA_DEBUG_INFO = ConfigBuilder("spark.serializer.extraDebugInfo")
    .version("1.3.0")
    .booleanConf
    .createWithDefault(true)

  private[spark] val JARS = ConfigBuilder("spark.jars")
    .version("0.9.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val FILES = ConfigBuilder("spark.files")
    .version("1.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val SUBMIT_DEPLOY_MODE = ConfigBuilder("spark.submit.deployMode")
    .version("1.5.0")
    .stringConf
    .createWithDefault("client")

  private[spark] val SUBMIT_PYTHON_FILES = ConfigBuilder("spark.submit.pyFiles")
    .version("1.0.1")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val SCHEDULER_ALLOCATION_FILE =
    ConfigBuilder("spark.scheduler.allocation.file")
      .version("0.8.1")
      .stringConf
      .createOptional

  private[spark] val SCHEDULER_MIN_REGISTERED_RESOURCES_RATIO =
    ConfigBuilder("spark.scheduler.minRegisteredResourcesRatio")
      .version("1.1.1")
      .doubleConf
      .createOptional

  private[spark] val SCHEDULER_MAX_REGISTERED_RESOURCE_WAITING_TIME =
    ConfigBuilder("spark.scheduler.maxRegisteredResourcesWaitingTime")
      .version("1.1.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("30s")

  private[spark] val SCHEDULER_MODE =
    ConfigBuilder("spark.scheduler.mode")
      .version("0.8.0")
      .stringConf
      .transform(_.toUpperCase(Locale.ROOT))
      .createWithDefault(SchedulingMode.FIFO.toString)

  private[spark] val SCHEDULER_REVIVE_INTERVAL =
    ConfigBuilder("spark.scheduler.revive.interval")
      .version("0.8.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val SPECULATION_ENABLED =
    ConfigBuilder("spark.speculation")
      .version("0.6.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SPECULATION_INTERVAL =
    ConfigBuilder("spark.speculation.interval")
      .version("0.6.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefault(100)

  private[spark] val SPECULATION_MULTIPLIER =
    ConfigBuilder("spark.speculation.multiplier")
      .version("0.6.0")
      .doubleConf
      .createWithDefault(1.5)

  private[spark] val SPECULATION_QUANTILE =
    ConfigBuilder("spark.speculation.quantile")
      .version("0.6.0")
      .doubleConf
      .createWithDefault(0.75)

  private[spark] val SPECULATION_TASK_DURATION_THRESHOLD =
    ConfigBuilder("spark.speculation.task.duration.threshold")
      .doc("Task duration after which scheduler would try to speculative run the task. If " +
        "provided, tasks would be speculatively run if current stage contains less tasks " +
        "than or equal to the number of slots on a single executor and the task is taking " +
        "longer time than the threshold. This config helps speculate stage with very few " +
        "tasks. Regular speculation configs may also apply if the executor slots are " +
        "large enough. E.g. tasks might be re-launched if there are enough successful runs " +
        "even though the threshold hasn't been reached. The number of slots is computed based " +
        "on the conf values of spark.executor.cores and spark.task.cpus minimum 1.")
      .version("3.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val DECOMMISSION_ENABLED =
    ConfigBuilder("spark.decommission.enabled")
      .doc("When decommission enabled, Spark will try its best to shutdown the executor " +
        s"gracefully. Spark will try to migrate all the RDD blocks (controlled by " +
        s"${STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key}) and shuffle blocks (controlled by " +
        s"${STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key}) from the decommissioning " +
        s"executor to a remote executor when ${STORAGE_DECOMMISSION_ENABLED.key} is enabled. " +
        s"With decommission enabled, Spark will also decommission an executor instead of " +
        s"killing when ${DYN_ALLOCATION_ENABLED.key} enabled.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_DECOMMISSION_KILL_INTERVAL =
    ConfigBuilder("spark.executor.decommission.killInterval")
      .doc("Duration after which a decommissioned executor will be killed forcefully." +
        "This config is useful for cloud environments where we know in advance when " +
        "an executor is going to go down after decommissioning signal i.e. around 2 mins " +
        "in aws spot nodes, 1/2 hrs in spot block nodes etc. This config is currently " +
        "used to decide what tasks running on decommission executors to speculate.")
      .version("3.1.0")
      .timeConf(TimeUnit.SECONDS)
      .createOptional

  private[spark] val STAGING_DIR = ConfigBuilder("spark.yarn.stagingDir")
    .doc("Staging directory used while submitting applications.")
    .version("2.0.0")
    .stringConf
    .createOptional

  private[spark] val BUFFER_PAGESIZE = ConfigBuilder("spark.buffer.pageSize")
    .doc("The amount of memory used per page in bytes")
    .version("1.5.0")
    .bytesConf(ByteUnit.BYTE)
    .createOptional

  private[spark] val RESOURCE_PROFILE_MERGE_CONFLICTS =
    ConfigBuilder("spark.scheduler.resource.profileMergeConflicts")
      .doc("If set to true, Spark will merge ResourceProfiles when different profiles " +
        "are specified in RDDs that get combined into a single stage. When they are merged, " +
        "Spark chooses the maximum of each resource and creates a new ResourceProfile. The " +
        "default of false results in Spark throwing an exception if multiple different " +
        "ResourceProfiles are found in RDDs going into the same stage.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val STANDALONE_SUBMIT_WAIT_APP_COMPLETION =
    ConfigBuilder("spark.standalone.submit.waitAppCompletion")
      .doc("In standalone cluster mode, controls whether the client waits to exit until the " +
        "application completes. If set to true, the client process will stay alive polling " +
        "the driver's status. Otherwise, the client process will exit after submission.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_ALLOW_SPARK_CONTEXT =
    ConfigBuilder("spark.executor.allowSparkContext")
      .doc("If set to true, SparkContext can be created in executors.")
      .version("3.0.1")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_KILL_ON_FATAL_ERROR_DEPTH =
    ConfigBuilder("spark.executor.killOnFatalError.depth")
      .doc("The max depth of the exception chain in a failed task Spark will search for a fatal " +
        "error to check whether it should kill an executor. 0 means not checking any fatal " +
        "error, 1 means checking only the exception but not the cause, and so on.")
      .internal()
      .version("3.1.0")
      .intConf
      .checkValue(_ >= 0, "needs to be a non-negative value")
      .createWithDefault(5)

  private[spark] val PUSH_BASED_SHUFFLE_ENABLED =
    ConfigBuilder("spark.shuffle.push.enabled")
      .doc("Set to 'true' to enable push-based shuffle on the client side and this works in " +
        "conjunction with the server side flag spark.shuffle.server.mergedShuffleFileManagerImpl " +
        "which needs to be set with the appropriate " +
        "org.apache.spark.network.shuffle.MergedShuffleFileManager implementation for push-based " +
        "shuffle to be enabled")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val SHUFFLE_MERGER_MAX_RETAINED_LOCATIONS =
    ConfigBuilder("spark.shuffle.push.maxRetainedMergerLocations")
      .doc("Maximum number of shuffle push merger locations cached for push based shuffle. " +
        "Currently, shuffle push merger locations are nothing but external shuffle services " +
        "which are responsible for handling pushed blocks and merging them and serving " +
        "merged blocks for later shuffle fetch.")
      .version("3.1.0")
      .intConf
      .createWithDefault(500)

  private[spark] val SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO =
    ConfigBuilder("spark.shuffle.push.mergersMinThresholdRatio")
      .doc("The minimum number of shuffle merger locations required to enable push based " +
        "shuffle for a stage. This is specified as a ratio of the number of partitions in " +
        "the child stage. For example, a reduce stage which has 100 partitions and uses the " +
        "default value 0.05 requires at least 5 unique merger locations to enable push based " +
        "shuffle. Merger locations are currently defined as external shuffle services.")
      .version("3.1.0")
      .doubleConf
      .createWithDefault(0.05)

  private[spark] val SHUFFLE_MERGER_LOCATIONS_MIN_STATIC_THRESHOLD =
    ConfigBuilder("spark.shuffle.push.mergersMinStaticThreshold")
      .doc(s"The static threshold for number of shuffle push merger locations should be " +
        "available in order to enable push based shuffle for a stage. Note this config " +
        s"works in conjunction with ${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key}. " +
        "Maximum of spark.shuffle.push.mergersMinStaticThreshold and " +
        s"${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key} ratio number of mergers needed to " +
        "enable push based shuffle for a stage. For eg: with 1000 partitions for the child " +
        "stage with spark.shuffle.push.mergersMinStaticThreshold as 5 and " +
        s"${SHUFFLE_MERGER_LOCATIONS_MIN_THRESHOLD_RATIO.key} set to 0.05, we would need " +
        "at least 50 mergers to enable push based shuffle for that stage.")
      .version("3.1.0")
      .doubleConf
      .createWithDefault(5)
}
