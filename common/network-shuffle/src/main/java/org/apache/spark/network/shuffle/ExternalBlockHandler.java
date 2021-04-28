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

package org.apache.spark.network.shuffle;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Counter;
import com.google.common.collect.Sets;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.MergedBlockMetaResponseCallback;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.StreamCallbackWithID;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.protocol.MergedBlockMetaRequest;
import org.apache.spark.network.server.OneForOneStreamManager;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.shuffle.protocol.*;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler for a server which can serve both RDD blocks and shuffle blocks from outside
 * of an Executor process.
 *
 * Handles registering executors and opening shuffle or disk persisted RDD blocks from them.
 * Blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one block.
 */
public class ExternalBlockHandler extends RpcHandler
    implements RpcHandler.MergedBlockMetaReqHandler {
  private static final Logger logger = LoggerFactory.getLogger(ExternalBlockHandler.class);
  private static final String SHUFFLE_MERGER_IDENTIFIER = "shuffle-push-merger";
  private static final String SHUFFLE_BLOCK_ID = "shuffle";
  private static final String SHUFFLE_CHUNK_ID = "shuffleChunk";

  @VisibleForTesting
  final ExternalShuffleBlockResolver blockManager;
  private final OneForOneStreamManager streamManager;
  private final ShuffleMetrics metrics;
  private final MergedShuffleFileManager mergeManager;

  public ExternalBlockHandler(TransportConf conf, File registeredExecutorFile)
    throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile),
      new NoOpMergedShuffleFileManager(conf));
  }

  public ExternalBlockHandler(
      TransportConf conf,
      File registeredExecutorFile,
      MergedShuffleFileManager mergeManager) throws IOException {
    this(new OneForOneStreamManager(),
      new ExternalShuffleBlockResolver(conf, registeredExecutorFile), mergeManager);
  }

  @VisibleForTesting
  public ExternalShuffleBlockResolver getBlockResolver() {
    return blockManager;
  }

  /** Enables mocking out the StreamManager and BlockManager. */
  @VisibleForTesting
  public ExternalBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager) {
    this(streamManager, blockManager, new NoOpMergedShuffleFileManager(null));
  }

  /** Enables mocking out the StreamManager, BlockManager, and MergeManager. */
  @VisibleForTesting
  public ExternalBlockHandler(
      OneForOneStreamManager streamManager,
      ExternalShuffleBlockResolver blockManager,
      MergedShuffleFileManager mergeManager) {
    this.metrics = new ShuffleMetrics();
    this.streamManager = streamManager;
    this.blockManager = blockManager;
    this.mergeManager = mergeManager;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(message);
    handleMessage(msgObj, client, callback);
  }

  @Override
  public StreamCallbackWithID receiveStream(
      TransportClient client,
      ByteBuffer messageHeader,
      RpcResponseCallback callback) {
    BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(messageHeader);
    if (msgObj instanceof PushBlockStream) {
      PushBlockStream message = (PushBlockStream) msgObj;
      checkAuth(client, message.appId);
      return mergeManager.receiveBlockDataAsStream(message);
    } else {
      throw new UnsupportedOperationException("Unexpected message with #receiveStream: " + msgObj);
    }
  }

  protected void handleMessage(
      BlockTransferMessage msgObj,
      TransportClient client,
      RpcResponseCallback callback) {
    if (msgObj instanceof AbstractFetchShuffleBlocks || msgObj instanceof OpenBlocks) {
      final Timer.Context responseDelayContext = metrics.openBlockRequestLatency.time();
      try {
        int numBlockIds;
        long streamId;
        if (msgObj instanceof AbstractFetchShuffleBlocks) {
          AbstractFetchShuffleBlocks msg = (AbstractFetchShuffleBlocks) msgObj;
          checkAuth(client, msg.appId);
          numBlockIds = ((AbstractFetchShuffleBlocks) msgObj).getNumBlocks();
          Iterator<ManagedBuffer> iterator;
          if (msgObj instanceof  FetchShuffleBlocks) {
            iterator = new ShuffleManagedBufferIterator((FetchShuffleBlocks)msgObj);
          } else {
            iterator = new ShuffleChunkManagedBufferIterator((FetchShuffleBlockChunks) msgObj);
          }
          streamId = streamManager.registerStream(client.getClientId(), iterator,
            client.getChannel());
        } else {
          // For the compatibility with the old version, still keep the support for OpenBlocks.
          OpenBlocks msg = (OpenBlocks) msgObj;
          numBlockIds = msg.blockIds.length;
          checkAuth(client, msg.appId);
          streamId = streamManager.registerStream(client.getClientId(),
            new ManagedBufferIterator(msg), client.getChannel());
        }
        if (logger.isTraceEnabled()) {
          logger.trace(
            "Registered streamId {} with {} buffers for client {} from host {}",
            streamId,
            numBlockIds,
            client.getClientId(),
            getRemoteAddress(client.getChannel()));
        }
        callback.onSuccess(new StreamHandle(streamId, numBlockIds).toByteBuffer());
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RegisterExecutor) {
      final Timer.Context responseDelayContext =
        metrics.registerExecutorRequestLatency.time();
      try {
        RegisterExecutor msg = (RegisterExecutor) msgObj;
        checkAuth(client, msg.appId);
        blockManager.registerExecutor(msg.appId, msg.execId, msg.executorInfo);
        mergeManager.registerExecutor(msg.appId, msg.executorInfo);
        callback.onSuccess(ByteBuffer.wrap(new byte[0]));
      } finally {
        responseDelayContext.stop();
      }

    } else if (msgObj instanceof RemoveBlocks) {
      RemoveBlocks msg = (RemoveBlocks) msgObj;
      checkAuth(client, msg.appId);
      int numRemovedBlocks = blockManager.removeBlocks(msg.appId, msg.execId, msg.blockIds);
      callback.onSuccess(new BlocksRemoved(numRemovedBlocks).toByteBuffer());

    } else if (msgObj instanceof GetLocalDirsForExecutors) {
      GetLocalDirsForExecutors msg = (GetLocalDirsForExecutors) msgObj;
      checkAuth(client, msg.appId);
      Set<String> execIdsForBlockResolver = Sets.newHashSet(msg.execIds);
      boolean fetchMergedBlockDirs = execIdsForBlockResolver.remove(SHUFFLE_MERGER_IDENTIFIER);
      Map<String, String[]> localDirs = blockManager.getLocalDirs(msg.appId,
        execIdsForBlockResolver);
      if (fetchMergedBlockDirs) {
        localDirs.put(SHUFFLE_MERGER_IDENTIFIER, mergeManager.getMergedBlockDirs(msg.appId));
      }
      callback.onSuccess(new LocalDirsForExecutors(localDirs).toByteBuffer());
    } else if (msgObj instanceof FinalizeShuffleMerge) {
      final Timer.Context responseDelayContext =
          metrics.finalizeShuffleMergeLatency.time();
      FinalizeShuffleMerge msg = (FinalizeShuffleMerge) msgObj;
      try {
        checkAuth(client, msg.appId);
        MergeStatuses statuses = mergeManager.finalizeShuffleMerge(msg);
        callback.onSuccess(statuses.toByteBuffer());
      } catch(IOException e) {
        throw new RuntimeException(String.format("Error while finalizing shuffle merge "
          + "for application %s shuffle %d", msg.appId, msg.shuffleId), e);
      } finally {
        responseDelayContext.stop();
      }
    } else {
      throw new UnsupportedOperationException("Unexpected message: " + msgObj);
    }
  }

  @Override
  public void receiveMergeBlockMetaReq(
      TransportClient client,
      MergedBlockMetaRequest metaRequest,
      MergedBlockMetaResponseCallback callback) {
    final Timer.Context responseDelayContext = metrics.fetchMergedBlocksMetaLatency.time();
    try {
      checkAuth(client, metaRequest.appId);
      MergedBlockMeta mergedMeta =
        mergeManager.getMergedBlockMeta(metaRequest.appId, metaRequest.shuffleId,
          metaRequest.reduceId);
      logger.debug(
        "Merged block chunks appId {} shuffleId {} reduceId {} num-chunks : {} ",
          metaRequest.appId, metaRequest.shuffleId, metaRequest.reduceId,
          mergedMeta.getNumChunks());
      callback.onSuccess(mergedMeta.getNumChunks(), mergedMeta.getChunksBitmapBuffer());
    } finally {
      responseDelayContext.stop();
    }
  }

  @Override
  public MergedBlockMetaReqHandler getMergedBlockMetaReqHandler() {
    return this;
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    metrics.caughtExceptions.inc();
  }

  public MetricSet getAllMetrics() {
    return metrics;
  }

  @Override
  public StreamManager getStreamManager() {
    return streamManager;
  }

  /**
   * Removes an application (once it has been terminated), and optionally will clean up any
   * local directories associated with the executors of that application in a separate thread.
   */
  public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
    blockManager.applicationRemoved(appId, cleanupLocalDirs);
    mergeManager.applicationRemoved(appId, cleanupLocalDirs);
  }

  /**
   * Clean up any non-shuffle files in any local directories associated with an finished executor.
   */
  public void executorRemoved(String executorId, String appId) {
    blockManager.executorRemoved(executorId, appId);
  }

  public void close() {
    blockManager.close();
  }

  private void checkAuth(TransportClient client, String appId) {
    if (client.getClientId() != null && !client.getClientId().equals(appId)) {
      throw new SecurityException(String.format(
        "Client for %s not authorized for application %s.", client.getClientId(), appId));
    }
  }

  /**
   * A simple class to wrap all shuffle service wrapper metrics
   */
  @VisibleForTesting
  public class ShuffleMetrics implements MetricSet {
    private final Map<String, Metric> allMetrics;
    // Time latency for open block request in ns
    private final Timer openBlockRequestLatency = new Timer();
    // Time latency for executor registration latency in ns
    private final Timer registerExecutorRequestLatency = new Timer();
    // Time latency for processing fetch merged blocks meta request latency in ns
    private final Timer fetchMergedBlocksMetaLatency = new Timer();
    // Time latency for processing finalize shuffle merge request latency in ns
    private final Timer finalizeShuffleMergeLatency = new Timer();
    // Block transfer rate in blocks per second
    private final Meter blockTransferRate = new Meter();
    // Block fetch message rate per second. When using non-batch fetches
    // (`OpenBlocks` or `FetchShuffleBlocks` with `batchFetchEnabled` as false), this will be the
    // same as the `blockTransferRate`. When batch fetches are enabled, this will represent the
    // number of batch fetches, and `blockTransferRate` will represent the number of blocks
    // returned by the fetches.
    private final Meter blockTransferMessageRate = new Meter();
    // Block transfer rate in byte per second
    private final Meter blockTransferRateBytes = new Meter();
    // Number of active connections to the shuffle service
    private Counter activeConnections = new Counter();
    // Number of exceptions caught in connections to the shuffle service
    private Counter caughtExceptions = new Counter();

    public ShuffleMetrics() {
      allMetrics = new HashMap<>();
      allMetrics.put("openBlockRequestLatency", openBlockRequestLatency);
      allMetrics.put("registerExecutorRequestLatency", registerExecutorRequestLatency);
      allMetrics.put("fetchMergedBlocksMetaLatency", fetchMergedBlocksMetaLatency);
      allMetrics.put("finalizeShuffleMergeLatency", finalizeShuffleMergeLatency);
      allMetrics.put("blockTransferRate", blockTransferRate);
      allMetrics.put("blockTransferMessageRate", blockTransferMessageRate);
      allMetrics.put("blockTransferRateBytes", blockTransferRateBytes);
      allMetrics.put("blockTransferAvgSize_1min", new RatioGauge() {
        @Override
        protected Ratio getRatio() {
          return Ratio.of(
              blockTransferRateBytes.getOneMinuteRate(),
              // use blockTransferMessageRate here instead of blockTransferRate to represent the
              // average size of the disk read / network message which has more operational impact
              // than the actual size of the block
              blockTransferMessageRate.getOneMinuteRate());
        }
      });
      allMetrics.put("registeredExecutorsSize",
                     (Gauge<Integer>) () -> blockManager.getRegisteredExecutorsSize());
      allMetrics.put("numActiveConnections", activeConnections);
      allMetrics.put("numCaughtExceptions", caughtExceptions);
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return allMetrics;
    }
  }

  private class ManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int index = 0;
    private final Function<Integer, ManagedBuffer> blockDataForIndexFn;
    private final int size;
    private boolean requestForMergedBlockChunks;

    ManagedBufferIterator(OpenBlocks msg) {
      String appId = msg.appId;
      String execId = msg.execId;
      String[] blockIds = msg.blockIds;
      String[] blockId0Parts = blockIds[0].split("_");
      if (blockId0Parts.length == 4 && blockId0Parts[0].equals(SHUFFLE_BLOCK_ID)) {
        final int shuffleId = Integer.parseInt(blockId0Parts[1]);
        final int[] mapIdAndReduceIds = shuffleMapIdAndReduceIds(blockIds, shuffleId);
        size = mapIdAndReduceIds.length;
        blockDataForIndexFn = index -> blockManager.getBlockData(appId, execId, shuffleId,
          mapIdAndReduceIds[index], mapIdAndReduceIds[index + 1]);
      } else if (blockId0Parts.length == 4 && blockId0Parts[0].equals(SHUFFLE_CHUNK_ID)) {
        requestForMergedBlockChunks = true;
        final int shuffleId = Integer.parseInt(blockId0Parts[1]);
        final int[] reduceIdAndChunkIds = shuffleMapIdAndReduceIds(blockIds, shuffleId);
        size = reduceIdAndChunkIds.length;
        blockDataForIndexFn = index -> mergeManager.getMergedBlockData(msg.appId, shuffleId,
          reduceIdAndChunkIds[index], reduceIdAndChunkIds[index + 1]);
      } else if (blockId0Parts.length == 3 && blockId0Parts[0].equals("rdd")) {
        final int[] rddAndSplitIds = rddAndSplitIds(blockIds);
        size = rddAndSplitIds.length;
        blockDataForIndexFn = index -> blockManager.getRddBlockData(appId, execId,
          rddAndSplitIds[index], rddAndSplitIds[index + 1]);
      } else {
        throw new IllegalArgumentException("Unexpected block id format: " + blockIds[0]);
      }
    }

    private int[] rddAndSplitIds(String[] blockIds) {
      final int[] rddAndSplitIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 3 || !blockIdParts[0].equals("rdd")) {
          throw new IllegalArgumentException("Unexpected RDD block id format: " + blockIds[i]);
        }
        rddAndSplitIds[2 * i] = Integer.parseInt(blockIdParts[1]);
        rddAndSplitIds[2 * i + 1] = Integer.parseInt(blockIdParts[2]);
      }
      return rddAndSplitIds;
    }

    private int[] shuffleMapIdAndReduceIds(String[] blockIds, int shuffleId) {
      // For regular shuffle blocks, primaryId is mapId and secondaryIds are reduceIds.
      // For shuffle chunks, primaryIds is reduceId and secondaryIds are chunkIds.
      final int[] primaryIdAndSecondaryIds = new int[2 * blockIds.length];
      for (int i = 0; i < blockIds.length; i++) {
        String[] blockIdParts = blockIds[i].split("_");
        if (blockIdParts.length != 4
          || (!requestForMergedBlockChunks && !blockIdParts[0].equals(SHUFFLE_BLOCK_ID))
          || (requestForMergedBlockChunks && !blockIdParts[0].equals(SHUFFLE_CHUNK_ID))) {
          throw new IllegalArgumentException("Unexpected shuffle block id format: " + blockIds[i]);
        }
        if (Integer.parseInt(blockIdParts[1]) != shuffleId) {
          throw new IllegalArgumentException("Expected shuffleId=" + shuffleId +
            ", got:" + blockIds[i]);
        }
        // For regular blocks, blockIdParts[2] is mapId. For chunks, it is reduceId.
        primaryIdAndSecondaryIds[2 * i] = Integer.parseInt(blockIdParts[2]);
        // For regular blocks, blockIdParts[3] is reduceId. For chunks, it is chunkId.
        primaryIdAndSecondaryIds[2 * i + 1] = Integer.parseInt(blockIdParts[3]);
      }
      return primaryIdAndSecondaryIds;
    }

    @Override
    public boolean hasNext() {
      return index < size;
    }

    @Override
    public ManagedBuffer next() {
      final ManagedBuffer block = blockDataForIndexFn.apply(index);
      index += 2;
      metrics.blockTransferRate.mark();
      metrics.blockTransferMessageRate.mark();
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  private class ShuffleManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int mapIdx = 0;
    private int reduceIdx = 0;

    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final long[] mapIds;
    private final int[][] reduceIds;
    private final boolean batchFetchEnabled;

    ShuffleManagedBufferIterator(FetchShuffleBlocks msg) {
      appId = msg.appId;
      execId = msg.execId;
      shuffleId = msg.shuffleId;
      mapIds = msg.mapIds;
      reduceIds = msg.reduceIds;
      batchFetchEnabled = msg.batchFetchEnabled;
    }

    @Override
    public boolean hasNext() {
      // mapIds.length must equal to reduceIds.length, and the passed in FetchShuffleBlocks
      // must have non-empty mapIds and reduceIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(mapIds.length != 0 && mapIds.length == reduceIds.length);
      return mapIdx < mapIds.length && reduceIdx < reduceIds[mapIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block;
      if (!batchFetchEnabled) {
        block = blockManager.getBlockData(
          appId, execId, shuffleId, mapIds[mapIdx], reduceIds[mapIdx][reduceIdx]);
        if (reduceIdx < reduceIds[mapIdx].length - 1) {
          reduceIdx += 1;
        } else {
          reduceIdx = 0;
          mapIdx += 1;
        }
        metrics.blockTransferRate.mark();
      } else {
        assert(reduceIds[mapIdx].length == 2);
        int startReduceId = reduceIds[mapIdx][0];
        int endReduceId = reduceIds[mapIdx][1];
        block = blockManager.getContinuousBlocksData(appId, execId, shuffleId, mapIds[mapIdx],
          startReduceId, endReduceId);
        mapIdx += 1;
        metrics.blockTransferRate.mark(endReduceId - startReduceId);
      }
      metrics.blockTransferMessageRate.mark();
      metrics.blockTransferRateBytes.mark(block != null ? block.size() : 0);
      return block;
    }
  }

  private class ShuffleChunkManagedBufferIterator implements Iterator<ManagedBuffer> {

    private int reduceIdx = 0;
    private int chunkIdx = 0;

    private final String appId;
    private final int shuffleId;
    private final int[] reduceIds;
    private final int[][] chunkIds;

    ShuffleChunkManagedBufferIterator(FetchShuffleBlockChunks msg) {
      appId = msg.appId;
      shuffleId = msg.shuffleId;
      reduceIds = msg.reduceIds;
      chunkIds = msg.chunkIds;
      // reduceIds.length must equal to chunkIds.length, and the passed in FetchShuffleBlockChunks
      // must have non-empty reduceIds and chunkIds, see the checking logic in
      // OneForOneBlockFetcher.
      assert(reduceIds.length != 0 && reduceIds.length == chunkIds.length);
    }

    @Override
    public boolean hasNext() {
      return reduceIdx < reduceIds.length && chunkIdx < chunkIds[reduceIdx].length;
    }

    @Override
    public ManagedBuffer next() {
      ManagedBuffer block = Preconditions.checkNotNull(mergeManager.getMergedBlockData(
        appId, shuffleId, reduceIds[reduceIdx], chunkIds[reduceIdx][chunkIdx]));
      if (chunkIdx < chunkIds[reduceIdx].length - 1) {
        chunkIdx += 1;
      } else {
        chunkIdx = 0;
        reduceIdx += 1;
      }
      metrics.blockTransferRateBytes.mark(block.size());
      return block;
    }
  }

  /**
   * Dummy implementation of merged shuffle file manager. Suitable for when push-based shuffle
   * is not enabled.
   *
   * @since 3.1.0
   */
  public static class NoOpMergedShuffleFileManager implements MergedShuffleFileManager {

    // This constructor is needed because we use this constructor to instantiate an implementation
    // of MergedShuffleFileManager using reflection.
    // See YarnShuffleService#newMergedShuffleFileManagerInstance.
    public NoOpMergedShuffleFileManager(TransportConf transportConf) {}

    @Override
    public StreamCallbackWithID receiveBlockDataAsStream(PushBlockStream msg) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergeStatuses finalizeShuffleMerge(FinalizeShuffleMerge msg) throws IOException {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public void registerExecutor(String appId, ExecutorShuffleInfo executorInfo) {
      // No-Op. Do nothing.
    }

    @Override
    public void applicationRemoved(String appId, boolean cleanupLocalDirs) {
      // No-Op. Do nothing.
    }

    @Override
    public ManagedBuffer getMergedBlockData(
        String appId, int shuffleId, int reduceId, int chunkId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public MergedBlockMeta getMergedBlockMeta(String appId, int shuffleId, int reduceId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }

    @Override
    public String[] getMergedBlockDirs(String appId) {
      throw new UnsupportedOperationException("Cannot handle shuffle block merge");
    }
  }

  @Override
  public void channelActive(TransportClient client) {
    metrics.activeConnections.inc();
    super.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    metrics.activeConnections.dec();
    super.channelInactive(client);
  }

}
