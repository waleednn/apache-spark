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

package org.apache.spark.util.collection.unsafe.sort;

import java.io.*;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;

import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.io.NioBufferedFileInputStream;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockId;
import org.apache.spark.unsafe.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads spill files written by {@link UnsafeSorterSpillWriter} (see that class for a description
 * of the file format).
 */
public final class UnsafeSorterSpillReader extends UnsafeSorterIterator implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(UnsafeSorterSpillReader.class);
  private static final int DEFAULT_BUFFER_SIZE_BYTES = 1024 * 1024; // 1 MB
  private static final int MAX_BUFFER_SIZE_BYTES = 16777216; // 16 mb

  private InputStream in;
  private DataInputStream din;

  // Variables that change with every record read:
  private int recordLength;
  private long keyPrefix;
  private final int numRecords;
  private int numRecordsRemaining;

  private byte[] arr = new byte[1024 * 1024];
  private Object baseObject = arr;
  private final long baseOffset = Platform.BYTE_ARRAY_OFFSET;
  private final TaskContext taskContext = TaskContext.get();

  private final long buffSize;
  private final File file;
  private final BlockId blockId;
  private final SerializerManager serializerManager;

  public UnsafeSorterSpillReader(
      SerializerManager serializerManager,
      File file,
      BlockId blockId) throws IOException {
    assert (file.length() > 0);
    long bufferSizeBytes =
        SparkEnv.get() == null ?
            DEFAULT_BUFFER_SIZE_BYTES:
            SparkEnv.get().conf().getSizeAsBytes("spark.unsafe.sorter.spill.reader.buffer.size",
                                                 DEFAULT_BUFFER_SIZE_BYTES);
    if (bufferSizeBytes > MAX_BUFFER_SIZE_BYTES || bufferSizeBytes < DEFAULT_BUFFER_SIZE_BYTES) {
      // fall back to a sane default value
      logger.warn("Value of config \"spark.unsafe.sorter.spill.reader.buffer.size\" = {} not in " +
        "allowed range [{}, {}). Falling back to default value : {} bytes", bufferSizeBytes,
        DEFAULT_BUFFER_SIZE_BYTES, MAX_BUFFER_SIZE_BYTES, DEFAULT_BUFFER_SIZE_BYTES);
      bufferSizeBytes = DEFAULT_BUFFER_SIZE_BYTES;
    }

    // No need to hold the file open until records need to be loaded.
    // This is to prevent too many files open issue partially.
    try (InputStream bs = new NioBufferedFileInputStream(file, (int) bufferSizeBytes);
        DataInputStream dataIn = new DataInputStream(serializerManager.wrapStream(blockId, bs))) {
      this.numRecords = dataIn.readInt();
      this.numRecordsRemaining = numRecords;
    }

    this.buffSize = bufferSizeBytes;
    this.file = file;
    this.blockId = blockId;
    this.serializerManager = serializerManager;
  }

  private void initStreams() throws IOException {
    final InputStream bs =
        new NioBufferedFileInputStream(file, (int) buffSize);
    try {
      this.in = serializerManager.wrapStream(blockId, bs);
      this.din = new DataInputStream(this.in);
      this.numRecordsRemaining = din.readInt();
    } catch (IOException e) {
      Closeables.close(bs, /* swallowIOException = */ true);
      throw e;
    }
  }

  @Override
  public int getNumRecords() {
    return numRecords;
  }

  @Override
  public boolean hasNext() {
    return (numRecordsRemaining > 0);
  }

  @Override
  public void loadNext() throws IOException {
    // Kill the task in case it has been marked as killed. This logic is from
    // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
    // to avoid performance overhead. This check is added here in `loadNext()` instead of in
    // `hasNext()` because it's technically possible for the caller to be relying on
    // `getNumRecords()` instead of `hasNext()` to know when to stop.
    if (taskContext != null) {
      taskContext.killTaskIfInterrupted();
    }
    if (this.din == null) {
      // It is time to initialize and hold the input stream of the spill file
      // for loading records. Keeping the input stream open too early will very possibly
      // encounter too many file open issue.
      initStreams();
    }
    recordLength = din.readInt();
    keyPrefix = din.readLong();
    if (recordLength > arr.length) {
      arr = new byte[recordLength];
      baseObject = arr;
    }
    ByteStreams.readFully(in, arr, 0, recordLength);
    numRecordsRemaining--;
    if (numRecordsRemaining == 0) {
      close();
    }
  }

  @Override
  public Object getBaseObject() {
    return baseObject;
  }

  @Override
  public long getBaseOffset() {
    return baseOffset;
  }

  @Override
  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public long getKeyPrefix() {
    return keyPrefix;
  }

  @Override
  public void close() throws IOException {
   if (in != null) {
     try {
       in.close();
     } finally {
       in = null;
       din = null;
     }
   }
  }
}
