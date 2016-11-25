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

package org.apache.spark.network.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.base.Objects;
/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
  public final StreamChunkId streamChunkId;

  public ChunkFetchRequest(StreamChunkId streamChunkId) {
    this.streamChunkId = streamChunkId;
  }

  @Override
  public Type type() { return Type.ChunkFetchRequest; }

  @Override
  public long encodedLength() {
    return streamChunkId.encodedLength();
  }

  @Override
  public void encode(OutputStream out) throws IOException {
    streamChunkId.encode(out);
  }

  public static ChunkFetchRequest decode(InputStream in) throws IOException {
    return new ChunkFetchRequest(StreamChunkId.decode(in));
  }

  @Override
  public int hashCode() {
    return streamChunkId.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchRequest) {
      ChunkFetchRequest o = (ChunkFetchRequest) other;
      return streamChunkId.equals(o.streamChunkId);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .toString();
  }
}
