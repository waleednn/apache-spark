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

package org.apache.spark.network.shuffle.protocol;

import java.util.Arrays;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

/**
 * Request to find the meta information for the specified merged blocks. The meta information
 * currently contains only the number of chunks in each merged blocks.
 */
public class FetchMergedBlocksMeta extends BlockTransferMessage {
  public final String appId;
  public final String[] blockIds;

  public FetchMergedBlocksMeta(String appId, String[] blockIds) {
    this.appId = appId;
    this.blockIds = blockIds;
  }

  @Override
  protected Type type() { return Type.FETCH_MERGED_BLOCKS_META; }

  @Override
  public int hashCode() {
    return appId.hashCode() * 41 + Arrays.hashCode(blockIds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("appId", appId)
      .add("blockIds", Arrays.toString(blockIds))
      .toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof FetchMergedBlocksMeta) {
      FetchMergedBlocksMeta o = (FetchMergedBlocksMeta) other;
      return Objects.equal(appId, o.appId)
        && Arrays.equals(blockIds, o.blockIds);
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
      + Encoders.StringArrays.encodedLength(blockIds);
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    Encoders.StringArrays.encode(buf, blockIds);
  }

  public static FetchMergedBlocksMeta decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    String[] blockIds = Encoders.StringArrays.decode(buf);
    return new FetchMergedBlocksMeta(appId, blockIds);
  }
}
