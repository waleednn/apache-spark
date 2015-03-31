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
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCountUtil;

/**
 * A wrapper message that holds two separate pieces (a header and a body) to avoid
 * copying the body's content.
 */
class MessageWithHeader extends AbstractReferenceCounted implements FileRegion {

  private final ByteBuf header;
  private final int headerLength;
  private final Object body;
  private final long bodyLength;
  private int totalBytesTransferred;

  MessageWithHeader(ByteBuf header, int headerLength, Object body, long bodyLength) {
    Preconditions.checkArgument(body instanceof ByteBuf || body instanceof FileRegion,
      "Body must be a ByteBuf or a FileRegion.");
    this.header = header;
    this.headerLength = headerLength;
    this.body = body;
    this.bodyLength = bodyLength;
  }

  @Override
  public long count() {
    return headerLength + bodyLength;
  }

  @Override
  public long position() {
    return 0;
  }

  @Override
  public long transfered() {
    return totalBytesTransferred;
  }

  @Override
  public long transferTo(WritableByteChannel target, long position) throws IOException {
    Preconditions.checkArgument(position >= 0 && position < count(), "Invalid position.");
    long written = 0;

    if (position < headerLength) {
      written += copyByteBuf(header, target, position);
      if (header.readableBytes() > 0) {
        totalBytesTransferred += written;
        return written;
      }
    }

    if (body instanceof FileRegion) {
      long bodyPos = position > headerLength ? position - headerLength : 0;
      written += ((FileRegion)body).transferTo(target, bodyPos);
    } else if (body instanceof ByteBuf) {
      written += copyByteBuf((ByteBuf) body, target, position);
    }

    totalBytesTransferred += written;
    return written;
  }

  @Override
  protected void deallocate() {
    header.release();
    ReferenceCountUtil.release(body);
  }

  private int copyByteBuf(ByteBuf buf, WritableByteChannel target, long position)
    throws IOException {

    if (position > totalBytesTransferred) {
      buf.skipBytes(Ints.checkedCast(position - totalBytesTransferred));
    }
    int written = target.write(buf.nioBuffer());
    buf.skipBytes(written);
    return written;
  }

}
