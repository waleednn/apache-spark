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

package org.apache.spark.network.sasl.aes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.buffer.ChunkedByteBuffer;
import org.apache.spark.network.buffer.ChunkedByteBufferOutputStream;
import org.apache.spark.network.buffer.ChunkedByteBufferUtil;
import org.apache.spark.network.protocol.Encodable;
import org.apache.spark.network.protocol.Encoders;

/**
 * The AES cipher options for encryption negotiation.
 */
public class AesConfigMessage implements Encodable {
  /** Serialization tag used to catch incorrect payloads. */
  private static final byte TAG_BYTE = (byte) 0xEB;

  public byte[] inKey;
  public byte[] outKey;
  public byte[] inIv;
  public byte[] outIv;

  public AesConfigMessage(byte[] inKey, byte[] inIv, byte[] outKey, byte[] outIv) {
    if (inKey == null || inIv == null || outKey == null || outIv == null) {
      throw new IllegalArgumentException("Cipher Key or IV must not be null!");
    }

    this.inKey = inKey;
    this.inIv = inIv;
    this.outKey = outKey;
    this.outIv = outIv;
  }

  @Override
  public long encodedLength() {
    return 1 +
      Encoders.ByteArrays.encodedLength(inKey) + Encoders.ByteArrays.encodedLength(outKey) +
      Encoders.ByteArrays.encodedLength(inIv) + Encoders.ByteArrays.encodedLength(outIv);
  }

  @Override
  public void encode(OutputStream output) throws IOException {
    output.write(TAG_BYTE);
    Encoders.ByteArrays.encode(output, inKey);
    Encoders.ByteArrays.encode(output, inIv);
    Encoders.ByteArrays.encode(output, outKey);
    Encoders.ByteArrays.encode(output, outIv);
  }

  /**
   * Encode the config message.
   * @return ByteBuffer which contains encoded config message.
   */
  public ChunkedByteBuffer encodeMessage() throws IOException {
    ChunkedByteBufferOutputStream outputStream = ChunkedByteBufferOutputStream.newInstance();
    encode(outputStream);
    outputStream.close();
    return outputStream.toChunkedByteBuffer();
  }

  /**
   * Decode the config message from buffer
   * @param in the buffer contain encoded config message
   * @return config message
   */
  public static AesConfigMessage decodeMessage(InputStream in) throws IOException {
    if (Encoders.Bytes.decode(in) != TAG_BYTE) {
      throw new IllegalStateException("Expected AesConfigMessage, received something else"
        + " (maybe your client does not have AES enabled?)");
    }

    byte[] outKey = Encoders.ByteArrays.decode(in);
    byte[] outIv = Encoders.ByteArrays.decode(in);
    byte[] inKey = Encoders.ByteArrays.decode(in);
    byte[] inIv = Encoders.ByteArrays.decode(in);
    return new AesConfigMessage(inKey, inIv, outKey, outIv);
  }

}
