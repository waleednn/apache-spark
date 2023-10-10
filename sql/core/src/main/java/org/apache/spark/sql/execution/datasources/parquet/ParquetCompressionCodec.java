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

package org.apache.spark.sql.execution.datasources.parquet;

import java.util.Locale;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * A mapper class from Spark supported parquet compression codecs to parquet compression codecs.
 */
public enum ParquetCompressionCodec {
  NONE(CompressionCodecName.UNCOMPRESSED),
  UNCOMPRESSED(CompressionCodecName.UNCOMPRESSED),
  SNAPPY(CompressionCodecName.SNAPPY),
  GZIP(CompressionCodecName.GZIP),
  LZO(CompressionCodecName.LZO),
  BROTLI(CompressionCodecName.BROTLI),
  LZ4(CompressionCodecName.LZ4),
  LZ4_RAW(CompressionCodecName.LZ4_RAW),
  ZSTD(CompressionCodecName.ZSTD);

  private final CompressionCodecName compressionCodec;

  ParquetCompressionCodec(CompressionCodecName compressionCodec) {
    this.compressionCodec = compressionCodec;
  }

  public CompressionCodecName getCompressionCodec() {
    return this.compressionCodec;
  }

  public static ParquetCompressionCodec fromString(String s) {
    return ParquetCompressionCodec.valueOf(s.toUpperCase(Locale.ROOT));
  }
}
