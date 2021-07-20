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

package org.apache.spark.shuffle.checksum;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.annotation.Private;
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.ShuffleBlockResolver;
import org.apache.spark.storage.ShuffleBlockId;
import org.apache.spark.storage.ShuffleChecksumBlockId;
import org.apache.spark.util.Utils;
import scala.Option;

/**
 * A set of utility functions for the shuffle checksum.
 */
@Private
public class ShuffleChecksumHelper {

  /**
   * Used when the checksum is disabled for shuffle.
   */
  private static final Checksum[] EMPTY_CHECKSUM = new Checksum[0];
  public static final long[] EMPTY_CHECKSUM_VALUE = new long[0];
  public static final int CHECKSUM_CALCULATION_BUFFER = 8192;

  public static boolean isShuffleChecksumEnabled(SparkConf conf) {
    return (boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED());
  }

  public static Checksum[] createPartitionChecksumsIfEnabled(int numPartitions, SparkConf conf)
    throws SparkException {
    if (!isShuffleChecksumEnabled(conf)) {
      return EMPTY_CHECKSUM;
    }

    String checksumAlgo = shuffleChecksumAlgorithm(conf);
    return getChecksumByAlgorithm(numPartitions, checksumAlgo);
  }

  private static Checksum[] getChecksumByAlgorithm(int num, String algorithm)
    throws SparkException {
    Checksum[] checksums;
    switch (algorithm) {
      case "ADLER32":
        checksums = new Adler32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new Adler32();
        }
        return checksums;

      case "CRC32":
        checksums = new CRC32[num];
        for (int i = 0; i < num; i++) {
          checksums[i] = new CRC32();
        }
        return checksums;

      default:
        throw new SparkException("Unsupported shuffle checksum algorithm: " + algorithm);
    }
  }

  public static long[] getChecksumValues(Checksum[] partitionChecksums) {
    int numPartitions = partitionChecksums.length;
    long[] checksumValues = new long[numPartitions];
    for (int i = 0; i < numPartitions; i++) {
      checksumValues[i] = partitionChecksums[i].getValue();
    }
    return checksumValues;
  }

  public static String shuffleChecksumAlgorithm(SparkConf conf) {
    return conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ALGORITHM());
  }

  public static Checksum getChecksumByFileExtension(String fileName) throws SparkException {
    int index = fileName.lastIndexOf(".");
    String algorithm = fileName.substring(index + 1);
    return getChecksumByAlgorithm(1, algorithm)[0];
  }

  public static Checksum getChecksumByConf(SparkConf conf) throws SparkException {
    String algorithm = shuffleChecksumAlgorithm(conf);
    return getChecksumByAlgorithm(1, algorithm)[0];
  }

  public static String getChecksumFileName(ShuffleChecksumBlockId blockId, SparkConf conf) {
    // append the shuffle checksum algorithm as the file extension
    return String.format("%s.%s", blockId.name(), shuffleChecksumAlgorithm(conf));
  }

  public static long readChecksumByReduceId(File checksumFile, int reduceId) throws IOException {
    try (DataInputStream in = new DataInputStream(new FileInputStream(checksumFile))) {
      in.skip(reduceId * 8L);
      return in.readLong();
    }
  }

  public static long calculateChecksumForPartition(
      ShuffleBlockId blockId,
      IndexShuffleBlockResolver resolver) throws IOException, SparkException {
    InputStream in = resolver.getBlockData(blockId, Option.empty()).createInputStream();
    File checksumFile = resolver.getChecksumFile(blockId.shuffleId(), blockId.reduceId(), Option.empty());
    Checksum checksumAlgo = getChecksumByFileExtension(checksumFile.getName());
    byte[] buffer = new byte[CHECKSUM_CALCULATION_BUFFER];
    try(CheckedInputStream checksumIn = new CheckedInputStream(in, checksumAlgo)) {
      while (checksumIn.read(buffer, 0, CHECKSUM_CALCULATION_BUFFER) != -1) {}
      return checksumAlgo.getValue();
    }
  }
}
