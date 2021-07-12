package org.apache.spark.shuffle.checksum;

import java.util.zip.Adler32;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.internal.config.package$;
import org.apache.spark.storage.ShuffleChecksumBlockId;

public class ShuffleChecksumHelper {

  /** Used when the checksum is disabled for shuffle. */
  private static final Checksum[] EMPTY_CHECKSUM = new Checksum[0];

  public static boolean isShuffleChecksumEnabled(SparkConf conf) {
    return (boolean) conf.get(package$.MODULE$.SHUFFLE_CHECKSUM_ENABLED());
  }

  public static Checksum[] createPartitionChecksumsIfEnabled(int numPartitions, SparkConf conf)
    throws SparkException {
    Checksum[] partitionChecksums;

    if (!isShuffleChecksumEnabled(conf)) {
      partitionChecksums = EMPTY_CHECKSUM;
      return partitionChecksums;
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
        for (int i = 0; i < num; i ++) {
          checksums[i] = new Adler32();
        }
        return checksums;

      case "CRC32":
        checksums = new CRC32[num];
        for (int i = 0; i < num; i ++) {
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
    for (int i = 0; i < numPartitions; i ++) {
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
    return getChecksumByAlgorithm(1, algorithm)[1];
  }

  public static String getChecksumFileName(ShuffleChecksumBlockId blockId, SparkConf conf) {
    // append the shuffle checksum algorithm as the file extension
    return String.format("%s.%s", blockId.name(), shuffleChecksumAlgorithm(conf));
  }
}
