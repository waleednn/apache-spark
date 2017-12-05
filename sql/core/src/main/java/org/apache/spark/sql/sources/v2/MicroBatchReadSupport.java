package org.apache.spark.sql.sources.v2;

import java.util.Optional;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.sql.sources.v2.reader.MicroBatchReader;
import org.apache.spark.sql.types.StructType;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * provide streaming micro-batch data reading ability.
 */
@InterfaceStability.Evolving
public interface MicroBatchReadSupport extends DataSourceV2 {
  /**
   * Creates a {@link MicroBatchReader} to scan a batch of data from this data source.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   *
   * @param options the options for the returned data source reader, which is an immutable
   *                case-insensitive string-to-string map.
   */
  MicroBatchReader createMicroBatchReader(Optional<StructType> schema, String checkpointLocation, DataSourceV2Options options);
}
