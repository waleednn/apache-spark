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

package org.apache.spark.sql.sources.v2.reader.streaming;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.execution.streaming.BaseStreamingSource;
import org.apache.spark.sql.sources.v2.reader.ScanConfig;
import org.apache.spark.sql.sources.v2.reader.ScanConfigBuilder;

/**
 * An interface which defines how to scan the data from data source for streaming processing with
 * micro-batch mode.
 */
@InterfaceStability.Evolving
public interface MicroBatchReadSupport extends StreamingReadSupport, BaseStreamingSource {

  /**
   * Returns a builder of {@link ScanConfig}. The builder can take some query specific information
   * like which operators to pushdown, streaming offsets, etc., and keep these information in the
   * created {@link ScanConfig}.
   *
   * This is the first step of the data scan. All other methods in {@link MicroBatchReadSupport}
   * needs to take {@link ScanConfig} as an input.
   *
   * If this method fails (by throwing an exception), the action will fail and no Spark job will be
   * submitted.
   */
  ScanConfigBuilder newScanConfigBuilder(Offset start, Offset end);

  /**
   * Returns the most recent offset available.
   */
  Offset latestOffset(Offset start);
}
