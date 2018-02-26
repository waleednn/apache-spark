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
import org.apache.spark.sql.sources.v2.reader.DataReader;

/**
 * A variation on {@link DataReader} for use with streaming in continuous processing mode.
 */
@InterfaceStability.Evolving
public interface ContinuousDataReader<T> extends DataReader<T> {
    /**
     * Get the offset of the current record, or the start offset if no records have been read.
     *
     * The execution engine will call this method along with get() to keep track of the current
     * offset. When an epoch ends, the offset of the previous record in each partition will be saved
     * as a restart checkpoint.
     */
    PartitionOffset getOffset();

    /**
     * Set the start offset for the current record, only used in task retry. If setOffset keep
     * default implementation, it means current ContinuousDataReader can't support task level retry.
     *
     * @param offset last offset before task retry.
     */
    default void setOffset(PartitionOffset offset) {
        throw new UnsupportedOperationException(
          "Current ContinuousDataReader can't support setOffset, task will restart " +
            "with checkpoints in ContinuousExecution.");
    }
}
