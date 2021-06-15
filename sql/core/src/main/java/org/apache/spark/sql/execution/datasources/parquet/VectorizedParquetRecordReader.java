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

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

import org.apache.parquet.schema.MessageType;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.collection.JavaConverters;

/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 *
 * This class can either return InternalRows or ColumnarBatches. With whole stage codegen
 * enabled, this class returns ColumnarBatches which offers significant performance gains.
 * TODO: make this always return ColumnarBatches.
 */
public class VectorizedParquetRecordReader extends SpecificParquetRecordReaderBase<Object> {

  // The capacity of vectorized batch.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * Column vectors for the top-level fields, including partition columns.
   * Note the size of this array is not the same as `columnReaders` above when nested type is
   * present: the former has length equal to the number of leaf nodes in the schema.
   */
  private WritableColumnVector[] columnVectors;

  /**
   * Encapsulate writable column vectors with other Parquet related info such as
   * repetition / definition levels.
   * Note the length of this is NOT the same as `columnVectors`: the former includes partition
   * columns while this doesn't. It is == `sparkSchema.fields().length`.
   */
  private ParquetColumn[] columns;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

  /**
   * For each leaf column, if it is in the set, it means the column is missing in the file and
   * we'll instead return NULLs.
   */
  private Set<ParquetTypeInfo> missingColumns;

  /**
   * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
   * workaround incompatibilities between different engines when writing timestamp values.
   */
  private final ZoneId convertTz;

  /**
   * The mode of rebasing date/timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String datetimeRebaseMode;

  /**
   * The mode of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar.
   */
  private final String int96RebaseMode;

  /**
   * columnBatch object that is used for batch decoding. This is created on first use and triggers
   * batched decoding. It is not valid to interleave calls to the batched interface with the row
   * by row RecordReader APIs.
   * This is only enabled with additional flags for development. This is still a work in progress
   * and currently unsupported cases will fail with potentially difficult to diagnose errors.
   * This should be only turned on for development to work on this feature.
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public VectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String int96RebaseMode,
      boolean useOffHeap,
      int capacity) {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.int96RebaseMode = int96RebaseMode;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
  }

  // For test only.
  public VectorizedParquetRecordReader(boolean useOffHeap, int capacity) {
    this(null, "CORRECTED", "LEGACY", useOffHeap, capacity);
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
      UnsupportedOperationException {
    super.initialize(path, columns);
    initializeInternal();
  }

  @Override
  public void initialize(
      MessageType fileSchema,
      MessageType requestedSchema,
      ParquetRowGroupReader rowGroupReader,
      int totalRowCount) throws IOException {
    super.initialize(fileSchema, requestedSchema, rowGroupReader, totalRowCount);
    initializeInternal();
  }

  @Override
  public void close() throws IOException {
    if (columnarBatch != null) {
      columnarBatch.close();
      columnarBatch = null;
    }
    super.close();
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
      MemoryMode memMode,
      StructType partitionColumns,
      InternalRow partitionValues) {
    StructType batchSchema = new StructType();
    for (StructField f: sparkSchema.fields()) {
      batchSchema = batchSchema.add(f);
    }
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        batchSchema = batchSchema.add(f);
      }
    }

    if (memMode == MemoryMode.OFF_HEAP) {
      columnVectors = OffHeapColumnVector.allocateColumns(capacity, batchSchema);
    } else {
      columnVectors = OnHeapColumnVector.allocateColumns(capacity, batchSchema);
    }
    columnarBatch = new ColumnarBatch(columnVectors);

    columns = new ParquetColumn[sparkSchema.fields().length];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new ParquetColumn(parquetSchemaInfo.children().apply(i),
          columnVectors[i], capacity, memMode);
    }

    if (partitionColumns != null) {
      int partitionIdx = sparkSchema.fields().length;
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(columnVectors[i + partitionIdx], partitionValues, i);
        columnVectors[i + partitionIdx].setIsConstant();
      }
    }

    // Initialize missing columns with nulls.
    for (ParquetColumn col : columns) {
      for (ParquetColumn leafCol : col.getLeaves()) {
        if (missingColumns.contains(leafCol.getColumnInfo())) {
          WritableColumnVector vector = leafCol.getValueVector();
          vector.putNulls(0, capacity);
          vector.setIsConstant();
        }
      }
    }
  }

  private void initBatch() {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  /**
   * Advances to the next batch of rows. Returns false if there are no more.
   */
  public boolean nextBatch() throws IOException {
    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }
    // also need to reset repetition & definition levels for all leaf columns
    for (ParquetColumn col : columns) {
      for (ParquetColumn leafCol : col.getLeaves()) {
        leafCol.getRepetitionLevelVector().reset();
        leafCol.getDefinitionLevelVector().reset();
      }
    }

    columnarBatch.setNumRows(0);
    if (rowsReturned >= totalRowCount) return false;
    checkEndOfRowGroup();

    int num = (int) Math.min(capacity, totalCountLoadedSoFar - rowsReturned);
    for (ParquetColumn col : columns) {
      for (ParquetColumn leafCol : col.getLeaves()) {
        VectorizedColumnReader columnReader = leafCol.getColumnReader();
        if (columnReader != null) {
          columnReader.readBatch(num, leafCol.getValueVector(),
              leafCol.getRepetitionLevelVector(), leafCol.getDefinitionLevelVector());
        }
      }
      col.assemble();
    }

    rowsReturned += num;
    columnarBatch.setNumRows(num);
    numBatched = num;
    batchIdx = 0;
    return true;
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    List<ParquetPrimitiveTypeInfo> leafColInfoList =
        JavaConverters.seqAsJavaList(parquetSchemaInfo.leaves());
    missingColumns = new HashSet<>();
    for (int i = 0; i < leafColInfoList.size(); i++) {
      ColumnDescriptor desc = leafColInfoList.get(i).descriptor();
      if (fileSchema.containsPath(desc.getPath())) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(desc.getPath());
        if (!fd.equals(desc)) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
      } else {
        if (leafColInfoList.get(i).required()) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " +
              Arrays.toString(desc.getPath()));
        }
        missingColumns.add(leafColInfoList.get(i));
      }
    }
  }

  private void checkEndOfRowGroup() throws IOException {
    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
          + rowsReturned + " out of " + totalRowCount);
    }

    for (ParquetColumn col : columns) {
      for (ParquetColumn leafCol: col.getLeaves()) {
        ParquetPrimitiveTypeInfo colInfo = (ParquetPrimitiveTypeInfo) leafCol.getColumnInfo();
        if (missingColumns.contains(colInfo)) continue;
        ColumnDescriptor descriptor = colInfo.descriptor();
        VectorizedColumnReader reader = new VectorizedColumnReader(
            descriptor, pages, convertTz, datetimeRebaseMode, int96RebaseMode);
        leafCol.setColumnReader(reader);
      }
    }
    totalCountLoadedSoFar += pages.getRowCount();
  }
}
