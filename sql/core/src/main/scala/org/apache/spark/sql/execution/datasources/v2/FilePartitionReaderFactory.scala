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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningUtils}
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class FilePartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter = filePartition.files.toIterator.map { file =>
      new PartitionedFileReader(file, buildReader(file))
    }
    new FilePartitionReader[InternalRow](iter)
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter = filePartition.files.toIterator.map { file =>
      new PartitionedFileReader(file, buildColumnarReader(file))
    }
    new FilePartitionReader[ColumnarBatch](iter)
  }

  def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow]

  def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot create columnar reader.")
  }

  protected def getReadDataSchema(
      readSchema: StructType,
      partitionSchema: StructType,
      isCaseSensitive: Boolean): StructType = {
    val partitionNameSet =
      partitionSchema.fields.map(PartitioningUtils.getColName(_, isCaseSensitive)).toSet
    val fields = readSchema.fields.filterNot { field =>
      partitionNameSet.contains(PartitioningUtils.getColName(field, isCaseSensitive))
    }

    StructType(fields)
  }
}

// A compound class for combining file and its corresponding reader.
private[v2] class PartitionedFileReader[T](
    file: PartitionedFile,
    reader: PartitionReader[T]) extends PartitionReader[T] {
  override def next(): Boolean = reader.next()

  override def get(): T = reader.get()

  override def close(): Unit = reader.close()

  override def toString: String = file.toString
}
