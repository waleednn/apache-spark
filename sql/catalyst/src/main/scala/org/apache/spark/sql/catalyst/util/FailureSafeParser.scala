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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String

class FailureSafeParser[IN](
    rawParser: IN => Seq[InternalRow],
    mode: ParseMode,
    schema: DataType,
    columnNameOfCorruptRecord: String,
    isMultiLine: Boolean) {
  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = schema match {
    case struct: StructType =>
      val corruptFieldIndex = struct.getFieldIndex(columnNameOfCorruptRecord)
      if (corruptFieldIndex.isDefined) {
        val actualSchema = StructType(struct.filterNot(_.name == columnNameOfCorruptRecord))
        val resultRow = new GenericInternalRow(struct.length)
        (row, badRecord) => {
          var i = 0
          while (i < actualSchema.length) {
            val from = actualSchema(i)
            resultRow(struct.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
            i += 1
          }
          resultRow(corruptFieldIndex.get) = badRecord()
          resultRow
        }
      } else {
        val nullResult = new GenericInternalRow(struct.length)
        (row, _) => row.getOrElse(nullResult)
      }
    case _ => (row, _) => row.getOrElse(new GenericInternalRow(1))
  }

  private val skipParsing = !isMultiLine && mode == PermissiveMode && (schema match {
    case struct: StructType => struct.isEmpty
    case _ => false
  })

  def parse(input: IN): Iterator[InternalRow] = {
    try {
     if (skipParsing) {
       Iterator.single(InternalRow.empty)
     } else {
       rawParser.apply(input).toIterator.map(row => toResultRow(Some(row), () => null))
     }
    } catch {
      case e: BadRecordException => mode match {
        case PermissiveMode =>
          Iterator(toResultRow(e.partialResult(), e.record))
        case DropMalformedMode =>
          Iterator.empty
        case FailFastMode =>
          throw new SparkException("Malformed records are detected in record parsing. " +
            s"Parse Mode: ${FailFastMode.name}.", e.cause)
      }
    }
  }
}
