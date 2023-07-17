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

package org.apache.spark.sql.execution.python

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

class AttachDistributedSequenceEvaluatorFactory(output: Seq[Attribute])
    extends PartitionEvaluatorFactory[(InternalRow, Long), InternalRow] {
  override def createEvaluator(): PartitionEvaluator[(InternalRow, Long), InternalRow] =
    new AttachDistributedSequenceEvaluator

  private class AttachDistributedSequenceEvaluator
      extends PartitionEvaluator[(InternalRow, Long), InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[(InternalRow, Long)]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val iter = inputs(0)
      val unsafeProj = UnsafeProjection.create(output, output)
      val joinedRow = new JoinedRow
      val unsafeRowWriter =
        new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1)

      iter
        .map { case (row, id) =>
          // Writes to an UnsafeRow directly
          unsafeRowWriter.reset()
          unsafeRowWriter.write(0, id)
          joinedRow(unsafeRowWriter.getRow, row)
        }
        .map(unsafeProj)
    }
  }
}
