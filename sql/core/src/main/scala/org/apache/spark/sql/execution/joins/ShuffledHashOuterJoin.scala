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

package org.apache.spark.sql.execution.joins

import scala.collection.JavaConverters._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.plans.{FullOuter, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs a hash based outer join for two child relations by shuffling the data using
 * the join keys. This operator requires loading the associated partition in both side into memory.
 */
@DeveloperApi
case class ShuffledHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with OuterJoin {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  protected override def doExecute(): RDD[InternalRow] = {
    val joinedRow = new JoinedRow()
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      joinType match {
        case LeftOuter =>
          val hashed = HashedRelation(rightIter, buildKeyGenerator)
          val keyGenerator = streamedKeyGenerator
          leftIter.flatMap( currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashed.get(rowKey))
          })

        case RightOuter =>
          val hashed = HashedRelation(leftIter, buildKeyGenerator)
          val keyGenerator = streamedKeyGenerator
          rightIter.flatMap ( currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashed.get(rowKey), joinedRow)
          })

        case FullOuter =>
          // TODO(davies): use UnsafeRow
          val leftHashTable = buildHashTable(leftIter, newProjection(leftKeys, left.output))
          val rightHashTable = buildHashTable(rightIter, newProjection(rightKeys, right.output))
          (leftHashTable.keySet.asScala ++ rightHashTable.keySet.asScala).iterator.flatMap { key =>
            val leftRows: CompactBuffer[InternalRow] = {
              val rows = leftHashTable.get(key)
              if (rows == null) EMPTY_LIST else rows
            }
            val rightRows: CompactBuffer[InternalRow] = {
              val rows = rightHashTable.get(key)
              if (rows == null) EMPTY_LIST else rows
            }
            fullOuterIterator(key, leftRows, rightRows, joinedRow)
          }

        case x =>
          throw new IllegalArgumentException(
            s"ShuffledHashOuterJoin should not take $x as the JoinType")
      }
    }
  }
}
