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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * :: DeveloperApi ::
 * Performs an sort merge outer join of two child relations.
 *
 * Note: this does not support full outer join yet; see SPARK-9730 for progress on this.
 */
@DeveloperApi
case class SortMergeOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with OuterJoin {

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case LeftOuter | RightOuter => requiredOrders(leftKeys)
    case x => throw new IllegalArgumentException(
      s"SortMergeOuterJoin should not take $x as the JoinType")
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      joinType match {
        case LeftOuter =>
          val resultProj = createResultProjection()
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator,
            buildKeyGenerator,
            keyOrdering,
            streamedIter = leftIter,
            bufferedIter = rightIter
          )
          new LeftOuterIterator(smjScanner, rightNullRow, boundCondition, resultProj).toScala

        case RightOuter =>
          val resultProj = createResultProjection()
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator,
            buildKeyGenerator,
            keyOrdering,
            streamedIter = rightIter,
            bufferedIter = leftIter
          )
          new RightOuterIterator(smjScanner, leftNullRow, boundCondition, resultProj).toScala

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeOuterJoin should not take $x as the JoinType")
      }
    }
  }
}


private class LeftOuterIterator(
    smjScanner: SortMergeJoinScanner,
    rightNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow
  ) extends RowIterator {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var rightIdx: Int = 0

  private def advanceLeft(): Boolean = {
    if (smjScanner.findNextOuterJoinRows()) {
      joinedRow.withLeft(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching right rows, so return nulls for the right row
        joinedRow.withRight(rightNullRow)
      } else {
        // Find the next row from the right input that satisfied the bound condition
        if (!advanceRightUntilBoundConditionSatisfied()) {
          joinedRow.withRight(rightNullRow)
        }
      }
      true
    } else {
      // Left input has been exhausted
      false
    }
  }

  private def advanceRightUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    if (!foundMatch && rightIdx < smjScanner.getBufferedMatches.length) {
      foundMatch = boundCondition(joinedRow.withRight(smjScanner.getBufferedMatches(rightIdx)))
      rightIdx += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    advanceRightUntilBoundConditionSatisfied() || advanceLeft()
  }

  override def getNext: InternalRow = resultProj(joinedRow)
}

private class RightOuterIterator(
    smjScanner: SortMergeJoinScanner,
    leftNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow
  ) extends RowIterator {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var rightIdx: Int = 0

  private def advanceRight(): Boolean = {
    if (smjScanner.findNextOuterJoinRows()) {
      joinedRow.withRight(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching left rows, so return nulls for the left row
        joinedRow.withLeft(leftNullRow)
      } else {
        // Find the next row from the left input that satisfied the bound condition
        if (!advanceLeftUntilBoundConditionSatisfied()) {
          joinedRow.withLeft(leftNullRow)
        }
      }
      true
    } else {
      // Right input has been exhausted
      false
    }
  }

  private def advanceLeftUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    if (!foundMatch && rightIdx < smjScanner.getBufferedMatches.length) {
      foundMatch = boundCondition(joinedRow.withLeft(smjScanner.getBufferedMatches(rightIdx)))
      rightIdx += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    advanceLeftUntilBoundConditionSatisfied() || advanceRight()
  }

  override def getNext: InternalRow = resultProj(joinedRow)
}
