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

import java.util.NoSuchElementException

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class SortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression] = None) extends BinaryNode {

  override protected[sql] val trackNumOfRowsEnabled = true

  val (streamedPlan, bufferedPlan, streamedKeys, bufferedKeys) = joinType match {
    case RightOuter => (right, left, rightKeys, leftKeys)
    case _ => (left, right, leftKeys, rightKeys)
  }

  override def output: Seq[Attribute] = joinType match {
    case Inner =>
      left.output ++ right.output
    case LeftOuter =>
      left.output ++ right.output.map(_.withNullability(true))
    case RightOuter =>
      left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter =>
      left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
    case x =>
      throw new IllegalStateException(s"SortMergeJoin should not take $x as the JoinType")
  }

  override def outputPartitioning: Partitioning = joinType match {
    case FullOuter =>
      // when doing Full Outer join, NULL rows from both sides are not so partitioned.
      UnknownPartitioning(streamedPlan.outputPartitioning.numPartitions)
    case Inner =>
      PartitioningCollection(Seq(streamedPlan.outputPartitioning, bufferedPlan.outputPartitioning))
    case LeftOuter | RightOuter =>
      streamedPlan.outputPartitioning
    case x =>
      throw new IllegalStateException(s"SortMergeJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = joinType match {
    case FullOuter => Nil // when doing Full Outer join, NULL rows from both sides are not ordered.
    case _ => requiredOrders(streamedKeys)
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  @transient protected lazy val streamedKeyGenerator =
    newProjection(streamedKeys, streamedPlan.output)
  @transient protected lazy val bufferedKeyGenerator =
    newProjection(bufferedKeys, bufferedPlan.output)

  // checks if the joinedRow can meet condition requirements
  @transient private[this] lazy val boundCondition =
    condition.map(newPredicate(_, streamedPlan.output ++ bufferedPlan.output)).getOrElse(
      (row: InternalRow) => true)

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val streamResults = streamedPlan.execute().map(_.copy())
    val bufferResults = bufferedPlan.execute().map(_.copy())

    streamResults.zipPartitions(bufferResults) ( (streamedIter, bufferedIter) => {
      // standard null rows
      val streamedNullRow = InternalRow.fromSeq(Seq.fill(streamedPlan.output.length)(null))
      val bufferedNullRow = InternalRow.fromSeq(Seq.fill(bufferedPlan.output.length)(null))
      new Iterator[InternalRow] {
        // An ordering that can be used to compare keys from both sides.
        private[this] val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow
        private[this] var streamedElement: InternalRow = _
        private[this] var bufferedElement: InternalRow = _
        private[this] var streamedKey: InternalRow = _
        private[this] var bufferedKey: InternalRow = _
        private[this] var bufferedMatches: CompactBuffer[InternalRow] = _
        private[this] var bufferedPosition: Int = -1
        private[this] var stop: Boolean = false
        private[this] var matchKey: InternalRow = _
        // when we do merge algorithm and find some not matched join key, there must be a side
        // that do not have a corresponding match. So we need to mark which side it is. True means
        // streamed side not have match, and False means the buffered side. Only set when needed.
        private[this] var continueStreamed: Boolean = _
        private[this] var streamNullGenerated: Boolean = false
        // Tracks if each element in bufferedMatches have a matched streamedElement.
        private[this] var bitSet: BitSet = _
        // marks if the found result has been fetched.
        private[this] var found: Boolean = false
        private[this] var bufferNullGenerated: Boolean = false

        // initialize iterator
        initialize()

        override final def hasNext: Boolean = {
          val matching = nextMatchingBlock()
          if (matching && !isBufferEmpty(bufferedMatches)) {
            // The buffer stores all rows that match key, but condition may not be matched.
            // If none of rows in the buffer match condition, we'll fetch next matching block.
            findNextInBuffer() || hasNext
          } else {
            matching
          }
        }

        /**
         * Run down the current `bufferedMatches` to find rows that match conditions.
         * If `joinType` is not `Inner`, we will use `bufferNullGenerated` to mark if
         * we need to build a bufferedNullRow for result.
         * If `joinType` is `FullOuter`, we will use `streamNullGenerated` to mark if
         * a buffered element need to join with a streamedNullRow.
         * The method can be called multiple times since `found` serves as a guardian.
         */
        def findNextInBuffer(): Boolean = {
          while (!found && streamedElement != null
            && keyOrdering.compare(streamedKey, matchKey) == 0) {
            while (bufferedPosition < bufferedMatches.size && !boundCondition(
              smartJoinRow(streamedElement, bufferedMatches(bufferedPosition)))) {
              bufferedPosition += 1
            }
            if (bufferedPosition == bufferedMatches.size) {
              if (joinType == Inner || bufferNullGenerated) {
                bufferNullGenerated = false
                bufferedPosition = 0
                fetchStreamed()
              } else {
                found = true
              }
            } else {
              // mark as true so we don't generate null row for streamed row.
              bufferNullGenerated = true
              bitSet.set(bufferedPosition)
              found = true
            }
          }
          if (!found) {
            if (joinType == FullOuter && !streamNullGenerated) {
              streamNullGenerated = true
            }
            if (streamNullGenerated) {
              while (bufferedPosition < bufferedMatches.size && bitSet.get(bufferedPosition)) {
                bufferedPosition += 1
              }
              if (bufferedPosition < bufferedMatches.size) {
                found = true
              }
            }
          }
          if (!found) {
            stop = false
            bufferedMatches = null
          }
          found
        }

        override final def next(): InternalRow = {
          if (hasNext) {
            if (isBufferEmpty(bufferedMatches)) {
              // we just found a row with no join match and we are here to produce a row
              // with this row and a standard null row from the other side.
              if (continueStreamed) {
                val joinedRow = smartJoinRow(streamedElement, bufferedNullRow)
                fetchStreamed()
                joinedRow
              } else {
                val joinedRow = smartJoinRow(streamedNullRow, bufferedElement)
                fetchBuffered()
                joinedRow
              }
            } else {
              // we are using the buffered right rows and run down left iterator
              val joinedRow = if (streamNullGenerated) {
                val ret = smartJoinRow(streamedNullRow, bufferedMatches(bufferedPosition))
                bufferedPosition += 1
                ret
              } else {
                if (bufferedPosition == bufferedMatches.size && !bufferNullGenerated) {
                  val ret = smartJoinRow(streamedElement, bufferedNullRow)
                  bufferNullGenerated = true
                  ret
                } else {
                  val ret = smartJoinRow(streamedElement, bufferedMatches(bufferedPosition))
                  bufferedPosition += 1
                  ret
                }
              }
              found = false
              joinedRow
            }
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        private def smartJoinRow(streamedRow: InternalRow, bufferedRow: InternalRow): InternalRow =
          joinType match {
            case RightOuter => joinRow(bufferedRow, streamedRow)
            case _ => joinRow(streamedRow, bufferedRow)
          }

        private def fetchStreamed(): Unit = {
          if (streamedIter.hasNext) {
            streamedElement = streamedIter.next()
            streamedKey = streamedKeyGenerator(streamedElement)
          } else {
            streamedElement = null
          }
        }

        private def fetchBuffered(): Unit = {
          if (bufferedIter.hasNext) {
            bufferedElement = bufferedIter.next()
            bufferedKey = bufferedKeyGenerator(bufferedElement)
          } else {
            bufferedElement = null
          }
        }

        private def initialize() = {
          fetchStreamed()
          fetchBuffered()
        }

        /**
         * Searches the right iterator for the next rows that have matches in left side (only check
         * key match), and stores them in a buffer.
         * This search will jump out every time from the same position until `next()` is called.
         * Unless we call `next()`, this function can be called multiple times, with the same
         * return value and result as running it once, since we have set guardians in it.
         *
         * @return true if the search is successful, and false if the right iterator runs out of
         *         tuples.
         */
        private def nextMatchingBlock(): Boolean = {
          if (!stop && streamedElement != null) {
            // step 1: run both side to get the first match pair
            while (!stop && streamedElement != null && bufferedElement != null) {
              val comparing = keyOrdering.compare(streamedKey, bufferedKey)
              // for inner join, we need to filter those null keys
              stop = comparing == 0 && !streamedKey.anyNull
              if (comparing > 0 || bufferedKey.anyNull) {
                if (joinType == FullOuter) {
                  // the join type is full outer and the buffered side has a row with no
                  // join match, so we have a result row with streamed null with buffered
                  // side as this row. Then we fetch next buffered element and go back.
                  continueStreamed = false
                  return true
                } else {
                  fetchBuffered()
                }
              } else if (comparing < 0 || streamedKey.anyNull) {
                if (joinType == Inner) {
                  fetchStreamed()
                } else {
                  // the join type is not inner and the streamed side has a row with no
                  // join match, so we have a result row with this streamed row with buffered
                  // null row. Then we fetch next streamed element and go back.
                  continueStreamed = true
                  return true
                }
              }
            }
            // step 2: run down the buffered side to put all matched rows in a buffer
            bufferedMatches = new CompactBuffer[InternalRow]()
            if (stop) {
              stop = false
              // iterate the right side to buffer all rows that matches
              // as the records should be ordered, exit when we meet the first that not match
              while (!stop) {
                bufferedMatches += bufferedElement
                fetchBuffered()
                stop =
                  keyOrdering.compare(streamedKey, bufferedKey) != 0 || bufferedElement == null
              }
              bufferedPosition = 0
              streamNullGenerated = false
              bitSet = new BitSet(bufferedMatches.size)
              matchKey = streamedKey
            }
          }
          // `stop` is false iff left or right has finished iteration in step 1.
          // if we get into step 2, `stop` cannot be false.
          if (!stop && isBufferEmpty(bufferedMatches)) {
            if (streamedElement == null && bufferedElement != null) {
              // streamedElement == null but bufferedElement != null
              if (joinType == FullOuter) {
                continueStreamed = false
                return true
              }
            } else if (streamedElement != null && bufferedElement == null) {
              // bufferedElement == null but streamedElement != null
              if (joinType != Inner) {
                continueStreamed = true
                return true
              }
            }
          }
          !isBufferEmpty(bufferedMatches)
        }

        private def isBufferEmpty(buffer: CompactBuffer[InternalRow]): Boolean =
          buffer == null || buffer.isEmpty
      }
    })
  }
}
