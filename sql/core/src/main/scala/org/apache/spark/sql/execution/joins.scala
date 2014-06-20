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

package org.apache.spark.sql.execution

import scala.collection.mutable.{ArrayBuffer, BitSet}

import org.apache.spark.SparkContext

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning}

@DeveloperApi
sealed abstract class BuildSide

@DeveloperApi
case object BuildLeft extends BuildSide

@DeveloperApi
case object BuildRight extends BuildSide

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class HashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  def output = left.output ++ right.output

  @transient lazy val buildSideKeyGenerator = new Projection(buildKeys, buildPlan.output)
  @transient lazy val streamSideKeyGenerator =
    () => new MutableProjection(streamedKeys, streamedPlan.output)

  def execute() = {

    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      // TODO: Use Spark's HashMap implementation.
      val hashTable = new java.util.HashMap[Row, ArrayBuffer[Row]]()
      var currentRow: Row = null

      // Create a mapping of buildKeys -> rows
      while (buildIter.hasNext) {
        currentRow = buildIter.next()
        val rowKey = buildSideKeyGenerator(currentRow)
        if(!rowKey.anyNull) {
          val existingMatchList = hashTable.get(rowKey)
          val matchList = if (existingMatchList == null) {
            val newMatchList = new ArrayBuffer[Row]()
            hashTable.put(rowKey, newMatchList)
            newMatchList
          } else {
            existingMatchList
          }
          matchList += currentRow.copy()
        }
      }

      new Iterator[Row] {
        private[this] var currentStreamedRow: Row = _
        private[this] var currentHashMatches: ArrayBuffer[Row] = _
        private[this] var currentMatchPosition: Int = -1

        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow

        private[this] val joinKeys = streamSideKeyGenerator()

        override final def hasNext: Boolean =
          (currentMatchPosition != -1 && currentMatchPosition < currentHashMatches.size) ||
          (streamIter.hasNext && fetchNext())

        override final def next() = {
          val ret = joinRow(currentStreamedRow, currentHashMatches(currentMatchPosition))
          currentMatchPosition += 1
          ret
        }

        /**
         * Searches the streamed iterator for the next row that has at least one match in hashtable.
         *
         * @return true if the search is successful, and false the streamed iterator runs out of
         *         tuples.
         */
        private final def fetchNext(): Boolean = {
          currentHashMatches = null
          currentMatchPosition = -1

          while (currentHashMatches == null && streamIter.hasNext) {
            currentStreamedRow = streamIter.next()
            if (!joinKeys(currentStreamedRow).anyNull) {
              currentHashMatches = hashTable.get(joinKeys.currentValue)
            }
          }

          if (currentHashMatches == null) {
            false
          } else {
            currentMatchPosition = 0
            true
          }
        }
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Build the right table's join keys into a HashSet, and iteratively go through the left
 * table, to find the if join keys are in the Hash set.
 */
@DeveloperApi
case class LeftSemiJoinHash(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  val (buildPlan, streamedPlan) = (right, left)
  val (buildKeys, streamedKeys) = (rightKeys, leftKeys)

  def output = left.output

  @transient lazy val buildSideKeyGenerator = new Projection(buildKeys, buildPlan.output)
  @transient lazy val streamSideKeyGenerator =
    () => new MutableProjection(streamedKeys, streamedPlan.output)

  def execute() = {

    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashSet = new java.util.HashSet[Row]()
      var currentRow: Row = null

      // Create a Hash set of buildKeys
      while (buildIter.hasNext) {
        currentRow = buildIter.next()
        val rowKey = buildSideKeyGenerator(currentRow)
        if(!rowKey.anyNull) {
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }
      }

      val joinKeys = streamSideKeyGenerator()
      streamIter.filter(current => {
        !joinKeys(current).anyNull && hashSet.contains(joinKeys.currentValue)
      })
    }
  }
}

/**
 * :: DeveloperApi ::
 * Using BroadcastNestedLoopJoin to calculate left semi join result when there's no join keys
 * for hash join.
 */
@DeveloperApi
case class LeftSemiJoinBNL(
    streamed: SparkPlan, broadcast: SparkPlan, condition: Option[Expression])
    (@transient sc: SparkContext)
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def otherCopyArgs = sc :: Nil

  def output = left.output

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  @transient lazy val boundCondition =
    InterpretedPredicate(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))


  def execute() = {
    val broadcastedRelation = sc.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    streamed.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow

      streamedIter.filter(streamedRow => {
        var i = 0
        var matched = false

        while (i < broadcastedRelation.value.size && !matched) {
          val broadcastedRow = broadcastedRelation.value(i)
          if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
            matched = true
          }
          i += 1
        }
        matched
      })
    }
  }
}

/**
 * :: DeveloperApi ::
 *In some case ,data skew happens.SkewJoin  sample the table rdd to find the largest key,then make the largest key
 *rows as a table rdd.The streamed rdd will be made  as mainstreamedtable rdd without the largest key and the maxkeystreamedtable rdd
 *with the largest key.
 *Then,join the two table  with the buildtable.
 *Finally,union the two result rdd.
 */
@DeveloperApi
case class SkewJoin(
                     leftKeys: Seq[Expression],
                     rightKeys: Seq[Expression],
                     buildSide: BuildSide,
                     left: SparkPlan,
                     right: SparkPlan,
                     @transient sc: SparkContext) extends BinaryNode {
  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }
  val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  def output = left.output ++ right.output

  @transient lazy val buildSideKeyGenerator = new Projection(buildKeys, buildPlan.output)
  @transient lazy val streamSideKeyGenerator = new Projection(streamedKeys, streamedPlan.output)


  def execute() = {
    val streamedTable = streamedPlan.execute()
    //This will later write as configuration
    val sample = streamedTable.sample(false, 0.3, 9).map(row => streamSideKeyGenerator(row)).collect()
    val sortedSample = sample.sortWith((row1, row2) => row1.hashCode() > row2.hashCode())
    var max = 0
    var num = sample.size - 1
    var temp = 0
    var maxrowKey = sortedSample(0)
    //find the largest key
    if (sortedSample.size > 1) {
      for (i <- 1 to num) {
        if (sortedSample(i - 1) == sortedSample(i)) temp += 1
        else {
          if (temp > max) {
            max = temp
            maxrowKey = sortedSample(i - 1)
          }
          temp = 0
        }
      }
    }
    val maxKeyStreamedTable = streamedTable.filter(row => {
      streamSideKeyGenerator(row).toString().equals(maxrowKey.toString())
    })
    val mainStreamedTable = streamedTable.filter(row => {
      !streamSideKeyGenerator(row).toString().equals(maxrowKey.toString())
    })
    val buildRdd = buildPlan.execute()
    val maxKeyJoinedRdd = maxKeyStreamedTable.map(_.copy()).cartesian(buildRdd.map(_.copy())).map {
      case (l: Row, r: Row) => buildRow(l ++ r)
    }
    val mainJoinedRdd = mainStreamedTable.map(_.copy()).cartesian(buildRdd.map(_.copy())).map {
      case (l: Row, r: Row) => buildRow(l ++ r)
    }
    sc.union(maxKeyJoinedRdd, mainJoinedRdd)
  }
}


object SkewJoin extends Strategy with PredicateHelper {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // Find inner joins where at least some predicates can be evaluated by matching hash keys
    // using the HashFilteredJoin pattern.
    case SkewFilteredJoin(Inner, leftKeys, rightKeys, condition, left, right) =>
      val hashJoin =
        execution.SkewJoin(leftKeys, rightKeys, BuildRight, planLater(left), planLater(right), sparkContext)
      condition.map(Filter(_, hashJoin)).getOrElse(hashJoin) :: Nil
    case _ => Nil
  }
}

object SkewFilteredJoin extends Logging with PredicateHelper {
  /** (joinType, rightKeys, leftKeys, condition, leftChild, rightChild) */
  type ReturnType =
  (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    // All predicates can be evaluated for inner join (i.e., those that are in the ON
    // clause and WHERE clause.)
    case FilteredOperation(predicates, join@Join(left, right, Inner, condition)) =>
      logger.debug(s"Considering Skew inner join on: ${predicates ++ condition}")
      splitPredicates(predicates ++ condition, join)
    case join@Join(left, right, joinType, condition) =>
      logger.debug(s"Considering Skew join on: $condition")
      splitPredicates(condition.toSeq, join)
    case _ => None
  }
/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class CartesianProduct(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  def output = left.output ++ right.output

  def execute() = left.execute().map(_.copy()).cartesian(right.execute().map(_.copy())).map {
    case (l: Row, r: Row) => buildRow(l ++ r)
  }
}

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class BroadcastNestedLoopJoin(
    streamed: SparkPlan, broadcast: SparkPlan, joinType: JoinType, condition: Option[Expression])
    (@transient sc: SparkContext)
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def otherCopyArgs = sc :: Nil

  def output = left.output ++ right.output

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  @transient lazy val boundCondition =
    InterpretedPredicate(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))


  def execute() = {
    val broadcastedRelation = sc.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    val streamedPlusMatches = streamed.execute().mapPartitions { streamedIter =>
      val matchedRows = new ArrayBuffer[Row]
      // TODO: Use Spark's BitSet.
      val includedBroadcastTuples = new BitSet(broadcastedRelation.value.size)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        var matched = false

        while (i < broadcastedRelation.value.size) {
          // TODO: One bitset per partition instead of per row.
          val broadcastedRow = broadcastedRelation.value(i)
          if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
            matchedRows += buildRow(streamedRow ++ broadcastedRow)
            matched = true
            includedBroadcastTuples += i
          }
          i += 1
        }

        if (!matched && (joinType == LeftOuter || joinType == FullOuter)) {
          matchedRows += buildRow(streamedRow ++ Array.fill(right.output.size)(null))
        }
      }
      Iterator((matchedRows, includedBroadcastTuples))
    }

    val includedBroadcastTuples = streamedPlusMatches.map(_._2)
    val allIncludedBroadcastTuples =
      if (includedBroadcastTuples.count == 0) {
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      } else {
        streamedPlusMatches.map(_._2).reduce(_ ++ _)
      }

    val rightOuterMatches: Seq[Row] =
      if (joinType == RightOuter || joinType == FullOuter) {
        broadcastedRelation.value.zipWithIndex.filter {
          case (row, i) => !allIncludedBroadcastTuples.contains(i)
        }.map {
          // TODO: Use projection.
          case (row, _) => buildRow(Vector.fill(left.output.size)(null) ++ row)
        }
      } else {
        Vector()
      }

    // TODO: Breaks lineage.
    sc.union(
      streamedPlusMatches.flatMap(_._1), sc.makeRDD(rightOuterMatches))
  }
}
