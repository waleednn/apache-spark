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

package org.apache.spark.sql.execution.aggregate

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, Logging, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.TungstenSort
import org.apache.spark.sql.types.StructType

abstract class AggregationIterator(
  groupingExpressions: Seq[NamedExpression],
  nonCompleteAggregateExpressions: Seq[AggregateExpression2],
  nonCompleteAggregateAttributes: Seq[Attribute],
  completeAggregateExpressions: Seq[AggregateExpression2],
  completeAggregateAttributes: Seq[Attribute],
  initialInputBufferOffset: Int,
  resultExpressions: Seq[NamedExpression],
  newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
  newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
  newOrdering: (Seq[SortOrder], Seq[Attribute]) => Ordering[InternalRow],
  inputAttributes: Seq[Attribute],
  inputIter: Iterator[InternalRow])
  extends Iterator[InternalRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  // An Seq of all AggregateExpressions.
  // It is important that all AggregateExpressions with the mode Partial, PartialMerge or Final
  // are at the beginning of the allAggregateExpressions.
  private val allAggregateExpressions =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  require(
    allAggregateExpressions.map(_.mode).distinct.length <= 2,
    s"$allAggregateExpressions are not supported becuase they have more than 2 distinct modes.")

  /**
   * The distinct modes of AggregateExpressions. Right now, we can handle the following mode:
   *  - Partial-only: all AggregateExpressions have the mode of Partial;
   *  - PartialMerge-only: all AggregateExpressions have the mode of PartialMerge);
   *  - Final-only: all AggregateExpressions have the mode of Final;
   *  - Final-Complete: some AggregateExpressions have the mode of Final and
   *    others have the mode of Complete;
   *  - Complete-only: nonCompleteAggregateExpressions is empty and we have AggregateExpressions
   *    with mode Complete in completeAggregateExpressions; and
   *  - Grouping-only: there is no AggregateExpression.
   */
  protected val aggregationMode: (Option[AggregateMode], Option[AggregateMode]) =
    nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
      completeAggregateExpressions.map(_.mode).distinct.headOption

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected val allAggregateFunctions: Array[AggregateFunction2] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
    val functions = new Array[AggregateFunction2](allAggregateExpressions.length)
    var i = 0
    while (i < allAggregateExpressions.length) {
      val func = allAggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences = allAggregateExpressions(i).mode match {
        case Partial | Complete if !func.isInstanceOf[AlgebraicAggregate] =>
          // We need to create BoundReferences if the function is not an
          // AlgebraicAggregate (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          func.inputBufferOffset = inputBufferOffset
          inputBufferOffset += func.bufferSchema.length
          func
      }
      // Set mutableBufferOffset for this function. It is important that setting
      // mutableBufferOffset happens after all potential bindReference operations
      // because bindReference will create a new instance of the function.
      funcWithBoundReferences.mutableBufferOffset = mutableBufferOffset
      mutableBufferOffset += funcWithBoundReferences.bufferSchema.length
      functions(i) = funcWithBoundReferences
      i += 1
    }
    functions
  }

  // Positions of those non-algebraic aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are non-algebraic aggregate functions.
  // nonAlgebraicAggregateFunctionPositions will be [1, 2].
  private val allNonAlgebraicAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < allAggregateFunctions.length) {
      allAggregateFunctions(i) match {
        case agg: AlgebraicAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All AggregateFunctions functions with mode Partial, PartialMerge, or Final.
  private val nonCompleteAggregateFunctions: Array[AggregateFunction2] =
    allAggregateFunctions.take(nonCompleteAggregateExpressions.length)

  // All non-algebraic aggregate functions with mode Partial, PartialMerge, or Final.
  private val nonCompleteNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    nonCompleteAggregateFunctions.collect {
      case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
    }

  // The projection used to initialize buffer values for all AlgebraicAggregates.
  private val algebraicInitialProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.initialValues
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // All non-Algebraic AggregateFunctions.
  private val allNonAlgebraicAggregateFunctions =
    allNonAlgebraicAggregateFunctionPositions.map(allAggregateFunctions)


  ///////////////////////////////////////////////////////////////////////////
  // Methods and fields used by sub-classes.
  ///////////////////////////////////////////////////////////////////////////

  // Initializing functions used to process a row.
  protected val processRow: (MutableRow, InternalRow) => Unit = {
    val rowToBeProcessed = new JoinedRow
    val aggregationBufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    aggregationMode match {
      // Partial-only
      case (Some(Partial), None) =>
        val updateExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: AlgebraicAggregate => ae.updateExpressions
          case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
        }
        val algebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ inputAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          algebraicUpdateProjection.target(currentBuffer)
          // Process all algebraic aggregate functions.
          algebraicUpdateProjection(rowToBeProcessed(currentBuffer, row))
          // Process all non-algebraic aggregate functions.
          var i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }
        }

      // PartialMerge-only or Final-only
      case (Some(PartialMerge), None) | (Some(Final), None) =>
        val inputAggregationBufferSchema =
          groupingExpressions.map(_.toAttribute) ++
            allAggregateFunctions.flatMap(_.cloneBufferAttributes)
        val mergeExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: AlgebraicAggregate => ae.mergeExpressions
          case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
        }
        // This projection is used to merge buffer values for all AlgebraicAggregates.
        val algebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferSchema ++ inputAggregationBufferSchema)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          // Process all algebraic aggregate functions.
          algebraicMergeProjection.target(currentBuffer)(rowToBeProcessed(currentBuffer, row))
          // Process all non-algebraic aggregate functions.
          var i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Final-Complete
      case (Some(Final), Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction2] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All non-algebraic aggregate functions with mode Complete.
        val completeNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
          completeAggregateFunctions.collect {
            case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
          }

        // The first initialInputBufferOffset values of the input aggregation buffer is
        // for grouping expressions and distinct columns.
        val groupingAttributesAndDistinctColumns = inputAttributes.take(initialInputBufferOffset)

        val completeOffsetExpressions =
          Seq.fill(completeAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        // We do not touch buffer values of aggregate functions with the Final mode.
        val finalOffsetExpressions =
          Seq.fill(nonCompleteAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)

        val mergeInputSchema =
          aggregationBufferSchema ++
            groupingAttributesAndDistinctColumns ++
            nonCompleteAggregateFunctions.flatMap(_.cloneBufferAttributes)
        val mergeExpressions =
          nonCompleteAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.mergeExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          } ++ completeOffsetExpressions
        val finalAlgebraicMergeProjection =
          newMutableProjection(mergeExpressions, mergeInputSchema)()

        val updateExpressions =
          finalOffsetExpressions ++ completeAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          }
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ inputAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeNonAlgebraicAggregateFunctions.length) {
            completeNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }

          // For all aggregate functions with mode Final, merge buffers.
          finalAlgebraicMergeProjection.target(currentBuffer)(input)
          i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Complete-only
      case (None, Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction2] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All non-algebraic aggregate functions with mode Complete.
        val completeNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
          completeAggregateFunctions.collect {
            case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
          }

        val updateExpressions =
          completeAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          }
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ inputAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeNonAlgebraicAggregateFunctions.length) {
            completeNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }
        }

      // Grouping only.
      case (None, None) => (currentBuffer: MutableRow, row: InternalRow) => {}

      case other =>
        sys.error(
          s"Could not evaluate ${nonCompleteAggregateExpressions} because we do not " +
            s"support evaluate modes $other in this iterator.")
    }
  }

  // Initializing the function used to generate the output row.
  protected val generateOutput: (InternalRow, MutableRow) => InternalRow = {
    val rowToBeEvaluated = new JoinedRow

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        // Because we cannot copy a joinedRow containing a UnsafeRow (UnsafeRow does not
        // support generic getter), we create a mutable projection to output the
        // JoinedRow(currentGroupingKey, currentBuffer)
        val bufferSchema = nonCompleteAggregateFunctions.flatMap(_.bufferAttributes)
        val resultProjection =
          newMutableProjection(
            groupingExpressions.map(_.toAttribute) ++ bufferSchema,
            groupingExpressions.map(_.toAttribute) ++ bufferSchema)()

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          resultProjection(rowToBeEvaluated(currentGroupingKey, currentBuffer))
        }

      // Final-only, Complete-only and Final-Complete: every output row contains values representing
      // resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val bufferSchemata =
          allAggregateFunctions.flatMap(_.bufferAttributes)
        val evalExpressions = allAggregateFunctions.map {
          case ae: AlgebraicAggregate => ae.evaluateExpression
          case agg: AggregateFunction2 => NoOp
        }
        val algebraicEvalProjection = newMutableProjection(evalExpressions, bufferSchemata)()
        val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes
        val aggregateResult: MutableRow = new GenericMutableRow(aggregateResultSchema.length)
        val resultProjection =
          newMutableProjection(
            resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateResultSchema)()

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          // Generate results for all algebraic aggregate functions.
          algebraicEvalProjection.target(aggregateResult)(currentBuffer)
          // Generate results for all non-algebraic aggregate functions.
          var i = 0
          while (i < allNonAlgebraicAggregateFunctions.length) {
            aggregateResult.update(
              allNonAlgebraicAggregateFunctionPositions(i),
              allNonAlgebraicAggregateFunctions(i).eval(currentBuffer))
            i += 1
          }
          resultProjection(rowToBeEvaluated(currentGroupingKey, aggregateResult))
        }

      // Grouping-only: we only output values of grouping expressions.
      case (None, None) =>
        val resultProjection =
          newMutableProjection(resultExpressions, groupingExpressions.map(_.toAttribute))()

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          resultProjection(currentGroupingKey)
        }

      case other =>
        sys.error(
          s"Could not evaluate ${nonCompleteAggregateExpressions} because we do not " +
            s"support evaluate modes $other in this iterator.")
    }
  }

  // This is used to project expressions for the grouping expressions.
  protected val groupGenerator = newProjection(groupingExpressions, inputAttributes)

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: MutableRow): Unit = {
    algebraicInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allNonAlgebraicAggregateFunctions.length) {
      allNonAlgebraicAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  protected var currentGroupingKey: InternalRow = _
  // The partition key of next partition.
  protected var nextGroupingKey: InternalRow = _
  // The first row of next partition.
  protected var firstRowInNextGroup: InternalRow = _
  // Indicates if we has new group of rows from the sorted input iterator
  protected var sortedInputHasNewGroup: Boolean = false

  protected var sortBasedInputIter: Iterator[InternalRow] = inputIter

  protected var currentBuffer: MutableRow = newBuffer

  /** Processes rows in the current group. It will stop when it find a new group. */
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(currentBuffer, firstRowInNextGroup)
    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (sortBasedInputIter.hasNext && !findNextPartition) {
      val currentRow = sortBasedInputIter.next()
      // Get the grouping key based on the grouping expressions.
      // For the below compare method, we do not need to make a copy of groupingKey.
      val groupingKey = groupGenerator(currentRow)
      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        processRow(currentBuffer, currentRow)
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   */
  protected def newBuffer: MutableRow
}

/**
 * A simple set built on top of the [[UnsafeFixedWidthAggregationMap]] to store
 * grouping keys.
 */
class GroupingKeySet(groupingExpressions: Seq[NamedExpression]) {

  private[this] val map = new UnsafeFixedWidthAggregationMap(
    EmptyRow,
    StructType(Nil),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get.taskMemoryManager(),
    1024 * 16, // initial capacity
    SparkEnv.get.conf.getSizeAsBytes("spark.buffer.pageSize", "64m"),
    false // disable tracking of performance metrics
  )

  def contains(groupingKey: InternalRow): Boolean = {
    map.contains(groupingKey)
  }

  def add(groupingKey: InternalRow): Unit = {
    map.getAggregationBuffer(groupingKey)
  }

  def size(): Int = {
    map.size()
  }

  def free(): Unit = {
    map.free()
  }
}

class UnsafeHybridAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
    newOrdering: (Seq[SortOrder], Seq[Attribute]) => Ordering[InternalRow],
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection,
    newProjection,
    newOrdering,
    inputAttributes,
    inputIter) {

  require(groupingExpressions.nonEmpty)

  logInfo("Using UnsafeHybridAggregationIterator.")

  ///////////////////////////////////////////////////////////////////////////
  // Unsafe Aggregation buffers
  ///////////////////////////////////////////////////////////////////////////

  // This is the Unsafe Aggregation Map used to store all buffers.
  private[this] val buffers = new UnsafeFixedWidthAggregationMap(
    newBuffer,
    StructType.fromAttributes(allAggregateFunctions.flatMap(_.bufferAttributes)),
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute)),
    TaskContext.get.taskMemoryManager(),
    1024 * 16, // initial capacity
    SparkEnv.get.conf.getSizeAsBytes("spark.buffer.pageSize", "64m"),
    false // disable tracking of performance metrics
  )

  // The set used to track what grouping keys in the `buffers` have been processed.
  private[this] val processedKeySet = new GroupingKeySet(groupingExpressions)

  // The buffer used when we fall back to the sort-based aggregation.
  private[this] val sortBasedBuffer: MutableRow = newBuffer

  override protected def newBuffer: MutableRow = {
    val bufferRowSize: Int = allAggregateFunctions.map(_.bufferSchema.length).sum
    val projection =
      UnsafeProjection.create(allAggregateFunctions.flatMap(_.bufferAttributes).map(_.dataType))
    // We use a mutable row and a mutable projection at here since we need to fill in
    // buffer values for those nonAlgebraicAggregateFunctions manually.
    val buffer = new GenericMutableRow(bufferRowSize)
    initializeBuffer(buffer)
    projection.apply(buffer)
  }

  // Sets the currentBuffer. If the grouping key already exists in the
  // Hash Aggregation Map, we use the buffer from the map. Otherwise, we use
  // the sortBasedBuffer.
  private def setBuffer(currentGroup: InternalRow): Unit = {
    if (buffers.contains(currentGroup)) {
      // This buffer already exists in the hash map.
      currentBuffer = buffers.getAggregationBuffer(nextGroupingKey)
      // Add an entry in processedKeys.
      processedKeySet.add(nextGroupingKey)
    } else {
      initializeBuffer(sortBasedBuffer)
      currentBuffer = sortBasedBuffer
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  // This can be abstract
  override final def hasNext: Boolean =
    sortedInputHasNewGroup || buffers.size() > processedKeySet.size()

  // This can be abstract
  override final def next(): InternalRow = {
    if (hasNext) {
      val result = if (isSortBased && sortedInputHasNewGroup) {
        // Process the current group from sorted input.
        processCurrentSortedGroup()
        // Generate output row for the current group.
        val outputRow = generateOutput(currentGroupingKey, currentBuffer)
        setBuffer(nextGroupingKey)
        outputRow
      } else {
        // Since hasNext is true, it guaranteed to find an entry that has not been processed
        // from resultIterator.
        var currentGroup = resultIterator.next()
        while (processedKeySet.contains(currentGroup.key)) {
          currentGroup = resultIterator.next()
        }
        // Add this key to the processedKeySet.
        processedKeySet.add(currentGroup.key)
        generateOutput(currentGroup.key, currentGroup.value)
      }

      if (hasNext) {
        result
      } else {
        val resultCopy = result.copy()
        buffers.free()
        processedKeySet.free()
        resultCopy
      }
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // initialize this iterator.
  ///////////////////////////////////////////////////////////////////////////

  // The flag indicates if we have switched to the sort-based aggregation.
  private[this] var isSortBased = false

  /**
   * Falls back to the sort-based aggregation if the memory consumption of
   * the Hash Aggregation Map exceeds a certain threshold. It returns
   * true when we have switched to the sort-based aggregation.
   */
  private def fallBackToSortBasedIfNecessary(): Boolean = {
    if (buffers.getTotalMemoryConsumption > 1 * 1024 * 1024 && inputIter.hasNext) {
      sortedInputHasNewGroup = true
      logInfo("fall back to sort based aggregation.")
      // If we need to fallback to the sort based aggregation,
      // we first redirect inputIter to the sorter.
      val sortOrder = groupingExpressions.map(SortOrder(_, Ascending))
      // TODO: Do not convert to Unsafe if the child output rows are unsafe rows.
      val convertToUnsafe = UnsafeProjection.create(inputAttributes.map(_.dataType).toArray)
      sortBasedInputIter =
        TungstenSort.doSort(
          inputIter.map(convertToUnsafe), sortOrder, newOrdering, inputAttributes)
      // Then, we need do setup work to process the incoming group.
      firstRowInNextGroup = sortBasedInputIter.next().copy()
      nextGroupingKey = groupGenerator(firstRowInNextGroup).copy()
      setBuffer(nextGroupingKey)
      true
    } else {
      false
    }
  }

  private def initialize(): Unit = {
    while (inputIter.hasNext && !isSortBased) {
      val currentRow = inputIter.next()
      val groupingKey = groupGenerator(currentRow)
      val buffer = buffers.getAggregationBuffer(groupingKey)
      processRow(buffer, currentRow)
      if (fallBackToSortBasedIfNecessary()) {
        isSortBased = true
      }
    }
  }

  // This is the starting point of this iterator.
  initialize()

  // Creates the iterator for the Hash Aggregation Map after we have populated
  // contents of that map.
  private[this] val resultIterator = buffers.iterator()
}

class SortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
    newOrdering: (Seq[SortOrder], Seq[Attribute]) => Ordering[InternalRow],
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends AggregationIterator(
    groupingExpressions,
    nonCompleteAggregateExpressions,
    nonCompleteAggregateAttributes,
    completeAggregateExpressions,
    completeAggregateAttributes,
    initialInputBufferOffset,
    resultExpressions,
    newMutableProjection,
    newProjection,
    newOrdering,
    inputAttributes,
    inputIter) {

  logInfo("Using SortBasedAggregationIterator.")

  override protected def newBuffer: MutableRow = {
    val bufferRowSize: Int = allAggregateFunctions.map(_.bufferSchema.length).sum
    // We use a mutable row and a mutable projection at here since we need to fill in
    // buffer values for those nonAlgebraicAggregateFunctions manually.
    val buffer = new GenericMutableRow(bufferRowSize)
    buffer
  }

  override final def hasNext: Boolean = sortedInputHasNewGroup

  override final def next(): InternalRow = {
    if (hasNext) {
      // Process the current group.
      processCurrentSortedGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput(currentGroupingKey, currentBuffer)
      // Initialize buffer values for the next group.
      initializeBuffer(currentBuffer)

      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  protected def initialize(): Unit = {
    if (inputIter.hasNext) {
      initializeBuffer(currentBuffer)
      val currentRow = inputIter.next().copy()
      // partitionGenerator is a mutable projection. Since we need to track nextGroupingKey,
      // we are making a copy at here.
      nextGroupingKey = groupGenerator(currentRow).copy()
      firstRowInNextGroup = currentRow
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  initialize()

  def generateResultForEmptyInput(): InternalRow = {
    initializeBuffer(currentBuffer)
    generateOutput(groupGenerator(new GenericInternalRow(inputAttributes.length)), currentBuffer)
  }
}
