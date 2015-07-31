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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.{UnspecifiedDistribution, ClusteredDistribution, AllTuples, Distribution}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.types.StructType

/**
 * An Aggregate Operator used to evaluate [[AggregateFunction2]]. Based on the data types
 * of the grouping expressions and aggregate functions, it determines if it uses
 * sort-based aggregation and hybrid (hash-based with sort-based as the fallback) to
 * process input rows.
 */
case class Aggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    unsafeEnabled: Boolean,
    child: SparkPlan)
  extends UnaryNode {

  private[this] val allAggregateExpressions =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  // Use the hybrid iterator if (1) unsafe is enabled, (2) the schemata of
  // grouping key and aggregation buffer is supported; and (3) all
  // aggregate functions are algebraic.
  private[this] val useHybridIterator: Boolean = {
    val aggregationBufferSchema: StructType =
      StructType.fromAttributes(
        allAggregateExpressions.flatMap(_.aggregateFunction.bufferAttributes))
    val groupKeySchema: StructType =
    StructType.fromAttributes(groupingExpressions.map(_.toAttribute))
    val schemaSupportsUnsafe: Boolean =
      UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema) &&
        UnsafeProjection.canSupport(groupKeySchema)

    val allAlgebraicAggregates =
      allAggregateExpressions.forall(_.aggregateFunction.isInstanceOf[AlgebraicAggregate])

    unsafeEnabled && schemaSupportsUnsafe && allAlgebraicAggregates
  }

  // We need to use sorted input if we have grouping expressions and
  // we cannot use the hybrid iterator.
  private[this] val requireSortedInput: Boolean = {
    groupingExpressions.nonEmpty && !useHybridIterator
  }

  override def canProcessUnsafeRows: Boolean = true

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (requireSortedInput) {
      // TODO: We should not sort the input rows if they are just in reversed order.
      groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
    } else {
      Seq.fill(children.size)(Nil)
    }
  }

  override def outputOrdering: Seq[SortOrder] = {
    if (requireSortedInput) {
      // It is possible that the child.outputOrdering starts with the required
      // ordering expressions (e.g. we require [a] as the sort expression and the
      // child's outputOrdering is [a, b]). We can only guarantee the output rows
      // are sorted by values of groupingExpressions.
      groupingExpressions.map(SortOrder(_, Ascending))
    } else {
      Nil
    }
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      if (iter.hasNext && useHybridIterator && groupingExpressions.nonEmpty) {
        new UnsafeHybridAggregationIterator(
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
          child.output,
          iter)
      } else {
        if (!iter.hasNext && groupingExpressions.nonEmpty) {
          // This is a grouped aggregate and the input iterator is empty,
          // so return an empty iterator.
          Iterator[InternalRow]()
        } else {
          val outputIter = new SortBasedAggregationIterator(
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
            child.output,
            iter)
          if (!iter.hasNext && groupingExpressions.isEmpty) {
            Iterator[InternalRow](outputIter.generateResultForEmptyInput())
            // outputIter
          } else {
            outputIter
          }
        }
      }
    }
  }

  override def simpleString: String = {
    val iterator = if (useHybridIterator && groupingExpressions.nonEmpty) {
      classOf[UnsafeHybridAggregationIterator].getSimpleName
    } else {
      classOf[SortBasedAggregationIterator].getSimpleName
    }

    s"""NewAggregate with $iterator ${groupingExpressions} ${allAggregateExpressions}"""
  }
}
