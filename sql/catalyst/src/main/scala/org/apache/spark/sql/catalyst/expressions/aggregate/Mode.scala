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

package org.apache.spark.sql.catalyst.expressions.aggregate

import scala.collection.mutable.TreeMap

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, UnresolvedWithinGroup}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, Expression, ExpressionDescription, ImplicitCastInputTypes, SortOrder}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.{CollationFactory, GenericArrayData}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.OpenHashMap


case class Mode(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    reverseOpt: Option[Boolean] = None)
  extends TypedAggregateWithHashMapAsBuffer with ImplicitCastInputTypes
    with SupportsOrderingWithinGroup with UnaryLike[Expression] {

  final lazy val collationId: Int = child.dataType.asInstanceOf[StringType].collationId

  def this(child: Expression) = this(child, 0, 0)

  def this(child: Expression, reverse: Boolean) = {
    this(child, 0, 0, Some(reverse))
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def prettyName: String = "mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buff: OpenHashMap[AnyRef, Long]): Any = {
    if (buff.isEmpty) {
      return null
    }
    val buffer = if (isCollatedString(child)) {
      val modeMap = buff.foldLeft(
        new TreeMap[org.apache.spark.unsafe.types.UTF8String, Long]()(Ordering.comparatorToOrdering(
          CollationFactory.fetchCollation(collationId).comparator
          )))
      {
        case (map, (key: String, count)) =>
          map(org.apache.spark.unsafe.types.UTF8String.fromString(key)) =
            map.getOrElse(org.apache.spark.unsafe.types.UTF8String.fromString(key), 0L) + count
          map
        case (map, (key: UTF8String, count)) =>
          map(key) = map.getOrElse(key, 0L) + count
          map
        case (_, _) =>
          throw new IllegalArgumentException("Mode expects non-null string type.")
      }
      modeMap
    } else {
      buff
    }

    val t2 = reverseOpt.map { reverse =>
      val defaultKeyOrdering = if (reverse) {
        PhysicalDataType.ordering(child.dataType).asInstanceOf[Ordering[AnyRef]].reverse
      } else {
        PhysicalDataType.ordering(child.dataType).asInstanceOf[Ordering[AnyRef]]
      }
      val ordering = Ordering.Tuple2(Ordering.Long, defaultKeyOrdering)
      buffer.maxBy { case (key, count) => (count, key) }(ordering)
    }.getOrElse(buffer.maxBy(_._2))

    if (t2._2 == 0L) {
      null
    } else {
      t2._1
    }
  }

  private def isCollatedString(child: Expression): Boolean = {
    child.dataType match {
      case s: StringType if s.collationId != CollationFactory.UTF8_BINARY_COLLATION_ID => true
      // maybe use supportsBinaryEquality or something else
      case _ => false
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): Mode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): Mode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def sql(isDistinct: Boolean): String = {
    reverseOpt.map {
      reverse =>
        if (reverse) {
          s"$prettyName() WITHIN GROUP (ORDER BY ${child.sql} DESC)"
        } else {
          s"$prettyName() WITHIN GROUP (ORDER BY ${child.sql})"
        }
    }.getOrElse(super.sql(isDistinct))
  }

  override def orderingFilled: Boolean = child != UnresolvedWithinGroup

  assert(orderingFilled || (!orderingFilled && reverseOpt.isEmpty))

  override def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction = {
    child match {
      case UnresolvedWithinGroup =>
        if (orderingWithinGroup.length != 1) {
          throw QueryCompilationErrors.wrongNumOrderingsForInverseDistributionFunctionError(
            nodeName, 1, orderingWithinGroup.length)
        }
        orderingWithinGroup.head match {
          case SortOrder(child, Ascending, _, _) =>
            this.copy(child = child, reverseOpt = Some(true))
          case SortOrder(child, Descending, _, _) =>
            this.copy(child = child, reverseOpt = Some(false))
        }
      case _ => this
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col[, deterministic]) - Returns the most frequent value for the values within `col`. NULL values are ignored. If all the values are NULL, or there are 0 rows, returns NULL.
      When multiple values have the same greatest frequency then either any of values is returned if `deterministic` is false or is not defined, or the lowest value is returned if `deterministic` is true.
    _FUNC_() WITHIN GROUP (ORDER BY col) - Returns the most frequent value for the values within `col` (specified in ORDER BY clause). NULL values are ignored.
      If all the values are NULL, or there are 0 rows, returns NULL. When multiple values have the same greatest frequency only one value will be returned.
      The value will be chosen based on sort direction. Return the smallest value if sort direction is asc or the largest value if sort direction is desc from multiple values with the same frequency.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10) AS tab(col);
       10
      > SELECT _FUNC_(col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-10
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10), (null), (null), (null) AS tab(col);
       10
      > SELECT _FUNC_(col, false) FROM VALUES (-10), (0), (10) AS tab(col);
       0
      > SELECT _FUNC_(col, true) FROM VALUES (-10), (0), (10) AS tab(col);
       -10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10) AS tab(col);
       10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10), (20), (20) AS tab(col);
       10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col DESC) FROM VALUES (0), (10), (10), (20), (20) AS tab(col);
       20
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object ModeBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 0) {
      Mode(UnresolvedWithinGroup)
    } else if (numArgs == 1) {
      // For compatibility with function calls without WITHIN GROUP.
      Mode(expressions(0))
    } else if (numArgs == 2) {
      // For compatibility with function calls without WITHIN GROUP.
      if (!expressions(1).foldable) {
        throw QueryCompilationErrors.nonFoldableArgumentError(
          funcName, "deterministic", BooleanType)
      }
      val deterministicResult = expressions(1).eval()
      if (deterministicResult == null) {
        throw QueryCompilationErrors.unexpectedNullError("deterministic", expressions(1))
      }
      if (expressions(1).dataType != BooleanType) {
        throw QueryCompilationErrors.unexpectedInputDataTypeError(
          funcName, 2, BooleanType, expressions(1))
      }
      if (deterministicResult.asInstanceOf[Boolean]) {
        new Mode(expressions(0), true)
      } else {
        Mode(expressions(0))
      }
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(0), numArgs)
    }
  }
}

/**
 * Mode in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * It has two main difference from `Mode`:
 * 1, it accepts NULLs when `ignoreNA` is False;
 * 2, it returns all the modes for a multimodal dataset;
 */
case class PandasMode(
    child: Expression,
    ignoreNA: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedAggregateWithHashMapAsBuffer
  with ImplicitCastInputTypes with UnaryLike[Expression] {

  def this(child: Expression) = this(child, true, 0, 0)

  // Returns empty array for empty inputs
  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = !ignoreNA)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def prettyName: String = "pandas_mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    } else if (!ignoreNA) {
      buffer.changeValue(null, 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.isEmpty) {
      return new GenericArrayData(Array.empty)
    }

    val modes = collection.mutable.ArrayBuffer.empty[AnyRef]
    var maxCount = -1L
    val iter = buffer.iterator
    while (iter.hasNext) {
      val (key, count) = iter.next()
      if (maxCount < count) {
        modes.clear()
        modes.append(key)
        maxCount = count
      } else if (maxCount == count) {
        modes.append(key)
      }
    }
    new GenericArrayData(modes)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): PandasMode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): PandasMode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
