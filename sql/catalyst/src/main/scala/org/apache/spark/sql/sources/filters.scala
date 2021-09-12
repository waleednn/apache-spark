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

package org.apache.spark.sql.sources

import java.sql.{Date, Timestamp}
import java.time.Instant

import org.apache.spark.annotation.{Evolving, Stable}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse => V2AlwaysFalse, AlwaysTrue => V2AlwaysTrue, And => V2And, EqualNullSafe => V2EqualNullSafe, EqualTo => V2EqualTo, Filter => V2Filter, GreaterThan => V2GreaterThan, GreaterThanOrEqual => V2GreaterThanOrEqual, In => V2In, IsNotNull => V2IsNotNull, IsNull => V2IsNull, LessThan => V2LessThan, LessThanOrEqual => V2LessThanOrEqual, Not => V2Not, Or => V2Or, StringContains => V2StringContains, StringEndsWith => V2StringEndsWith, StringStartsWith => V2StringStartsWith}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

////////////////////////////////////////////////////////////////////////////////////////////////////
// This file defines all the filters that we can push down to the data sources.
////////////////////////////////////////////////////////////////////////////////////////////////////

/**
 * A filter predicate for data sources. Mapping between Spark SQL types and filter value
 * types follow the convention for return type of [[org.apache.spark.sql.Row#get(int)]].
 *
 * @since 1.3.0
 */
@Stable
sealed abstract class Filter {
  /**
   * List of columns that are referenced by this filter.
   *
   * Note that, each element in `references` represents a column. The column name follows ANSI SQL
   * names and identifiers: dots are used as separators for nested columns, name will be quoted if
   * it contains special chars.
   *
   * @since 2.1.0
   */
  def references: Array[String]

  private[sql] def toV2: V2Filter

  private[sql] def getLiteralValue(value: Any): LiteralValue[_] = value match {
    case _: java.math.BigDecimal => LiteralValue(value, DecimalType.SYSTEM_DEFAULT)
    case _: Boolean => LiteralValue(value, BooleanType)
    case _: Byte => LiteralValue(value, ByteType)
    case _: Array[Byte] => LiteralValue(value, BinaryType)
    case _: Date => LiteralValue(value, DateType)
    case _: java.time.LocalDate => LiteralValue(value, DateType)
    case _: Double => LiteralValue(value, DoubleType)
    case _: Float => LiteralValue(value, FloatType)
    case _: Integer => LiteralValue(value, IntegerType)
    case _: Long => LiteralValue(value, LongType)
    case _: Short => LiteralValue(value, ShortType)
    case _: String => LiteralValue(UTF8String.fromString(value.toString), StringType)
    case _: Timestamp => LiteralValue(value, TimestampType)
    case _: Instant => LiteralValue(value, TimestampType)
    case _ =>
      throw QueryCompilationErrors.invalidDataTypeForFilterValue(value)
  }

  protected def findReferences(value: Any): Array[String] = value match {
    case f: Filter => f.references
    case _ => Array.empty
  }

  /**
   * List of columns that are referenced by this filter.
   *
   * @return each element is a column name as an array of string multi-identifier
   * @since 3.0.0
   */
  def v2references: Array[Array[String]] = {
    this.references.map(parseColumnPath(_).toArray)
  }

  /**
   * If any of the references of this filter contains nested column
   */
  private[sql] def containsNestedColumn: Boolean = {
    this.v2references.exists(_.length > 1)
  }
}

/**
 * A filter that evaluates to `true` iff the column evaluates to a value
 * equal to `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class EqualTo(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 = {
    new V2EqualTo(FieldReference(attribute), getLiteralValue(value))
  }
}

/**
 * Performs equality comparison, similar to [[EqualTo]]. However, this differs from [[EqualTo]]
 * in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
 * (rather than NULL) if one of the input is NULL and the other is not NULL.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.5.0
 */
@Stable
case class EqualNullSafe(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 =
    new V2EqualNullSafe(FieldReference(attribute), getLiteralValue(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class GreaterThan(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 =
    new V2GreaterThan(FieldReference(attribute), getLiteralValue(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * greater than or equal to `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class GreaterThanOrEqual(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 =
    new V2GreaterThanOrEqual(FieldReference(attribute), getLiteralValue(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class LessThan(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 =
    new V2LessThan(FieldReference(attribute), getLiteralValue(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a value
 * less than or equal to `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class LessThanOrEqual(attribute: String, value: Any) extends Filter {
  override def references: Array[String] = Array(attribute) ++ findReferences(value)
  override private[sql] def toV2 =
    new V2LessThanOrEqual(FieldReference(attribute), getLiteralValue(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to one of the values in the array.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class In(attribute: String, values: Array[Any]) extends Filter {
  override def hashCode(): Int = {
    var h = attribute.hashCode
    values.foreach { v =>
      h *= 41
      h += (if (v != null) v.hashCode() else 0)
    }
    h
  }
  override def equals(o: Any): Boolean = o match {
    case In(a, vs) =>
      a == attribute && vs.length == values.length && vs.zip(values).forall(x => x._1 == x._2)
    case _ => false
  }
  private def formatValue(v: Any): String = v match {
    case null => "null"
    case ar: Seq[Any] => ar.map(formatValue).mkString("[", ", ", "]")
    case _ => v.toString
  }
  override def toString: String = {
    // Sort elements for deterministic behaviours
    s"In($attribute, [${values.map(formatValue).sorted.mkString(",")}])"
  }

  override def references: Array[String] = Array(attribute) ++ values.flatMap(findReferences)
  override private[sql] def toV2 =
    new V2In(FieldReference(attribute),
      values.map(value => getLiteralValue(value)))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to null.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class IsNull(attribute: String) extends Filter {
  override def references: Array[String] = Array(attribute)
  override private[sql] def toV2 = new V2IsNull(FieldReference(attribute))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to a non-null value.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.0
 */
@Stable
case class IsNotNull(attribute: String) extends Filter {
  override def references: Array[String] = Array(attribute)
  override private[sql] def toV2 = new V2IsNotNull(FieldReference(attribute))
}

/**
 * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
 *
 * @since 1.3.0
 */
@Stable
case class And(left: Filter, right: Filter) extends Filter {
  override def references: Array[String] = left.references ++ right.references
  override private[sql] def toV2 = new V2And(left.toV2, right.toV2)
}

/**
 * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
 *
 * @since 1.3.0
 */
@Stable
case class Or(left: Filter, right: Filter) extends Filter {
  override def references: Array[String] = left.references ++ right.references
  override private[sql] def toV2 = new V2Or(left.toV2, right.toV2)
}

/**
 * A filter that evaluates to `true` iff `child` is evaluated to `false`.
 *
 * @since 1.3.0
 */
@Stable
case class Not(child: Filter) extends Filter {
  override def references: Array[String] = child.references
  override private[sql] def toV2 = new V2Not(child.toV2)
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that starts with `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.1
 */
@Stable
case class StringStartsWith(attribute: String, value: String) extends Filter {
  override def references: Array[String] = Array(attribute)
  override private[sql] def toV2 = new V2StringStartsWith(FieldReference(attribute),
    UTF8String.fromString(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that ends with `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.1
 */
@Stable
case class StringEndsWith(attribute: String, value: String) extends Filter {
  override def references: Array[String] = Array(attribute)
  override private[sql] def toV2 = new V2StringEndsWith(FieldReference(attribute),
    UTF8String.fromString(value))
}

/**
 * A filter that evaluates to `true` iff the attribute evaluates to
 * a string that contains the string `value`.
 *
 * @param attribute of the column to be evaluated; `dots` are used as separators
 *                  for nested columns. If any part of the names contains `dots`,
 *                  it is quoted to avoid confusion.
 * @since 1.3.1
 */
@Stable
case class StringContains(attribute: String, value: String) extends Filter {
  override def references: Array[String] = Array(attribute)
  override private[sql] def toV2 = new V2StringContains(FieldReference(attribute),
    UTF8String.fromString(value))
}

/**
 * A filter that always evaluates to `true`.
 *
 * @since 3.0.0
 */
@Evolving
case class AlwaysTrue() extends Filter {
  override def references: Array[String] = Array.empty
  override private[sql] def toV2 = new V2AlwaysTrue()
}

@Evolving
object AlwaysTrue extends AlwaysTrue {
}

/**
 * A filter that always evaluates to `false`.
 *
 * @since 3.0.0
 */
@Evolving
case class AlwaysFalse() extends Filter {
  override def references: Array[String] = Array.empty
  override private[sql] def toV2 = new V2AlwaysFalse()
}

@Evolving
object AlwaysFalse extends AlwaysFalse {
}
