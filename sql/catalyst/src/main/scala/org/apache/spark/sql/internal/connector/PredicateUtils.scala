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

package org.apache.spark.sql.internal.connector

import scala.collection.mutable

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.connector.expressions.{LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.sources.{AlwaysFalse, AlwaysTrue, And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.StringType

private[sql] object PredicateUtils {

  def toV1(predicate: Predicate): Option[Filter] = {

    predicate.name() match {
      case "IN" if isValidPredicate(predicate) =>
        val attribute = predicate.children()(0).toString
        val values = predicate.children().drop(1)
        if (values.length > 0) {
          val dataType = values(0).asInstanceOf[LiteralValue[_]].dataType
          val inValues = values.map(v =>
            CatalystTypeConverters.convertToScala(v.asInstanceOf[LiteralValue[_]].value, dataType))
          Some(In(attribute, inValues))
        } else {
          Some(In(attribute, Array.empty[Any]))
        }

      case "=" | "<=>" | ">" | "<" | ">=" | "<=" if isValidPredicate(predicate) =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        val v1Value = CatalystTypeConverters.convertToScala(value.value, value.dataType)
        val v1Filter = predicate.name() match {
          case "=" => EqualTo(attribute, v1Value)
          case "<=>" => EqualNullSafe(attribute, v1Value)
          case ">" => GreaterThan(attribute, v1Value)
          case ">=" => GreaterThanOrEqual(attribute, v1Value)
          case "<" => LessThan(attribute, v1Value)
          case "<=" => LessThanOrEqual(attribute, v1Value)
        }
        Some(v1Filter)

      case "IS_NULL" | "IS_NOT_NULL" if isValidPredicate(predicate) =>
        val attribute = predicate.children()(0).toString
        val v1Filter = predicate.name() match {
          case "IS_NULL" => IsNull(attribute)
          case "IS_NOT_NULL" => IsNotNull(attribute)
        }
        Some(v1Filter)

      case "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" if isValidPredicate(predicate) =>
        val attribute = predicate.children()(0).toString
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        val v1Value = value.value.toString
        val v1Filter = predicate.name() match {
          case "STARTS_WITH" =>
            StringStartsWith(attribute, v1Value)
          case "ENDS_WITH" =>
            StringEndsWith(attribute, v1Value)
          case "CONTAINS" =>
            StringContains(attribute, v1Value)
        }
        Some(v1Filter)

      case "ALWAYS_TRUE" | "ALWAYS_FALSE" if isValidPredicate(predicate) =>
        val v1Filter = predicate.name() match {
          case "ALWAYS_TRUE" => AlwaysTrue()
          case "ALWAYS_FALSE" => AlwaysFalse()
        }
        Some(v1Filter)

      case "AND" if isValidPredicate(predicate) =>
        val and = predicate.asInstanceOf[V2And]
        val left = toV1(and.left())
        val right = toV1(and.right())
        if (left.nonEmpty && right.nonEmpty) {
          Some(And(left.get, right.get))
        } else {
          None
        }

      case "OR" if isValidPredicate(predicate) =>
        val or = predicate.asInstanceOf[V2Or]
        val left = toV1(or.left())
        val right = toV1(or.right())
        if (left.nonEmpty && right.nonEmpty) {
          Some(Or(left.get, right.get))
        } else if (left.nonEmpty) {
          left
        } else {
          right
        }

      case "NOT" =>
        val child = toV1(predicate.asInstanceOf[V2Not].child())
        if (child.nonEmpty) {
          Some(Not(child.get))
        } else {
          None
        }

      case _ => None
    }
  }

  def toV1(
      predicates: Array[Predicate],
      throwExceptionIfNotConvertible: Boolean): Array[Filter] = {
    val filters = mutable.ArrayBuilder.make[Filter]
    for (predicate <- predicates) {
      val converted = toV1(predicate)
      if (converted.isEmpty) {
        if (throwExceptionIfNotConvertible) {
          throw QueryCompilationErrors.unsupportedPredicateToFilterConversionError(predicate.name())
        }
      } else {
        filters += converted.get
      }
    }
    filters.result()
  }

  def isValidPredicate(predicate: Predicate): Boolean = {

    def isValidBinaryPredicate(): Boolean = {
      if (predicate.children().length == 2 &&
        predicate.children()(0).isInstanceOf[NamedReference] &&
        predicate.children()(1).isInstanceOf[LiteralValue[_]]) {
        true
      } else {
        false
      }
    }

    predicate.name() match {
      case "=" | "<=>" | ">" | "<" | ">=" | "<=" if isValidBinaryPredicate => true

      case "STARTS_WITH" | "ENDS_WITH" | "CONTAINS" if isValidBinaryPredicate =>
        val value = predicate.children()(1).asInstanceOf[LiteralValue[_]]
        if (value.dataType.sameType(StringType)) true else false

      case "IN" if predicate.children()(0).isInstanceOf[NamedReference] =>
        val values = predicate.children().drop(1)
        if (values.length > 0) {
          if (!values.forall(_.isInstanceOf[LiteralValue[_]])) return false
          val dataType = values(0).asInstanceOf[LiteralValue[_]].dataType
          if (!values.forall(_.asInstanceOf[LiteralValue[_]].dataType.sameType(dataType))) {
            return false
          }
        }
        true

      case "IS_NULL" | "IS_NOT_NULL" if predicate.children().length == 1 &&
          predicate.children()(0).isInstanceOf[NamedReference] =>
        true

      case "ALWAYS_TRUE" | "ALWAYS_FALSE" if predicate.children().isEmpty => true

      case "AND" =>
        val and = predicate.asInstanceOf[V2And]
        isValidPredicate(and.left()) && isValidPredicate(and.right())

      case "OR" =>
        val or = predicate.asInstanceOf[V2Or]
        isValidPredicate(or.left()) || isValidPredicate(or.right())

      case _ => false
    }
  }
}
