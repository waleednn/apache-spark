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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType}

object InterpretedPredicate {
  def create(expression: Expression, inputSchema: Seq[Attribute]): (Row => Boolean) =
    create(BindReferences.bindReference(expression, inputSchema))

  def create(expression: Expression): (Row => Boolean) = {
    (r: Row) => expression.eval(r).asInstanceOf[Boolean]
  }
}

trait Predicate extends Expression {
  self: Product =>

  override def dataType: DataType = BooleanType
}

trait PredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  protected def splitDisjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case Or(cond1, cond2) =>
        splitDisjunctivePredicates(cond1) ++ splitDisjunctivePredicates(cond2)
      case other => other :: Nil
    }
  }

  /**
   * Returns true if `expr` can be evaluated using only the output of `plan`.  This method
   * can be used to determine when is is acceptable to move expression evaluation within a query
   * plan.
   *
   * For example consider a join between two relations R(a, b) and S(c, d).
   *
   * `canEvaluate(EqualTo(a,b), R)` returns `true` where as `canEvaluate(EqualTo(a,c), R)` returns
   * `false`.
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)
}


case class Not(child: Expression) extends UnaryExpression with Predicate with ExpectsInputTypes {
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"NOT $child"

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType)

  override def eval(input: Row): Any = {
    child.eval(input) match {
      case null => null
      case b: Boolean => !b
    }
  }
}

/**
 * Evaluates to `true` if `list` contains `value`.
 */
case class In(value: Expression, list: Seq[Expression]) extends Predicate {
  override def children: Seq[Expression] = value +: list

  override def nullable: Boolean = true // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: Row): Any = {
    val evaluatedValue = value.eval(input)
    list.exists(e => e.eval(input) == evaluatedValue)
  }
}

/**
 * Optimized version of In clause, when all filter values of In clause are
 * static.
 */
case class InSet(value: Expression, hset: Set[Any])
  extends Predicate {

  override def children: Seq[Expression] = value :: Nil

  override def foldable: Boolean = value.foldable
  override def nullable: Boolean = true // TODO: Figure out correct nullability semantics of IN.
  override def toString: String = s"$value INSET ${hset.mkString("(", ",", ")")}"

  override def eval(input: Row): Any = {
    hset.contains(value.eval(input))
  }
}

case class And(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)

  override def symbol: String = "&&"

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == false) {
       false
    } else {
      val r = right.eval(input)
      if (r == false) {
        false
      } else {
        if (l != null && r != null) {
          true
        } else {
          null
        }
      }
    }
  }
}

case class Or(left: Expression, right: Expression)
  extends BinaryExpression with Predicate with ExpectsInputTypes {

  override def expectedChildTypes: Seq[DataType] = Seq(BooleanType, BooleanType)

  override def symbol: String = "||"

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == true) {
      true
    } else {
      val r = right.eval(input)
      if (r == true) {
        true
      } else {
        if (l != null && r != null) {
          false
        } else {
          null
        }
      }
    }
  }
}

abstract class BinaryComparison extends BinaryExpression with Predicate {
  self: Product =>

  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType != right.dataType) {
      TypeCheckResult.fail(
        s"differing types in ${this.getClass.getSimpleName} " +
        s"(${left.dataType} and ${right.dataType}).")
    } else {
      TypeUtils.checkForOrderingExpr(left.dataType, "operator " + symbol)
    }
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if (evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        evalInternal(evalE1, evalE2)
      }
    }
  }

  protected def evalInternal(evalE1: Any, evalE2: Any): Any =
    sys.error(s"BinaryComparisons must either override eval or evalInternal")
}

case class EqualTo(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "="

  // EqualTo don't need 2 equal orderable types
  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.success

  protected override def evalInternal(l: Any, r: Any) = {
    if (left.dataType != BinaryType) l == r
    else java.util.Arrays.equals(l.asInstanceOf[Array[Byte]], r.asInstanceOf[Array[Byte]])
  }
}

case class EqualNullSafe(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<=>"
  override def nullable: Boolean = false

  // EqualNullSafe don't need 2 equal orderable types
  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.success

  override def eval(input: Row): Any = {
    val l = left.eval(input)
    val r = right.eval(input)
    if (l == null && r == null) {
      true
    } else if (l == null || r == null) {
      false
    } else {
      l == r
    }
  }
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<"

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.lt(evalE1, evalE2)
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = "<="

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.lteq(evalE1, evalE2)
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">"

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.gt(evalE1, evalE2)
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  override def symbol: String = ">="

  private lazy val ordering = TypeUtils.getOrdering(left.dataType)

  protected override def evalInternal(evalE1: Any, evalE2: Any) = ordering.gteq(evalE1, evalE2)
}

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.fail(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (trueValue.dataType != falseValue.dataType) {
      TypeCheckResult.fail(
        s"differing types in If (${trueValue.dataType} and ${falseValue.dataType}).")
    } else {
      TypeCheckResult.success
    }
  }

  override def dataType: DataType = trueValue.dataType

  override def eval(input: Row): Any = {
    if (true == predicate.eval(input)) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"
}

trait CaseWhenLike extends Expression {
  self: Product =>

  // Note that `branches` are considered in consecutive pairs (cond, val), and the optional last
  // element is the value for the default catch-all case (if provided).
  // Hence, `branches` consists of at least two elements, and can have an odd or even length.
  def branches: Seq[Expression]

  @transient lazy val whenList =
    branches.sliding(2, 2).collect { case Seq(whenExpr, _) => whenExpr }.toSeq
  @transient lazy val thenList =
    branches.sliding(2, 2).collect { case Seq(_, thenExpr) => thenExpr }.toSeq
  val elseValue = if (branches.length % 2 == 0) None else Option(branches.last)

  // both then and else val should be considered.
  def valueTypes: Seq[DataType] = (thenList ++ elseValue).map(_.dataType)
  def valueTypesEqual: Boolean = valueTypes.distinct.size == 1

  override def dataType: DataType = valueTypes.head

  override def nullable: Boolean = {
    // If no value is nullable and no elseValue is provided, the whole statement defaults to null.
    thenList.exists(_.nullable) || (elseValue.map(_.nullable).getOrElse(true))
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = branches

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!whenList.forall(_.dataType == BooleanType)) {
      TypeCheckResult.fail(s"WHEN expressions should all be boolean type")
    } else if (!valueTypesEqual) {
      TypeCheckResult.fail(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    } else {
      TypeCheckResult.success
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (branchesArr(i).eval(input) == true) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  override def toString: String = {
    "CASE" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseKeyWhen(key: Expression, branches: Seq[Expression]) extends CaseWhenLike {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!valueTypesEqual) {
      TypeCheckResult.fail(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    } else {
      TypeCheckResult.success
    }
  }

  /** Written in imperative fashion for performance considerations. */
  override def eval(input: Row): Any = {
    val evaluatedKey = key.eval(input)
    val len = branchesArr.length
    var i = 0
    // If all branches fail and an elseVal is not provided, the whole statement
    // defaults to null, according to Hive's semantics.
    while (i < len - 1) {
      if (equalNullSafe(evaluatedKey, branchesArr(i).eval(input))) {
        return branchesArr(i + 1).eval(input)
      }
      i += 2
    }
    var res: Any = null
    if (i == len - 1) {
      res = branchesArr(i).eval(input)
    }
    return res
  }

  private def equalNullSafe(l: Any, r: Any) = {
    if (l == null && r == null) {
      true
    } else if (l == null || r == null) {
      false
    } else {
      l == r
    }
  }

  override def toString: String = {
    s"CASE $key" + branches.sliding(2, 2).map {
      case Seq(cond, value) => s" WHEN $cond THEN $value"
      case Seq(elseValue) => s" ELSE $elseValue"
    }.mkString
  }
}
