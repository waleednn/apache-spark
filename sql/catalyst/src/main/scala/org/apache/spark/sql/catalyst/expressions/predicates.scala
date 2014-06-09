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

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.types.BooleanType


object InterpretedPredicate {
  def apply(expression: Expression): (Row => Boolean) = {
    (r: Row) => expression.eval(r).asInstanceOf[Boolean]
  }
}

trait Predicate extends Expression {
  self: Product =>

  def dataType = BooleanType

  type EvaluatedType = Any
}

trait PredicateHelper {
  protected def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
    condition match {
      case And(cond1, cond2) =>
        splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
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
   * `canEvaluate(Equals(a,b), R)` returns `true` where as `canEvaluate(Equals(a,c), R)` returns
   * `false`.
   */
  protected def canEvaluate(expr: Expression, plan: LogicalPlan): Boolean =
    expr.references.subsetOf(plan.outputSet)
}

abstract class BinaryPredicate extends BinaryExpression with Predicate {
  self: Product =>
  def nullable = left.nullable || right.nullable
}

case class Not(child: Expression) extends UnaryExpression with Predicate {
  override def foldable = child.foldable
  def nullable = child.nullable
  override def toString = s"NOT $child"

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
  def children = value +: list
  def references = children.flatMap(_.references).toSet
  def nullable = true // TODO: Figure out correct nullability semantics of IN.
  override def toString = s"$value IN ${list.mkString("(", ",", ")")}"

  override def eval(input: Row): Any = {
    val evaluatedValue = value.eval(input)
    list.exists(e => e.eval(input) == evaluatedValue)
  }
}

case class And(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "&&"

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

case class Or(left: Expression, right: Expression) extends BinaryPredicate {
  def symbol = "||"

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

abstract class BinaryComparison extends BinaryPredicate {
  self: Product =>
}

case class Equals(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "="
  override def eval(input: Row): Any = {
    val l = left.eval(input)
    if (l == null) {
      null
    } else {
      val r = right.eval(input)
      if (r == null) null else l == r
    }
  }
}

case class LessThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<"
  override def eval(input: Row): Any = c2(input, left, right, _.lt(_, _))
}

case class LessThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = "<="
  override def eval(input: Row): Any = c2(input, left, right, _.lteq(_, _))
}

case class GreaterThan(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">"
  override def eval(input: Row): Any = c2(input, left, right, _.gt(_, _))
}

case class GreaterThanOrEqual(left: Expression, right: Expression) extends BinaryComparison {
  def symbol = ">="
  override def eval(input: Row): Any = c2(input, left, right, _.gteq(_, _))
}

case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    extends Expression {

  def children = predicate :: trueValue :: falseValue :: Nil
  override def nullable = trueValue.nullable || falseValue.nullable
  def references = children.flatMap(_.references).toSet
  override lazy val resolved = childrenResolved && trueValue.dataType == falseValue.dataType
  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(
        this,
        s"Can not resolve due to differing types ${trueValue.dataType}, ${falseValue.dataType}")
    }
    trueValue.dataType
  }

  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    if (true == predicate.eval(input)) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def toString = s"if ($predicate) $trueValue else $falseValue"
}

// TODO: is it a good idea to put this class in this file?
// CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END
// When a = true, returns b; when c = true, return d; else return e
case class Case(key: Option[Expression], branches: Seq[Expression]) extends Expression {
  // Branches are considered in consecutive pairs (cond, val), and the last element
  // is the val for the default catch-all case (w/o a companion condition, that is).

  def children = key.toSeq ++ branches

  override def nullable = branches
    .sliding(2, 2)
    .map {
      case Seq(cond, value) => value.nullable
      case Seq(elseValue) => elseValue.nullable
    }
    .reduce(_ || _)

  def references = children.flatMap(_.references).toSet

  override lazy val resolved = {
    val allBranchesEqual = branches.sliding(2, 2).map {
      case Seq(cond, value) => value.dataType
      case Seq(elseValue) => elseValue.dataType
    }.reduce(_ == _)
    childrenResolved && allBranchesEqual
  }

  def dataType = {
    if (!resolved) {
      throw new UnresolvedException(this, "cannot resolve due to differing types in some branches")
    }
    branches(1).dataType
  }

  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    def slidingCheck(expectedVal: Any): Any = {
      branches.sliding(2, 2).foldLeft(None.asInstanceOf[Option[Any]]) {
        case (Some(x), _) =>
          Some(x)
        case (None, Seq(cond, value)) =>
          if (cond.eval(input) == true) Some(value.eval(input)) else None
        case (None, Seq(elseValue)) =>
          Some(elseValue.eval(input))
      }.getOrElse(null)
      // If all branches fail and an elseVal is not provided, the whole statement
      // evaluates to null, according to Hive's semantics.
    }
    // Check if any branch's cond evaluates either to the key (if provided), or to true.
    if (key.isDefined) {
      slidingCheck(key.get.eval(input))
    } else {
      slidingCheck(true)
    }
  }

  override def toString = {
    var firstBranch = ""
    var otherBranches = ""
    if (key.isDefined) {
      val keyString = key.get.toString
      firstBranch = s"if ($keyString == ${branches(0)}) { ${branches(1)} }"
      otherBranches = branches.sliding(2, 2).drop(1).map {
        case Seq(cond, value) => s"\nelse if ($keyString == $cond) { $value }"
        case Seq(elseValue) => s"\nelse { $elseValue }"
      }.mkString
    } else {
      firstBranch = s"if (${branches(0)}) { ${branches(1)} }"
      otherBranches = branches.sliding(2, 2).drop(1).map {
        case Seq(cond, value) => s"\nelse if ($cond) { $value }"
        case Seq(elseValue) => s"\nelse { $elseValue }"
      }.mkString
    }
    firstBranch ++ otherBranches
  }
}
