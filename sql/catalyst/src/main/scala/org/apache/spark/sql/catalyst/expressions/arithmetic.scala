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

import org.apache.spark.sql.types._

abstract class UnaryNumeric extends UnaryExpression with NumericHolder {
  self: Product =>

  type EvaluatedType = Any
  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable

  override lazy val resolved =
    child.resolved && NumericTypeChecker(child.dataType)
}

case class UnaryMinus(child: Expression) extends UnaryNumeric {
  override def toString: String = s"-$child"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.negate(evalE)
    }
  }
}

case class Sqrt(child: Expression) extends UnaryNumeric {
  override def dataType: DataType = DoubleType
  override def nullable: Boolean = true
  override def toString: String = s"SQRT($child)"

  override protected def baseType = child.dataType

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      val value = numeric.toDouble(evalE)
      if (value < 0) null
      else math.sqrt(value)
    }
  }
}

/**
 * A function that get the absolute value of the numeric value.
 */
case class Abs(child: Expression) extends UnaryNumeric {
  override def toString: String = s"Abs($child)"

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      numeric.abs(evalE)
    }
  }
}

abstract class BinaryArithmetic extends BinaryExpression
  with TypeEqualConstraint with DataTypeMerger { self: Product =>

  type EvaluatedType = Any

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
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

  def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any =
    sys.error(s"BinaryArithmetics must either override eval or evalInternal")
}

abstract class NumericBinaryArithmetic extends BinaryArithmetic {
  self: Product =>

  override protected def typeChecker =
    t => NumericTypeChecker(t) && !DecimalType.isFixed(t)
}

case class Add(left: Expression, right: Expression) extends NumericBinaryArithmetic
  with NumericHolder {

  override def symbol: String = "+"

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.plus(evalE1, evalE2)
      }
    }
  }
}

case class Subtract(left: Expression, right: Expression) extends NumericBinaryArithmetic
  with NumericHolder {

  override def symbol: String = "-"

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.minus(evalE1, evalE2)
      }
    }
  }
}

case class Multiply(left: Expression, right: Expression) extends NumericBinaryArithmetic
  with NumericHolder {

  override def symbol: String = "*"

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      null
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        null
      } else {
        numeric.times(evalE1, evalE2)
      }
    }
  }
}

case class Divide(left: Expression, right: Expression) extends NumericBinaryArithmetic {
  override def symbol: String = "/"

  override def nullable: Boolean = true

  lazy val div: (Any, Any) => Any = dataType match {
    case ft: FractionalType => ft.fractional.asInstanceOf[Fractional[Any]].div
    case it: IntegralType => it.integral.asInstanceOf[Integral[Any]].quot
    case NullType => UnresolvedIntegral.quot
  }
  
  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        div(evalE1, evalE2)
      }
    }
  }
}

case class Remainder(left: Expression, right: Expression) extends NumericBinaryArithmetic {
  override def symbol: String = "%"

  override def nullable: Boolean = true

  lazy val integral = dataType match {
    case i: IntegralType => i.integral.asInstanceOf[Integral[Any]]
    case i: FractionalType => i.asIntegral.asInstanceOf[Integral[Any]]
    case NullType => UnresolvedIntegral
  }

  override def eval(input: Row): Any = {
    val evalE2 = right.eval(input)
    if (evalE2 == null || evalE2 == 0) {
      null
    } else {
      val evalE1 = left.eval(input)
      if (evalE1 == null) {
        null
      } else {
        integral.rem(evalE1, evalE2)
      }
    }
  }
}

abstract class BitwiseBinaryArithmetic extends BinaryArithmetic {
  self: Product =>

  override protected def typeChecker = BitwiseTypeChecker
}

/**
 * A function that calculates bitwise and(&) of two numbers.
 */
case class BitwiseAnd(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def symbol: String = "&"

  lazy val and: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 & evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 & evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 & evalE2).asInstanceOf[(Any, Any) => Any]
    case NullType => UnresolvedIntegral.bitwiseAnd
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = and(evalE1, evalE2)
}

/**
 * A function that calculates bitwise or(|) of two numbers.
 */
case class BitwiseOr(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def symbol: String = "|"

  lazy val or: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 | evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 | evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 | evalE2).asInstanceOf[(Any, Any) => Any]
    case NullType => UnresolvedIntegral.bitwiseOr
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = or(evalE1, evalE2)
}

/**
 * A function that calculates bitwise xor(^) of two numbers.
 */
case class BitwiseXor(left: Expression, right: Expression) extends BitwiseBinaryArithmetic {
  override def symbol: String = "^"

  lazy val xor: (Any, Any) => Any = dataType match {
    case ByteType =>
      ((evalE1: Byte, evalE2: Byte) => (evalE1 ^ evalE2).toByte).asInstanceOf[(Any, Any) => Any]
    case ShortType =>
      ((evalE1: Short, evalE2: Short) => (evalE1 ^ evalE2).toShort).asInstanceOf[(Any, Any) => Any]
    case IntegerType =>
      ((evalE1: Int, evalE2: Int) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case LongType =>
      ((evalE1: Long, evalE2: Long) => evalE1 ^ evalE2).asInstanceOf[(Any, Any) => Any]
    case NullType => UnresolvedIntegral.bitwiseXor
  }

  override def evalInternal(evalE1: EvaluatedType, evalE2: EvaluatedType): Any = xor(evalE1, evalE2)
}

/**
 * A function that calculates bitwise not(~) of a number.
 */
case class BitwiseNot(child: Expression) extends UnaryExpression {
  type EvaluatedType = Any

  override def dataType: DataType = child.dataType
  override def foldable: Boolean = child.foldable
  override def nullable: Boolean = child.nullable
  override def toString: String = s"~$child"

  override lazy val resolved = child.resolved && BitwiseTypeChecker(child.dataType)

  lazy val not: (Any) => Any = dataType match {
    case ByteType =>
      ((evalE: Byte) => (~evalE).toByte).asInstanceOf[(Any) => Any]
    case ShortType =>
      ((evalE: Short) => (~evalE).toShort).asInstanceOf[(Any) => Any]
    case IntegerType =>
      ((evalE: Int) => ~evalE).asInstanceOf[(Any) => Any]
    case LongType =>
      ((evalE: Long) => ~evalE).asInstanceOf[(Any) => Any]
    case NullType => UnresolvedIntegral.bitwiseNot
  }

  override def eval(input: Row): Any = {
    val evalE = child.eval(input)
    if (evalE == null) {
      null
    } else {
      not(evalE)
    }
  }
}

abstract class OrderingBinaryArithmetic extends BinaryArithmetic with OrderingHolder {
  self: Product =>

  override def nullable: Boolean = left.nullable && right.nullable

  override protected def typeChecker = OrderingTypeChecker
}

case class MaxOf(left: Expression, right: Expression) extends OrderingBinaryArithmetic {
  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE2
      } else {
        evalE1
      }
    }
  }

  override def toString: String = s"MaxOf($left, $right)"
}

case class MinOf(left: Expression, right: Expression) extends OrderingBinaryArithmetic {
  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    val evalE2 = right.eval(input)
    if (evalE1 == null) {
      evalE2
    } else if (evalE2 == null) {
      evalE1
    } else {
      if (ordering.compare(evalE1, evalE2) < 0) {
        evalE1
      } else {
        evalE2
      }
    }
  }

  override def toString: String = s"MinOf($left, $right)"
}
