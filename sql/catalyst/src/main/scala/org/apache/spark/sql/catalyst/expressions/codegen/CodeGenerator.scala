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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.language.existentials

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types._

/**
 * A base class for generators of byte code that performs expression evaluation.  Includes helpers
 * for refering to Catalyst types and building trees that perform evaluation of individual
 * expressions.
 */
abstract class CodeGenerator extends Logging {
  import scala.reflect.runtime.{universe => ru}
  import scala.reflect.runtime.universe._

  import scala.tools.reflect.ToolBox

  val toolBox = runtimeMirror(getClass.getClassLoader).mkToolBox()

  // TODO: Use typetags?
  val rowType = tq"org.apache.spark.sql.catalyst.expressions.Row"
  val mutableRowType = tq"org.apache.spark.sql.catalyst.expressions.MutableRow"
  val genericRowType = tq"org.apache.spark.sql.catalyst.expressions.GenericRow"
  val genericMutableRowType = tq"org.apache.spark.sql.catalyst.expressions.GenericMutableRow"

  val projectionType = tq"org.apache.spark.sql.catalyst.expressions.Projection"
  val mutableProjectionType = tq"org.apache.spark.sql.catalyst.expressions.MutableProjection"

  private val curId = new java.util.concurrent.atomic.AtomicInteger()
  private val javaSeperator = "$"

  /**
   * Returns a term name that is unique within this instance of a `CodeGenerator`.
   *
   * (Since we aren't in a macro context we do not seem to have access to the built in `freshName`
   * function.)
   */
  protected def freshName(prefix: String): TermName = {
    newTermName(s"$prefix$javaSeperator${curId.getAndIncrement}")
  }

  /**
   * Scala ASTs for evaluating an [[Expression]] given a [[Row]] of input.
   *
   * @param code The sequence of statements required to evaluate the expression.
   * @param nullTerm A term that holds a boolean value representing whether the expression evaluated
   *                 to null.
   * @param primitiveTerm A term for a possible primitive value of the result of the evaluation. Not
   *                      valid if `nullTerm` is set to `false`.
   * @param objectTerm An possibly boxed version of the result of evaluating this expression.
   */
  protected case class EvaluatedExpression(
                                            code: Seq[Tree],
                                            nullTerm: TermName,
                                            primitiveTerm: TermName,
                                            objectTerm: TermName) {

    def withObjectTerm = ???
  }

  /**
   * Given an expression tree returns the code required to determine both if the result is NULL
   * as well as the code required to compute the value.
   */
  def expressionEvaluator(e: Expression): EvaluatedExpression = {
    val primitiveTerm = freshName("primitiveTerm")
    val nullTerm = freshName("nullTerm")
    val objectTerm = freshName("objectTerm")

    implicit class Evaluate1(e: Expression) {
      def castOrNull(f: TermName => Tree, dataType: DataType): Seq[Tree] = {
        val eval = expressionEvaluator(e)
        eval.code ++
          q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(dataType)}
            else
              ${f(eval.primitiveTerm)}
        """.children
      }
    }

    implicit class Evaluate2(expressions: (Expression, Expression)) {

      /**
       * Short hand for generating binary evaluation code, which depends on two sub-evaluations of
       * the same type.  If either of the sub-expressions is null, the results of this computation
       * is assumed to be null.
       *
       * @param f a function from two primitive term names to a tree that evaluates them.
       */
      def evaluate(f: (TermName, TermName) => Tree): Seq[Tree] =
        evaluateAs(expressions._1.dataType)(f)

      def evaluateAs(resultType: DataType)(f: (TermName, TermName) => Tree): Seq[Tree] = {
        require(expressions._1.dataType == expressions._2.dataType,
          s"${expressions._1.dataType} != ${expressions._2.dataType}")

        val eval1 = expressionEvaluator(expressions._1)
        val eval2 = expressionEvaluator(expressions._2)
        val resultCode = f(eval1.primitiveTerm, eval2.primitiveTerm)

        eval1.code ++ eval2.code ++
          q"""
          val $nullTerm = ${eval1.nullTerm} || ${eval2.nullTerm}
          val $primitiveTerm: ${termForType(resultType)} =
            if($nullTerm) {
              ${defaultPrimitive(resultType)}
            } else {
              $resultCode.asInstanceOf[${termForType(resultType)}]
            }
        """.children : Seq[Tree]
      }
    }

    val inputTuple = newTermName(s"i")

    // TODO: Skip generation of null handling code when expression are not nullable.
    val primitiveEvaluation: PartialFunction[Expression, Seq[Tree]] = {
      case b @ BoundReference(ordinal, _) =>
        q"""
          val $nullTerm: Boolean = $inputTuple.isNullAt($ordinal)
          val $primitiveTerm: ${termForType(b.dataType)} =
            if($nullTerm)
              ${defaultPrimitive(e.dataType)}
            else
              ${getColumn(inputTuple, b.dataType, ordinal)}
         """.children

      case expressions.Literal(null, dataType) =>
        q"""
          val $nullTerm = true
          val $primitiveTerm: ${termForType(dataType)} = null.asInstanceOf[${termForType(dataType)}]
         """.children

      case expressions.Literal(value: Boolean, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case expressions.Literal(value: String, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children
      case expressions.Literal(value: Int, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children
      case expressions.Literal(value: Long, dataType) =>
        q"""
          val $nullTerm = ${value == null}
          val $primitiveTerm: ${termForType(dataType)} = $value
         """.children

      case Cast(e @ BinaryType(), StringType) =>
        val eval = expressionEvaluator(e)
        eval.code ++
          q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              new String(${eval.primitiveTerm}.asInstanceOf[Array[Byte]])
        """.children

      case Cast(child @ NumericType(), IntegerType) =>
        child.castOrNull(c => q"$c.toInt", IntegerType)

      case Cast(child @ NumericType(), LongType) =>
        child.castOrNull(c => q"$c.toLong", LongType)

      case Cast(child @ NumericType(), DoubleType) =>
        child.castOrNull(c => q"$c.toDouble", DoubleType)

      case Cast(child @ NumericType(), FloatType) =>
        child.castOrNull(c => q"$c.toFloat", IntegerType)

      case Cast(e, StringType) =>
        val eval = expressionEvaluator(e)
        eval.code ++
          q"""
          val $nullTerm = ${eval.nullTerm}
          val $primitiveTerm =
            if($nullTerm)
              ${defaultPrimitive(StringType)}
            else
              ${eval.primitiveTerm}.toString
        """.children

      case EqualTo(e1, e2) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 == $eval2" }

      /* TODO: Fix null semantics.
      case In(e1, list) if !list.exists(!_.isInstanceOf[expressions.Literal]) =>
        val eval = expressionEvaluator(e1)

        val checks = list.map {
          case expressions.Literal(v: String, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
          case expressions.Literal(v: Int, dataType) =>
            q"if(${eval.primitiveTerm} == $v) return true"
        }

        val funcName = newTermName(s"isIn${curId.getAndIncrement()}")

        q"""
            def $funcName: Boolean = {
              ..${eval.code}
              if(${eval.nullTerm}) return false
              ..$checks
              return false
            }
            val $nullTerm = false
            val $primitiveTerm = $funcName
        """.children
      */

      case GreaterThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 > $eval2" }
      case GreaterThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 >= $eval2" }
      case LessThan(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 < $eval2" }
      case LessThanOrEqual(e1 @ NumericType(), e2 @ NumericType()) =>
        (e1, e2).evaluateAs (BooleanType) { case (eval1, eval2) => q"$eval1 <= $eval2" }

      case And(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
          q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if ((!${eval1.nullTerm} && !${eval1.primitiveTerm}) ||
              (!${eval2.nullTerm} && !${eval2.primitiveTerm})) {
            $nullTerm = false
            $primitiveTerm = false
          } else if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else {
            $nullTerm = false
            $primitiveTerm = true
          }
         """.children

      case Or(e1, e2) =>
        val eval1 = expressionEvaluator(e1)
        val eval2 = expressionEvaluator(e2)

        eval1.code ++ eval2.code ++
          q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = false

          if ((!${eval1.nullTerm} && ${eval1.primitiveTerm}) ||
              (!${eval2.nullTerm} && ${eval2.primitiveTerm})) {
            $nullTerm = false
            $primitiveTerm = true
          } else if (${eval1.nullTerm} || ${eval2.nullTerm} ) {
            $nullTerm = true
          } else {
            $nullTerm = false
            $primitiveTerm = false
          }
         """.children

      case Not(child) =>
        // Uh, bad function name...
        child.castOrNull(c => q"!$c", BooleanType)

      case Add(e1, e2) =>      (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 + $eval2" }
      case Subtract(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 - $eval2" }
      case Multiply(e1, e2) => (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 * $eval2" }
      case Divide(e1, e2) =>   (e1, e2) evaluate { case (eval1, eval2) => q"$eval1 / $eval2" }

      case IsNotNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = !${eval.nullTerm}
        """.children

      case IsNull(e) =>
        val eval = expressionEvaluator(e)
        q"""
          ..${eval.code}
          var $nullTerm = false
          var $primitiveTerm: ${termForType(BooleanType)} = ${eval.nullTerm}
        """.children

      case c @ Coalesce(children) =>
        q"""
          var $nullTerm = true
          var $primitiveTerm: ${termForType(c.dataType)} = ${defaultPrimitive(c.dataType)}
        """.children ++
          children.map { c =>
            val eval = expressionEvaluator(c)
            q"""
            if($nullTerm) {
              ..${eval.code}
              if(!${eval.nullTerm}) {
                $nullTerm = false
                $primitiveTerm = ${eval.primitiveTerm}
              }
            }
           """
          }

      case i @ expressions.If(condition, trueValue, falseValue) =>
        val condEval = expressionEvaluator(condition)
        val trueEval = expressionEvaluator(trueValue)
        val falseEval = expressionEvaluator(falseValue)

        q"""
          var $nullTerm = false
          var $primitiveTerm: ${termForType(i.dataType)} = ${defaultPrimitive(i.dataType)}
          ..${condEval.code}
          if(!${condEval.nullTerm} && ${condEval.primitiveTerm}) {
            ..${trueEval.code}
            $nullTerm = ${trueEval.nullTerm}
            $primitiveTerm = ${trueEval.primitiveTerm}
          } else {
            ..${falseEval.code}
            $nullTerm = ${falseEval.nullTerm}
            $primitiveTerm = ${falseEval.primitiveTerm}
          }
        """.children
    }

    // If there was no match in the partial function above, we fall back on calling the interpreted
    // expression evaluator.
    val code: Seq[Tree] =
      primitiveEvaluation.lift.apply(e)
        .getOrElse {
        log.debug(s"No rules to generate $e")
        val tree = reify { e }
        q"""
            val $objectTerm = $tree.eval(i)
            val $nullTerm = $objectTerm == null
            val $primitiveTerm = $objectTerm.asInstanceOf[${termForType(e.dataType)}]
          """.children
      }

    EvaluatedExpression(code, nullTerm, primitiveTerm, objectTerm)
  }

  protected def getColumn(inputRow: TermName, dataType: DataType, ordinal: Int) = {
    dataType match {
      case dt @ NativeType() => q"$inputRow.${accessorForType(dt)}($ordinal)"
      case _ => q"$inputRow.apply($ordinal).asInstanceOf[${termForType(dataType)}]"
    }
  }

  protected def setColumn(
                           destinationRow: TermName,
                           dataType: DataType,
                           ordinal: Int,
                           value: TermName) = {
    dataType match {
      case dt @ NativeType() => q"$destinationRow.${mutatorForType(dt)}($ordinal, $value)"
      case _ => q"$destinationRow.update($ordinal, $value)"
    }
  }

  protected def accessorForType(dt: DataType) = newTermName(s"get${primitiveForType(dt)}")
  protected def mutatorForType(dt: DataType) = newTermName(s"set${primitiveForType(dt)}")

  protected def primitiveForType(dt: DataType) = dt match {
    case IntegerType => "Int"
    case LongType => "Long"
    case ShortType => "Short"
    case ByteType => "Byte"
    case DoubleType => "Double"
    case FloatType => "Float"
    case BooleanType => "Boolean"
    case StringType => "String"
  }

  protected def defaultPrimitive(dt: DataType) = dt match {
    case BooleanType => ru.Literal(Constant(false))
    case FloatType => ru.Literal(Constant(-1.0.toFloat))
    case StringType => ru.Literal(Constant("<uninit>"))
    case ShortType => ru.Literal(Constant(-1.toShort))
    case LongType => ru.Literal(Constant(1L))
    case ByteType => ru.Literal(Constant(-1.toByte))
    case DoubleType => ru.Literal(Constant(-1.toDouble))
    case DecimalType => ru.Literal(Constant(-1)) // Will get implicity converted as needed.
    case IntegerType => ru.Literal(Constant(-1))
    case _ => ru.Literal(Constant(null))
  }

  protected def termForType(dt: DataType) = dt match {
    case n: NativeType => n.tag
    case _ => typeTag[Any]
  }
}
