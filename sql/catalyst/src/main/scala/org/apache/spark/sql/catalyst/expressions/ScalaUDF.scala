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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * User-defined function.
 * @param dataType  Return type of function.
 */
case class ScalaUDF(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Seq[DataType] = Nil)
  extends Expression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def toString: String = s"UDF(${children.mkString(",")})"

  // Accessors used in genCode
  def userDefinedFunc(): AnyRef = function
  def getChildren(): Seq[Expression] = children

  val inputSchema: StructType = {
      val fields = if (inputTypes == Nil) {
        // from the deprecated callUDF codepath
        children.zipWithIndex.map { case (e, i) =>
          StructField(s"_c$i", e.dataType)
        }
      } else {
        inputTypes.zipWithIndex.map { case (t, i) =>
          StructField(s"_c$i", t)
        }
      }
      StructType(fields)
    }

  // scalastyle:off

  /** This method has been generated by this script

    (1 to 22).map { x =>
      val anys = (1 to x).map(x => "Any").reduce(_ + ", " + _)
      val evals = (0 to x - 1).map(x => s"convertedRow.get($x)").reduce(_ + ",\n      " + _)

      s"""case $x =>
      val func = function.asInstanceOf[($anys) => Any]
      (input: InternalRow) => {
        val convertedRow: Row = inputEncoder.fromRow(input)
        func(
          $evals)
      }
      """
    }.foreach(println)

  */

  private[this] val f = {
    lazy val inputEncoder: ExpressionEncoder[Row] = RowEncoder(inputSchema)
    children.size match {
      case 0 =>
        val func = function.asInstanceOf[() => Any]
        (input: InternalRow) => {
          func()
        }

      case 1 =>
        val func = function.asInstanceOf[(Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0))
        }

      case 2 =>
        val func = function.asInstanceOf[(Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1))
        }

      case 3 =>
        val func = function.asInstanceOf[(Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2))
        }

      case 4 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3))
        }

      case 5 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4))
        }

      case 6 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5))
        }

      case 7 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6))
        }

      case 8 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7))
        }

      case 9 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8))
        }

      case 10 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9))
        }

      case 11 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10))
        }

      case 12 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11))
        }

      case 13 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12))
        }

      case 14 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13))
        }

      case 15 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14))
        }

      case 16 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15))
        }

      case 17 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16))
        }

      case 18 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16), convertedRow.get(17))
        }

      case 19 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16), convertedRow.get(17), convertedRow.get(18))
        }

      case 20 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16), convertedRow.get(17), convertedRow.get(18), convertedRow.get(19))
        }

      case 21 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16), convertedRow.get(17), convertedRow.get(18), convertedRow.get(19),
            convertedRow.get(20))
        }

      case 22 =>
        val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
        (input: InternalRow) => {
          val convertedRow: Row = inputEncoder.fromRow(input)
          func(convertedRow.get(0), convertedRow.get(1), convertedRow.get(2), convertedRow.get(3),
            convertedRow.get(4), convertedRow.get(5), convertedRow.get(6), convertedRow.get(7),
            convertedRow.get(8), convertedRow.get(9), convertedRow.get(10), convertedRow.get(11),
            convertedRow.get(12), convertedRow.get(13), convertedRow.get(14), convertedRow.get(15),
            convertedRow.get(16), convertedRow.get(17), convertedRow.get(18), convertedRow.get(19),
            convertedRow.get(20), convertedRow.get(21))
        }
    }
  }

  // scalastyle:on

  // Generate codes used to convert the arguments to Scala type for user-defined funtions
  private[this] def genCodeForConverter(
      ctx: CodeGenContext,
      scalaUDFTermIdx: Int,
      index: Int): String = {
    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"
    val expressionClassName = classOf[Expression].getName
    val scalaUDFClassName = classOf[ScalaUDF].getName

    val converterTerm = ctx.freshName("converter")
    ctx.addMutableState(converterClassName, converterTerm,
      s"this.$converterTerm = ($converterClassName)$typeConvertersClassName" +
        s".createToScalaConverter(((${expressionClassName})((($scalaUDFClassName)" +
          s"expressions[$scalaUDFTermIdx]).getChildren().apply($index))).dataType());")
    converterTerm
  }

  override def genCode(
      ctx: CodeGenContext,
      ev: GeneratedExpressionCode): String = {

    ctx.references += this
    val scalaUDFTermIdx = ctx.references.size - 1

    val scalaUDFClassName = classOf[ScalaUDF].getName
    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"
    val expressionClassName = classOf[Expression].getName

    // Generate codes used to convert the returned value of user-defined functions to Catalyst type
    val catalystConverterTerm = ctx.freshName("catalystConverter")
    ctx.addMutableState(converterClassName, catalystConverterTerm,
      s"this.$catalystConverterTerm = ($converterClassName)$typeConvertersClassName" +
        s".createToCatalystConverter((($scalaUDFClassName)expressions" +
          s"[$scalaUDFTermIdx]).dataType());")

    val resultTerm = ctx.freshName("result")

    // This must be called before children expressions' codegen
    // because ctx.references is used in genCodeForConverter
    val converterTerms = (0 until children.size).map(genCodeForConverter(ctx, scalaUDFTermIdx, _))

    // Initialize user-defined function
    val funcClassName = s"scala.Function${children.size}"

    val funcTerm = ctx.freshName("udf")
    ctx.addMutableState(funcClassName, funcTerm,
      s"this.$funcTerm = ($funcClassName)((($scalaUDFClassName)expressions" +
        s"[$scalaUDFTermIdx]).userDefinedFunc());")

    // codegen for children expressions
    val evals = children.map(_.gen(ctx))

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString
    val funcArguments = converterTerms.zip(evals).map {
      case (converter, eval) => s"$converter.apply(${eval.value})"
    }.mkString(",")
    val callFunc = s"${ctx.boxedType(ctx.javaType(dataType))} $resultTerm = " +
      s"(${ctx.boxedType(ctx.javaType(dataType))})${catalystConverterTerm}" +
        s".apply($funcTerm.apply($funcArguments));"

    evalCode + s"""
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      Boolean ${ev.isNull};

      $callFunc

      ${ev.value} = $resultTerm;
      ${ev.isNull} = $resultTerm == null;
    """
  }

  override def eval(input: InternalRow): Any = {
    val outputEncoder: ExpressionEncoder[Row] =
      RowEncoder(StructType(StructField("_c0", dataType) :: Nil))
    outputEncoder.toRow(Row(f(input)))
  }
}
