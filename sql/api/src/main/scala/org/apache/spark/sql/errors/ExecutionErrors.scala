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
package org.apache.spark.sql.errors

import java.time.LocalDate
import java.time.temporal.ChronoField

import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.{SparkArithmeticException, SparkDateTimeException, SparkIllegalArgumentException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.sql.catalyst.trees.SQLQueryContext
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] trait ExecutionErrors extends DataTypeErrorsBase {
  def fieldDiffersFromDerivedLocalDateError(
      field: ChronoField,
      actual: Int,
      expected: Int,
      candidate: LocalDate): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2129",
      messageParameters = Map(
        "field" -> field.toString,
        "actual" -> actual.toString,
        "expected" -> expected.toString,
        "candidate" -> candidate.toString),
      context = Array.empty,
      summary = "")
  }

  def failToParseDateTimeInNewParserError(s: String, e: Throwable): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER",
      messageParameters = Map(
        "datetime" -> toSQLValue(s),
        "config" -> toSQLConf(SqlApiConf.LEGACY_TIME_PARSER_POLICY_KEY)),
      e)
  }

  def failToRecognizePatternAfterUpgradeError(
      pattern: String, e: Throwable, docroot: String): Throwable = {
    new SparkUpgradeException(
      errorClass = "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION",
      messageParameters = Map(
        "pattern" -> toSQLValue(pattern),
        "config" -> toSQLConf(SqlApiConf.LEGACY_TIME_PARSER_POLICY_KEY),
        "docroot" -> docroot),
      e)
  }

  def failToRecognizePatternError(
      pattern: String, e: Throwable, docroot: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2130",
      messageParameters = Map(
        "pattern" -> toSQLValue(pattern),
        "docroot" -> docroot),
      cause = e)
  }

  def unreachableError(err: String = ""): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2028",
      messageParameters = Map("err" -> err))
  }

  def invalidInputInCastToDatetimeError(
      value: UTF8String,
      to: DataType,
      context: SQLQueryContext): SparkDateTimeException = {
    invalidInputInCastToDatetimeErrorInternal(toSQLValue(value), StringType, to, context)
  }

  def invalidInputInCastToDatetimeError(
     value: Double,
     to: DataType,
     context: SQLQueryContext): SparkDateTimeException = {
    invalidInputInCastToDatetimeErrorInternal(toSQLValue(value), DoubleType, to, context)
  }

  protected def invalidInputInCastToDatetimeErrorInternal(
      sqlValue: String,
      from: DataType,
      to: DataType,
      context: SQLQueryContext): SparkDateTimeException = {
    new SparkDateTimeException(
      errorClass = "CAST_INVALID_INPUT",
      messageParameters = Map(
        "expression" -> sqlValue,
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def ansiIllegalArgumentError(message: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "_LEGACY_ERROR_TEMP_2000",
      messageParameters = Map(
        "message" -> message,
        "ansiConfig" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)))
  }

  def timestampAddOverflowError(micros: Long, amount: Int, unit: String): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "DATETIME_OVERFLOW",
      messageParameters = Map(
        "operation" -> (s"add ${toSQLValue(amount)} $unit to " +
          s"${DateTimeUtils.microsToInstant(micros)}")),
      context = Array.empty,
      summary = "")
  }

  def arithmeticOverflowError(
      message: String,
      hint: String = "",
      context: SQLQueryContext = null): ArithmeticException = {
    val alternative = if (hint.nonEmpty) {
      s" Use '$hint' to tolerate overflow and return NULL instead."
    } else ""
    new SparkArithmeticException(
      errorClass = "ARITHMETIC_OVERFLOW",
      messageParameters = Map(
        "message" -> message,
        "alternative" -> alternative,
        "config" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)),
      context = getQueryContext(context),
      summary = getSummary(context))
  }

  def cannotParseStringAsDataTypeError(pattern: String, value: String, dataType: DataType)
  : SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_2134",
      messageParameters = Map(
        "value" -> toSQLValue(value),
        "pattern" -> toSQLValue(pattern),
        "dataType" -> dataType.toString))
  }

  def binaryArithmeticCauseOverflowError(
      eval1: Short, symbol: String, eval2: Short): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "BINARY_ARITHMETIC_OVERFLOW",
      messageParameters = Map(
        "value1" -> toSQLValue(eval1),
        "symbol" -> symbol,
        "value2" -> toSQLValue(eval2)),
      context = Array.empty,
      summary = "")
  }

  def unaryMinusCauseOverflowError(originValue: Int): SparkArithmeticException = {
    new SparkArithmeticException(
      errorClass = "_LEGACY_ERROR_TEMP_2043",
      messageParameters = Map("sqlValue" -> toSQLValue(originValue)),
      context = Array.empty,
      summary = "")
  }

  def castingCauseOverflowError(t: Long, to: DataType): ArithmeticException = {
    castingCauseOverflowErrorInternal(toSQLValue(t), LongType, to)
  }

  def castingCauseOverflowError(t: Float, to: DataType): ArithmeticException = {
    castingCauseOverflowErrorInternal(toSQLValue(t), FloatType, to)
  }

  def castingCauseOverflowError(t: Double, to: DataType): ArithmeticException = {
    castingCauseOverflowErrorInternal(toSQLValue(t), DoubleType, to)
  }

  protected def castingCauseOverflowErrorInternal(
      t: String, from: DataType, to: DataType): ArithmeticException = {
    new SparkArithmeticException(
      errorClass = "CAST_OVERFLOW",
      messageParameters = Map(
        "value" -> t,
        "sourceType" -> toSQLType(from),
        "targetType" -> toSQLType(to),
        "ansiConfig" -> toSQLConf(SqlApiConf.ANSI_ENABLED_KEY)),
      context = Array.empty,
      summary = "")
  }

  def unsupportedArrowTypeError(typeName: ArrowType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_ARROWTYPE",
      messageParameters = Map("typeName" -> typeName.toString))
  }

  def duplicatedFieldNameInArrowStructError(
      fieldNames: Seq[String]): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
      messageParameters = Map("fieldNames" -> fieldNames.mkString("[", ", ", "]")))
  }

  def unsupportedDataTypeError(typeName: DataType): SparkUnsupportedOperationException = {
    new SparkUnsupportedOperationException(
      errorClass = "UNSUPPORTED_DATATYPE",
      messageParameters = Map("typeName" -> toSQLType(typeName)))
  }
}

private[sql] object ExecutionErrors extends ExecutionErrors
