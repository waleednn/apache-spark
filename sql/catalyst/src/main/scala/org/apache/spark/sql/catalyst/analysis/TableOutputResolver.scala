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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, HiveUtils}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegralType, MapType, StructType}

object TableOutputResolver {
  def resolveOutputColumns(
      tableName: String,
      expectedX: Seq[Attribute],
      queryX: LogicalPlan,
      byName: Boolean,
      conf: SQLConf, expectedOrder: Option[String] = None): LogicalPlan = {

    val (query, expected) = if (expectedOrder.isEmpty || byName) {
      queryX -> expectedX
    } else {
      // the input plan queryX is as per expected schema. we need to convert it to actual schema

      val actualOutput = HiveUtils.convertExpectedToActual(expectedOrder.get, queryX.output)
      Project(actualOutput, queryX) ->
        HiveUtils.convertExpectedToActual(expectedOrder.get, expectedX)
    }

    val actualExpectedCols = expected.map { attr =>
      attr.withDataType(CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType))
    }

    if (actualExpectedCols.size < query.output.size) {
      throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
        tableName, actualExpectedCols, query)
    }

    val errors = new mutable.ArrayBuffer[String]()
    val resolved: Seq[NamedExpression] = if (byName) {
      reorderColumnsByName(query.output, actualExpectedCols, conf, errors += _)
    } else {
      if (actualExpectedCols.size > query.output.size) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(
          tableName, actualExpectedCols, query)
      }

      resolveColumnsByPosition(query.output, actualExpectedCols, conf, errors += _)
    }

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.cannotWriteIncompatibleDataToTableError(tableName, errors.toSeq)
    }

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  private def reorderColumnsByName(
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {
    val matchedCols = mutable.HashSet.empty[String]
    val reordered = expectedCols.flatMap { expectedCol =>
      val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
      val newColPath = colPath :+ expectedCol.name
      if (matched.isEmpty) {
        addError(s"Cannot find data for output column '${newColPath.quoted}'")
        None
      } else if (matched.length > 1) {
        addError(s"Ambiguous column name in the input data: '${newColPath.quoted}'")
        None
      } else {
        matchedCols += matched.head.name
        val expectedName = expectedCol.name
        val matchedCol = matched.head match {
          // Save an Alias if we can change the name directly.
          case a: Attribute => a.withName(expectedName)
          case a: Alias => a.withName(expectedName)
          case other => other
        }
        (matchedCol.dataType, expectedCol.dataType) match {
          case (matchedType: StructType, expectedType: StructType) =>
            resolveStructType(
              matchedCol, matchedType, expectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case (matchedType: ArrayType, expectedType: ArrayType) =>
            resolveArrayType(
              matchedCol, matchedType, expectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case (matchedType: MapType, expectedType: MapType) =>
            resolveMapType(
              matchedCol, matchedType, expectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case _ =>
            checkField(expectedCol, matchedCol, byName = true, conf, addError, newColPath)
        }
      }
    }

    if (reordered.length == expectedCols.length) {
      if (matchedCols.size < inputCols.length) {
        val extraCols = inputCols.filterNot(col => matchedCols.contains(col.name))
          .map(col => s"'${col.name}'").mkString(", ")
        addError(s"Cannot write extra fields to struct '${colPath.quoted}': $extraCols")
        Nil
      } else {
        reordered
      }
    } else {
      Nil
    }
  }

  private def resolveColumnsByPosition(
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {

    if (inputCols.size > expectedCols.size) {
      val extraColsStr = inputCols.takeRight(inputCols.size - expectedCols.size)
        .map(col => s"'${col.name}'")
        .mkString(", ")
      addError(s"Cannot write extra fields to struct '${colPath.quoted}': $extraColsStr")
      return Nil
    } else if (inputCols.size < expectedCols.size) {
      val missingColsStr = expectedCols.takeRight(expectedCols.size - inputCols.size)
        .map(col => s"'${col.name}'")
        .mkString(", ")
      addError(s"Struct '${colPath.quoted}' missing fields: $missingColsStr")
      return Nil
    }

    inputCols.zip(expectedCols).flatMap { case (inputCol, expectedCol) =>
      val newColPath = colPath :+ expectedCol.name
      (inputCol.dataType, expectedCol.dataType) match {
        case (inputType: StructType, expectedType: StructType) =>
          resolveStructType(
            inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case (inputType: ArrayType, expectedType: ArrayType) =>
          resolveArrayType(
            inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case (inputType: MapType, expectedType: MapType) =>
          resolveMapType(
            inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case _ =>
          checkField(expectedCol, inputCol, byName = false, conf, addError, newColPath)
      }
    }
  }

  private def checkNullability(
      input: Expression,
      expected: Attribute,
      conf: SQLConf,
      colPath: Seq[String]): Expression = {
    if (requiresNullChecks(input, expected, conf)) {
      AssertNotNull(input, colPath)
    } else {
      input
    }
  }

  private def requiresNullChecks(
      input: Expression,
      attr: Attribute,
      conf: SQLConf): Boolean = {
    input.nullable && !attr.nullable && conf.storeAssignmentPolicy != StoreAssignmentPolicy.LEGACY
  }

  private def resolveStructType(
      input: NamedExpression,
      inputType: StructType,
      expected: Attribute,
      expectedType: StructType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)
    val fields = inputType.zipWithIndex.map { case (f, i) =>
      Alias(GetStructField(nullCheckedInput, i, Some(f.name)), f.name)()
    }
    val resolved = if (byName) {
      reorderColumnsByName(fields, expectedType.toAttributes, conf, addError, colPath)
    } else {
      resolveColumnsByPosition(fields, expectedType.toAttributes, conf, addError, colPath)
    }
    if (resolved.length == expectedType.length) {
      val struct = CreateStruct(resolved)
      val res = if (nullCheckedInput.nullable) {
        If(IsNull(nullCheckedInput), Literal(null, struct.dataType), struct)
      } else {
        struct
      }
      Some(Alias(res, expected.name)())
    } else {
      None
    }
  }

  private def resolveArrayType(
      input: NamedExpression,
      inputType: ArrayType,
      expected: Attribute,
      expectedType: ArrayType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)
    val param = NamedLambdaVariable("element", inputType.elementType, inputType.containsNull)
    val fakeAttr =
      AttributeReference("element", expectedType.elementType, expectedType.containsNull)()
    val res = if (byName) {
      reorderColumnsByName(Seq(param), Seq(fakeAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(Seq(param), Seq(fakeAttr), conf, addError, colPath)
    }
    if (res.length == 1) {
      val func = LambdaFunction(res.head, Seq(param))
      Some(Alias(ArrayTransform(nullCheckedInput, func), expected.name)())
    } else {
      None
    }
  }

  private def resolveMapType(
      input: NamedExpression,
      inputType: MapType,
      expected: Attribute,
      expectedType: MapType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)

    val keyParam = NamedLambdaVariable("key", inputType.keyType, nullable = false)
    val fakeKeyAttr = AttributeReference("key", expectedType.keyType, nullable = false)()
    val resKey = if (byName) {
      reorderColumnsByName(Seq(keyParam), Seq(fakeKeyAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(Seq(keyParam), Seq(fakeKeyAttr), conf, addError, colPath)
    }

    val valueParam =
      NamedLambdaVariable("value", inputType.valueType, inputType.valueContainsNull)
    val fakeValueAttr =
      AttributeReference("value", expectedType.valueType, expectedType.valueContainsNull)()
    val resValue = if (byName) {
      reorderColumnsByName(Seq(valueParam), Seq(fakeValueAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(Seq(valueParam), Seq(fakeValueAttr), conf, addError, colPath)
    }

    if (resKey.length == 1 && resValue.length == 1) {
      val keyFunc = LambdaFunction(resKey.head, Seq(keyParam))
      val valueFunc = LambdaFunction(resValue.head, Seq(valueParam))
      val newKeys = ArrayTransform(MapKeys(nullCheckedInput), keyFunc)
      val newValues = ArrayTransform(MapValues(nullCheckedInput), valueFunc)
      Some(Alias(MapFromArrays(newKeys, newValues), expected.name)())
    } else {
      None
    }
  }

  // For table insertions, capture the overflow errors and show proper message.
  // Without this method, the overflow errors of castings will show hints for turning off ANSI SQL
  // mode, which are not helpful since the behavior is controlled by the store assignment policy.
  def checkCastOverflowInTableInsert(cast: Cast, columnName: String): Expression = {
    if (canCauseCastOverflow(cast)) {
      CheckOverflowInTableInsert(cast, columnName)
    } else {
      cast
    }
  }

  private def containsIntegralOrDecimalType(dt: DataType): Boolean = dt match {
    case _: IntegralType | _: DecimalType => true
    case a: ArrayType => containsIntegralOrDecimalType(a.elementType)
    case m: MapType =>
      containsIntegralOrDecimalType(m.keyType) || containsIntegralOrDecimalType(m.valueType)
    case s: StructType =>
      s.fields.exists(sf => containsIntegralOrDecimalType(sf.dataType))
    case _ => false
  }

  private def canCauseCastOverflow(cast: Cast): Boolean = {
    containsIntegralOrDecimalType(cast.dataType) &&
      !Cast.canUpCast(cast.child.dataType, cast.dataType)
  }

  private def isCompatible(tableAttr: Attribute, queryExpr: NamedExpression): Boolean = {
    DataTypeUtils.sameType(tableAttr.dataType, queryExpr.dataType) &&
      tableAttr.name == queryExpr.name &&
      tableAttr.metadata == queryExpr.metadata
  }

  private def checkField(
      tableAttr: Attribute,
      queryExpr: NamedExpression,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {

    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(tableAttr.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(tableAttr.dataType)
    } else {
      tableAttr.dataType
    }
    val storeAssignmentPolicy = conf.storeAssignmentPolicy
    lazy val outputField = if (isCompatible(tableAttr, queryExpr)) {
      if (requiresNullChecks(queryExpr, tableAttr, conf)) {
        val assert = AssertNotNull(queryExpr, colPath)
        Some(Alias(assert, tableAttr.name)(explicitMetadata = Some(tableAttr.metadata)))
      } else {
        Some(queryExpr)
      }
    } else {
      val nullCheckedQueryExpr = checkNullability(queryExpr, tableAttr, conf, colPath)
      val casted = cast(nullCheckedQueryExpr, attrTypeWithoutCharVarchar, conf, colPath.quoted)
      val exprWithStrLenCheck = if (conf.charVarcharAsString || !attrTypeHasCharVarchar) {
        casted
      } else {
        CharVarcharUtils.stringLengthCheck(casted, tableAttr.dataType)
      }
      // Renaming is needed for handling the following cases like
      // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
      // 2) Target tables have column metadata
      Some(Alias(exprWithStrLenCheck, tableAttr.name)(explicitMetadata = Some(tableAttr.metadata)))
    }

    storeAssignmentPolicy match {
      case StoreAssignmentPolicy.LEGACY =>
        outputField

      case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
        // run the type check first to ensure type errors are present
        val canWrite = DataType.canWrite(
          queryExpr.dataType, attrTypeWithoutCharVarchar, byName, conf.resolver, colPath.quoted,
          storeAssignmentPolicy, addError)

        if (canWrite) outputField else None
    }
  }

  private def cast(
      expr: Expression,
      expectedType: DataType,
      conf: SQLConf,
      colName: String): Expression = {

    conf.storeAssignmentPolicy match {
      case StoreAssignmentPolicy.ANSI =>
        val cast = Cast(expr, expectedType, Option(conf.sessionLocalTimeZone), ansiEnabled = true)
        cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
        checkCastOverflowInTableInsert(cast, colName)

      case StoreAssignmentPolicy.LEGACY =>
        Cast(expr, expectedType, Option(conf.sessionLocalTimeZone), ansiEnabled = false)

      case _ =>
        Cast(expr, expectedType, Option(conf.sessionLocalTimeZone))
    }
  }
}
