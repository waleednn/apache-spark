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

package org.apache.spark.sql.jdbc

import java.sql.Types
import java.util.Locale

import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, GeneralAggregateFunc}
import org.apache.spark.sql.types._


private case object TeradataDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:teradata")

  // scalastyle:off line.size.limit
  // See https://docs.teradata.com/r/Teradata-VantageTM-SQL-Functions-Expressions-and-Predicates/March-2019/Aggregate-Functions
  // scalastyle:on line.size.limit
  override def compileAggregate(aggFunction: AggregateFunc): Option[String] = {
    super.compileAggregate(aggFunction).orElse(
      aggFunction match {
        case f: GeneralAggregateFunc if f.name() == "VAR_POP" =>
          assert(f.children().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_POP($distinct${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "VAR_SAMP" =>
          assert(f.children().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"VAR_SAMP($distinct${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_POP" =>
          assert(f.children().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_POP($distinct${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_SAMP" =>
          assert(f.children().length == 1)
          val distinct = if (f.isDistinct) "DISTINCT " else ""
          Some(s"STDDEV_SAMP($distinct${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_POP" && f.isDistinct == false =>
          assert(f.children().length == 2)
          Some(s"COVAR_POP(${f.children().head}, ${f.children().last})")
        case f: GeneralAggregateFunc if f.name() == "COVAR_SAMP" && f.isDistinct == false =>
          assert(f.children().length == 2)
          Some(s"COVAR_SAMP(${f.children().head}, ${f.children().last})")
        case f: GeneralAggregateFunc if f.name() == "CORR" && f.isDistinct == false =>
          assert(f.children().length == 2)
          Some(s"CORR(${f.children().head}, ${f.children().last})")
        case _ => None
      }
    )
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR(255)", java.sql.Types.VARCHAR))
    case BooleanType => Option(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case _ => None
  }

  // Teradata does not support cascading a truncation
  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  /**
   * The SQL query used to truncate a table. Teradata does not support the 'TRUNCATE' syntax that
   * other dialects use. Instead, we need to use a 'DELETE FROM' statement.
   * @param table The table to truncate.
   * @param cascade Whether or not to cascade the truncation. Default value is the
   *                value of isCascadingTruncateTable(). Teradata does not support cascading a
   *                'DELETE FROM' statement (and as mentioned, does not support 'TRUNCATE' syntax)
   * @return The SQL query to use for truncating a table
   */
  override def getTruncateQuery(
      table: String,
      cascade: Option[Boolean] = isCascadingTruncateTable): String = {
    s"DELETE FROM $table ALL"
  }

  // See https://docs.teradata.com/reader/scPHvjfglIlB8F70YliLAw/wysTNUMsP~0aGzksLCl1kg
  override def renameTable(oldTable: String, newTable: String): String = {
    s"RENAME TABLE $oldTable TO $newTable"
  }

  override def getLimitClause(limit: Integer): String = {
    ""
  }

  override def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    // Use 18 as default scale respect to DecimalType definition
    val defaultScale = 18
    if (sqlType == Types.NUMERIC) {
      if (md == null) {
        Option(DecimalType(DecimalType.MAX_PRECISION, defaultScale))
      } else {
        val scale = md.build().getLong("scale")
        // Scale returned from JDBC is 0 when NUMBER's scale is not explicitly specified
        // Note, even if the NUMBER is defined like NUMBER(20, 0), Spark will treat it as
        // (20, $defaultScale)
        if (scale == 0) {
          Option(DecimalType(DecimalType.MAX_PRECISION, defaultScale))
        } else {
          // Precision returned from JDBC is 40 when NUMBER's precision is not explicitly specified
          if (size == 40) {
            val precision = DecimalType.MAX_PRECISION
            Option(DecimalType(precision, scale.toInt))
          } else {
            Option(DecimalType(size, scale.toInt))
          }
        }
      }
    } else {
      None
    }
  }
}
