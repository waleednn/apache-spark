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
package org.apache.spark.sql.execution

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{CatalystQl, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf, SimpleParserConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

private[sql] class SparkQl(conf: ParserConf = SimpleParserConf()) extends CatalystQl(conf) {
  /** Check if a command should not be explained. */
  protected def isNoExplainCommand(command: String): Boolean = "TOK_DESCTABLE" == command

  protected override def nodeToPlan(node: ASTNode): LogicalPlan = {
    node match {
      // Just fake explain for any of the native commands.
      case Token("TOK_EXPLAIN", explainArgs) if isNoExplainCommand(explainArgs.head.text) =>
        ExplainCommand(OneRowRelation)

      case Token("TOK_EXPLAIN", explainArgs) if "TOK_CREATETABLE" == explainArgs.head.text =>
        val Some(crtTbl) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_CREATETABLE", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(crtTbl), extended = extended.isDefined)

      case Token("TOK_EXPLAIN", explainArgs) =>
        // Ignore FORMATTED if present.
        val Some(query) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_QUERY", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(query), extended = extended.isDefined)

      case Token("TOK_REFRESHTABLE", nameParts :: Nil) =>
        val tableIdent = extractTableIdent(nameParts)
        RefreshTable(tableIdent)

      case Token("TOK_CREATETABLEUSING", createTableArgs) =>
        val Seq(
          temp,
          allowExisting,
          Some(tabName),
          tableCols,
          Some(Token("TOK_TABLEPROVIDER", providerNameParts)),
          tableOpts,
          tableAs) = getClauses(Seq(
          "TEMPORARY",
          "TOK_IFNOTEXISTS",
          "TOK_TABNAME", "TOK_TABCOLLIST",
          "TOK_TABLEPROVIDER",
          "TOK_TABLEOPTIONS",
          "TOK_QUERY"), createTableArgs)

        val tableIdent: TableIdentifier = tabName match {
          case Token("TOK_TABNAME", Token(dbName, _) :: Token(tableName, _) :: Nil) =>
            new TableIdentifier(cleanIdentifier(tableName), Some(cleanIdentifier(dbName)))
          case Token("TOK_TABNAME", Token(tableName, _) :: Nil) =>
            TableIdentifier(cleanIdentifier(tableName))
        }

        val columns = tableCols.map {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }

        val provider = providerNameParts.map {
          case Token(name, _) => name
        }.mkString(".")

        val options = tableOpts.map { opts =>
          opts match {
            case Token("TOK_TABLEOPTIONS", options) =>
              options.map {
                case Token("TOK_TABLEOPTION", keysAndValue) =>
                  val key = keysAndValue.init.map {
                    case Token(k, _) => k
                  }.mkString(".")
                  val value = unquoteString(keysAndValue.last.text)
                  (key, unquoteString(value))
              }.asInstanceOf[Seq[(String, String)]].toMap
          }
        }.getOrElse(Map.empty[String, String])

        val asClause = tableAs.map(nodeToPlan(_))

        if (temp.isDefined && allowExisting.isDefined) {
          throw new AnalysisException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        if (asClause.isDefined) {
          if (columns.isDefined) {
            throw new AnalysisException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }

          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          CreateTableUsingAsSelect(tableIdent,
            provider,
            temp.isDefined,
            Array.empty[String],
            bucketSpec = None,
            mode,
            options,
            asClause.get)
        } else {
          CreateTableUsing(
            tableIdent,
            columns,
            provider,
            temp.isDefined,
            options,
            allowExisting.isDefined,
            managedIfNoPath = false)
        }

      case Token("TOK_DESCTABLE", describeArgs) =>
        // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        val Some(tableType) :: formatted :: extended :: pretty :: Nil =
          getClauses(Seq("TOK_TABTYPE", "FORMATTED", "EXTENDED", "PRETTY"), describeArgs)
        if (formatted.isDefined || pretty.isDefined) {
          // FORMATTED and PRETTY are not supported and this statement will be treated as
          // a Hive native command.
          nodeToDescribeFallback(node)
        } else {
          tableType match {
            case Token("TOK_TABTYPE", Token("TOK_TABNAME", nameParts) :: Nil) =>
              nameParts match {
                case Token(dbName, _) :: Token(tableName, _) :: Nil =>
                  // It is describing a table with the format like "describe db.table".
                  // TODO: Actually, a user may mean tableName.columnName. Need to resolve this
                  // issue.
                  val tableIdent = TableIdentifier(
                    cleanIdentifier(tableName), Some(cleanIdentifier(dbName)))
                  datasources.DescribeCommand(
                    UnresolvedRelation(tableIdent, None), isExtended = extended.isDefined)
                case Token(dbName, _) :: Token(tableName, _) :: Token(colName, _) :: Nil =>
                  // It is describing a column with the format like "describe db.table column".
                  nodeToDescribeFallback(node)
                case tableName :: Nil =>
                  // It is describing a table with the format like "describe table".
                  datasources.DescribeCommand(
                    UnresolvedRelation(TableIdentifier(cleanIdentifier(tableName.text)), None),
                    isExtended = extended.isDefined)
                case _ =>
                  nodeToDescribeFallback(node)
              }
            // All other cases.
            case _ =>
              nodeToDescribeFallback(node)
          }
        }

      case _ =>
        super.nodeToPlan(node)
    }
  }

  protected def nodeToDescribeFallback(node: ASTNode): LogicalPlan = noParseRule("Describe", node)
}
