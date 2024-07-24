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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.analysis.{AnalysisTest, UnresolvedIdentifier}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parsePlan
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.DropVariable
import org.apache.spark.sql.test.SharedSparkSession

class DropVariableParserSuite extends AnalysisTest with SharedSparkSession {

  test("drop variable") {
    comparePlans(
      parsePlan("DROP TEMPORARY VARIABLE var1"),
      DropVariable(UnresolvedIdentifier(Seq("var1")), ifExists = false))
    comparePlans(
      parsePlan("DROP TEMPORARY VAR var1"),
      DropVariable(UnresolvedIdentifier(Seq("var1")), ifExists = false))
    comparePlans(
      parsePlan("DROP TEMPORARY VARIABLE IF EXISTS var1"),
      DropVariable(UnresolvedIdentifier(Seq("var1")), ifExists = true))
  }

  test("drop variable - not support syntax 'DROP VARIABLE|VAR'") {
    checkError(
      exception = intercept[ParseException] {
        parsePlan("DROP VARIABLE var1")
      },
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'VARIABLE'", "hint" -> "")
    )
    checkError(
      exception = intercept[ParseException] {
        parsePlan("DROP VAR var1")
      },
      errorClass = "PARSE_SYNTAX_ERROR",
      parameters = Map("error" -> "'VAR'", "hint" -> "")
    )
  }
}
