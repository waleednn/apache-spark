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

package org.apache.spark.sql

import java.util.TimeZone

import org.apache.spark.sql.test.TestCaseInsensitiveSQLContext
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.catalyst.CatalystConf

/* Implicits */

import org.apache.spark.sql.test.TestCaseInsensitiveSQLContext._

object CaseInsensitiveTestData{
  case class StringData(s: String)
  val table = TestCaseInsensitiveSQLContext.sparkContext.parallelize(StringData("test") :: Nil)
  table.registerTempTable("caseInsensitiveTable")
}

class SQLQueryCaseInsensitiveSuite extends QueryTest with BeforeAndAfterAll {
  CaseInsensitiveTestData

  var origZone: TimeZone = _

  override protected def beforeAll() {
    origZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override protected def afterAll() {
    TimeZone.setDefault(origZone)
  }

  test("SPARK-4699 case sensitivity SQL query") {
    setConf(CatalystConf.CASE_SENSITIVE, "false")
    checkAnswer(sql("SELECT S FROM CASEINSENSITIVETABLE"), "test")
  }

}
