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

package org.apache.spark.sql.sources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.BeforeAndAfter

abstract class DataSourceTest extends QueryTest with BeforeAndAfter {
  // Case sensitivity is not configurable yet, but we want to test some edge cases.
  // TODO: Remove when it is configurable
  implicit val caseInsensisitiveContext = new SQLContext(TestSQLContext.sparkContext) {
    @transient
    override protected[sql] lazy val analyzer: Analyzer =
      new Analyzer(catalog, functionRegistry, caseSensitive = false) {
        override val extendedRules =
          PreInsertCastAndRename(resolver) ::
          Nil
      }
  }
}

