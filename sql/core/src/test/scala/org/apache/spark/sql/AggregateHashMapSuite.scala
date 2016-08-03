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

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

abstract class AggregateHashMapSuite extends DataFrameAggregateSuite {
  import testImplicits._

  protected def setAggregateHashMapImpl(): Unit

  protected override def beforeAll(): Unit = {
      setAggregateHashMapImpl()
      sparkConf.set("spark.sql.codegen.fallback", "false")
      super.beforeAll()
  }

  test("SQL decimal test") {
    checkAnswer(
      decimalData.groupBy('a cast DecimalType(10, 2)).agg(avg('b cast DecimalType(10, 2))),
      Seq(Row(new java.math.BigDecimal(1.0), new java.math.BigDecimal(1.5)),
        Row(new java.math.BigDecimal(2.0), new java.math.BigDecimal(1.5)),
        Row(new java.math.BigDecimal(3.0), new java.math.BigDecimal(1.5))))
  }
}
