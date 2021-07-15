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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.sql.types.DataType

// Aggregate Functions in SQL statement.
// e.g. SELECT COUNT(EmployeeID), Max(salary) FROM dept GROUP BY deptID
// aggregateExpressions are (COUNT(EmployeeID), Max(salary)), groupByColumns are (deptID)
case class Aggregation(aggregateExpressions: Seq[AggregateFunc],
                       groupByColumns: Seq[Expression])

abstract class AggregateFunc

case class Min(column: Expression, dataType: DataType) extends AggregateFunc
case class Max(column: Expression, dataType: DataType) extends AggregateFunc
case class Sum(column: Expression, dataType: DataType, isDistinct: Boolean)
  extends AggregateFunc
case class Count(column: Expression, dataType: DataType, isDistinct: Boolean)
  extends AggregateFunc
