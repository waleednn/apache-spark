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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, CollationKey}
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.StringType

object RewriteGroupByCollation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case a: Aggregate =>
      val aliasMap = a.groupingExpressions.collect {
        case attr: AttributeReference if attr.dataType.isInstanceOf[StringType] &&
          !CollationFactory.fetchCollation(
            attr.dataType.asInstanceOf[StringType].collationId).supportsBinaryEquality =>
          attr -> CollationKey(attr)
      }.toMap

      val newGroupingExpressions = a.groupingExpressions.map {
        case attr: AttributeReference if aliasMap.contains(attr) =>
          aliasMap(attr)
        case other => other
      }

      val newAggregateExpressions = a.aggregateExpressions.map {
        case attr: AttributeReference if aliasMap.contains(attr) =>
          Alias(First(attr, ignoreNulls = false).toAggregateExpression(), attr.name)()
        case other => other
      }

      val newAggregate = a.copy(
        groupingExpressions = newGroupingExpressions,
        aggregateExpressions = newAggregateExpressions
      )

      if (!newAggregate.fastEquals(a)) {
        (newAggregate, a.output.zip(newAggregate.output))
      } else {
        (a, a.output.zip(a.output))
      }
  }
}
