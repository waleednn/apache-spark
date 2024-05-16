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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, CollationKey, EqualNullSafe, EqualTo, Lower}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types.StringType

object RewriteCollationJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUpWithNewOutput {
    case j @ Join(_, _, _, Some(condition), _) =>
      val newCondition = condition transform {
        case EqualTo(l: AttributeReference, r: AttributeReference) =>
          (l.dataType, r.dataType) match {
            case (st: StringType, _: StringType) =>
              val collation = CollationFactory.fetchCollation(st.collationId)
              if (collation.supportsBinaryEquality) {
                condition
              } else if (collation.supportsLowercaseEquality) {
                EqualTo(Lower(l), Lower(r))
              } else {
                EqualTo(CollationKey(l), CollationKey(r))
              }
            case _ =>
              condition
          }
        case EqualNullSafe(l: AttributeReference, r: AttributeReference) =>
          (l.dataType, r.dataType) match {
            case (st: StringType, _: StringType) =>
              val collation = CollationFactory.fetchCollation(st.collationId)
              if (collation.supportsBinaryEquality) {
                condition
              } else if (collation.supportsLowercaseEquality) {
                EqualNullSafe(Lower(l), Lower(r))
              } else {
                EqualNullSafe(CollationKey(l), CollationKey(r))
              }
            case _ =>
              condition
          }
      }
      if (!newCondition.fastEquals(condition)) {
        val newJoin = j.copy(condition = Some(newCondition))
        (newJoin, j.output.zip(newJoin.output))
      } else {
        (j, j.output.zip(j.output))
      }
  }
}
