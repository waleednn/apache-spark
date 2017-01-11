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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Statistics}


object AggregateEstimation {
  import EstimationUtils._

  /**
   * Estimate the number of output rows based on column stats of group-by columns, and propagate
   * column stats for aggregate expressions.
   */
  def estimate(agg: Aggregate): Option[Statistics] = {
    val childStats = agg.child.statistics
    // Check if we have column stats for all group-by columns.
    val colStatsExist = agg.groupingExpressions.forall { e =>
      e.isInstanceOf[Attribute] && childStats.attributeStats.contains(e.asInstanceOf[Attribute])
    }
    if (rowCountsExist(agg.child) && colStatsExist) {
      // Multiply distinct counts of group-by columns. This is an upper bound, which assumes
      // the data contains all combinations of distinct values of group-by columns.
      var outputRows: BigInt = agg.groupingExpressions.foldLeft(BigInt(1))(
        (res, expr) => res * childStats.attributeStats(expr.asInstanceOf[Attribute]).distinctCount)

      // Here we set another upper bound for the number of output rows: it must not be larger than
      // child's number of rows.
      outputRows = outputRows.min(childStats.rowCount.get)

      val outputAttrStats = getOutputMap(childStats.attributeStats, agg.output)
      Some(Statistics(
        sizeInBytes = outputRows * getRowSize(agg.output, outputAttrStats),
        rowCount = Some(outputRows),
        attributeStats = outputAttrStats,
        isBroadcastable = childStats.isBroadcastable))
    } else {
      None
    }
  }
}
