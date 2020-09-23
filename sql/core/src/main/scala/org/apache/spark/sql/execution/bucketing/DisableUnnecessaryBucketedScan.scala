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

package org.apache.spark.sql.execution.bucketing

import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashClusteredDistribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.internal.SQLConf

/**
 * Disable unnecessary bucketed table scan based on actual physical query plan.
 * NOTE: this rule is designed to be applied right after [[EnsureRequirements]],
 * where all [[ShuffleExchangeExec]] and [[SortExec]] have been added to plan properly.
 *
 * When BUCKETING_ENABLED and AUTO_BUCKETED_SCAN_ENABLED are set to true, go through
 * query plan to check where bucketed table scan is unnecessary, and disable bucketed table
 * scan if:
 *
 * (1).The sub-plan from root to bucketed table scan, only contains operator which
 * [[hasInterestingPartition]], [[Exchange]], and allowed single-child operators
 * ([[isAllowedUnaryExecNode]]).
 *
 * (2).The sub-plan from root to bucketed table scan, does not contain operator which
 * [[hasInterestingPartition]].
 *
 * Examples:
 * (1).join:
 *         SortMergeJoin(t1.i = t2.j)
 *            /            \
 *        Sort(i)        Sort(j)
 *          /               \
 *      Shuffle(i)       Scan(t2: i, j)
 *        /         (bucketed on column j, enable bucketed scan)
 *   Scan(t1: i, j)
 * (bucketed on column j, DISABLE bucketed scan)
 *
 * (2).aggregate:
 *         HashAggregate(i, ..., Final)
 *                      |
 *                  Shuffle(i)
 *                      |
 *         HashAggregate(i, ..., Partial)
 *                      |
 *                    Filter
 *                      |
 *                  Scan(t1: i, j)
 *  (bucketed on column j, DISABLE bucketed scan)
 *
 * The idea of [[hasInterestingPartition]] is inspired from "interesting order" in
 * the paper "Access Path Selection in a Relational Database Management System"
 * (https://dl.acm.org/doi/10.1145/582095.582099).
 */
case class DisableUnnecessaryBucketedScan(conf: SQLConf) extends Rule[SparkPlan] {

  /**
   * Disable bucketed table scan with pre-order traversal of plan.
   *
   * @param withInterestingPartition The traversed plan has operator with interesting partition.
   * @param withExchange The traversed plan has [[Exchange]] operator.
   * @param withNonAllowedNodes The traversed plan has operators not to be
   *                            [[isAllowedUnaryExecNode]].
   */
  private def disableBucketWithInterestingPartition(
      plan: SparkPlan,
      withInterestingPartition: Boolean,
      withExchange: Boolean,
      withNonAllowedNodes: Boolean): SparkPlan = {
    plan match {
      case p if hasInterestingPartition(p) =>
        // Operator with interesting partition, propagates `withInterestingPartition` as true
        // to its children, and resets `withExchange` and `withNonAllowedNodes` as false.
        p.mapChildren(disableBucketWithInterestingPartition(_, true, false, false))
      case exchange: Exchange =>
        // Exchange operator propagates `withExchange` as true to its child.
        exchange.mapChildren(disableBucketWithInterestingPartition(
          _, withInterestingPartition, true, withNonAllowedNodes))
      case scan: FileSourceScanExec =>
        if (isBucketedScanWithoutFilter(scan)) {
          val isSafeToDisableWithInterestingPartition =
            withInterestingPartition && withExchange && !withNonAllowedNodes
          if (isSafeToDisableWithInterestingPartition || !withInterestingPartition) {
            // Disable bucketed table scan if:
            // (1).The sub-plan has operator with interesting partition, exchange operator,
            //     and allowed single-child operators.
            // (2).The sub-plan does not have operator with interesting partition.
            scan.copy(disableBucketedScan = true)
          } else {
            scan
          }
        } else {
          scan
        }
      case o =>
        o.mapChildren(disableBucketWithInterestingPartition(
          _,
          withInterestingPartition,
          withExchange,
          withNonAllowedNodes || !isAllowedUnaryExecNode(o)))
    }
  }

  private def hasInterestingPartition(plan: SparkPlan): Boolean = {
    plan.requiredChildDistribution.exists {
      case _: ClusteredDistribution | _: HashClusteredDistribution => true
      case _ => false
    }
  }

  private def isAllowedUnaryExecNode(plan: SparkPlan): Boolean = {
    plan match {
      case _: SortExec | _: ProjectExec | _: FilterExec => true
      case partialAgg: BaseAggregateExec =>
        partialAgg.requiredChildDistributionExpressions.isEmpty
      case _ => false
    }
  }

  private def isBucketedScanWithoutFilter(scan: FileSourceScanExec): Boolean = {
    // Do not disable bucketed table scan if it has filter pruning,
    // because bucketed table scan is still useful here to save CPU/IO cost with
    // only reading selected bucket files.
    scan.bucketedScan && scan.optionalBucketSet.isEmpty
  }

  def apply(plan: SparkPlan): SparkPlan = {
    lazy val hasBucketedScanWithoutFilter = plan.find {
      case scan: FileSourceScanExec => isBucketedScanWithoutFilter(scan)
      case _ => false
    }.isDefined

    if (!conf.bucketingEnabled || !conf.autoBucketedScanEnabled || !hasBucketedScanWithoutFilter) {
      plan
    } else {
      disableBucketWithInterestingPartition(plan, false, false, false)
    }
  }
}
