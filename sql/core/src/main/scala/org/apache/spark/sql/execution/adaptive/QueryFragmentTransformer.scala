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

package org.apache.spark.sql.execution.adaptive

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.internal.SQLConf

/**
 * Transform a physical plan tree into an query fragment tree
 */
case class QueryFragmentTransformer(conf: SQLConf, maxIterations: Int = 100)
  extends Rule[SparkPlan] {

  private val nextFragmentId = new AtomicLong(0)

  def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transformUp {
      case operator: SparkPlan => withQueryFragment(operator)
    }
    if (newPlan.isInstanceOf[ExecutedCommandExec]) {
      newPlan
    } else {
      val childFragments = Utils.findChildFragment(newPlan)
      val newFragment = new RootQueryFragment(childFragments,
        nextFragmentId.getAndIncrement(), true)
      childFragments.foreach(child => child.setParentFragment(newFragment))
      newFragment.setRootPlan(newPlan)
      newFragment
    }
  }

  private[this] def withQueryFragment(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    val children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    val supportsAdaptiveExecution =
      if (children.exists(_.isInstanceOf[ShuffleExchange])) {
        // Right now, Adaptive execution only support HashPartitionings.
        children.forall {
          case e @ ShuffleExchange(hash: HashPartitioning, _, _) => true
          case child =>
            child.outputPartitioning match {
              case hash: HashPartitioning => true
              case collection: PartitioningCollection =>
                collection.partitionings.forall(_.isInstanceOf[HashPartitioning])
              case _ => false
            }
        }
      } else {
        // In this case, although we do not have Exchange operators, we may still need to
        // shuffle data when we have more than one children because data generated by
        // these children may not be partitioned in the same way.
        // Please see the comment in withCoordinator for more details.
        val supportsDistribution =
          requiredChildDistributions.forall(_.isInstanceOf[ClusteredDistribution])
        children.length > 1 && supportsDistribution
      }

    val withFragments =
      if (supportsAdaptiveExecution) {
        children.zip(requiredChildDistributions).map {
          case (e: ShuffleExchange, _) =>
            // This child is an Exchange, we need to add the fragment.
            val childFragments = Utils.findChildFragment(e)
            val newFragment = new UnaryQueryFragment(childFragments,
              nextFragmentId.getAndIncrement(), false)
            childFragments.foreach(child => child.setParentFragment(newFragment))
            val fragmentInput = FragmentInput(newFragment)
            fragmentInput.setInputPlan(e)
            newFragment.setExchange(e)
            newFragment.setFragmentInput(fragmentInput)
            fragmentInput

          case (child, distribution) =>
            child
        }
      } else {
        children
      }

    operator.withNewChildren(withFragments)
  }
}
