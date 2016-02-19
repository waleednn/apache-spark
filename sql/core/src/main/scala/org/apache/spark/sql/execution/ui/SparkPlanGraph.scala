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

package org.apache.spark.sql.execution.ui

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetricParam, SQLMetricValue}

/**
 * A graph used for storing information of an executionPlan of DataFrame.
 *
 * Each graph is defined with a set of nodes and a set of edges. Each node represents a node in the
 * SparkPlan tree, and each edge represents a parent-child relationship between two nodes.
 */
private[ui] case class SparkPlanGraph(
    nodes: Seq[SparkPlanGraphNode], edges: Seq[SparkPlanGraphEdge]) {

  def makeDotFile(metrics: Map[Long, String]): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    nodes.foreach(node => dotFile.append(node.makeDotNode(metrics) + "\n"))
    edges.foreach(edge => dotFile.append(edge.makeDotEdge + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }
}

private[sql] object SparkPlanGraph {

  /**
   * Build a SparkPlanGraph from the root of a SparkPlan tree.
   */
  def apply(plan: SparkPlan): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    buildSparkPlanGraphNode(plan, nodeIdGenerator, nodes, edges)
    new SparkPlanGraph(nodes, edges)
  }

  private def buildSparkPlanGraphNode(
      plan: SparkPlan,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge]): SparkPlanGraphNode = {
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      SQLPlanMetric(metric.name.getOrElse(key), metric.id,
        metric.param.asInstanceOf[SQLMetricParam[SQLMetricValue[Any], Any]])
    }
    val node = SparkPlanGraphNode(
      nodeIdGenerator.getAndIncrement(), plan.nodeName, plan.simpleString, plan.metadata, metrics)
    nodes += node
    val childrenNodes = plan.children.map(
      child => buildSparkPlanGraphNode(child, nodeIdGenerator, nodes, edges))
    for (child <- childrenNodes) {
      edges += SparkPlanGraphEdge(child.id, node.id)
    }
    node
  }
}

/**
 * Represent a node in the SparkPlan tree, along with its metrics.
 *
 * @param id generated by "SparkPlanGraph". There is no duplicate id in a graph
 * @param name the name of this SparkPlan node
 * @param metrics metrics that this SparkPlan node will track
 */
private[ui] case class SparkPlanGraphNode(
    id: Long,
    name: String,
    desc: String,
    metadata: Map[String, String],
    metrics: Seq[SQLPlanMetric]) {

  def makeDotNode(metricsValue: Map[Long, String]): String = {
    val builder = new mutable.StringBuilder(name)

    val values = for {
      metric <- metrics
      value <- metricsValue.get(metric.accumulatorId)
    } yield {
      metric.name + ": " + value
    }

    if (values.nonEmpty) {
      // If there are metrics, display each entry in a separate line. We should use an escaped
      // "\n" here to follow the dot syntax.
      //
      // Note: whitespace between two "\n"s is to create an empty line between the name of
      // SparkPlan and metrics. If removing it, it won't display the empty line in UI.
      builder ++= "\\n \\n"
      builder ++= values.mkString("\\n")
    }

    s"""  $id [label="${builder.toString()}"];"""
  }
}

/**
 * Represent an edge in the SparkPlan tree. `fromId` is the parent node id, and `toId` is the child
 * node id.
 */
private[ui] case class SparkPlanGraphEdge(fromId: Long, toId: Long) {

  def makeDotEdge: String = s"""  $fromId->$toId;\n"""
}
