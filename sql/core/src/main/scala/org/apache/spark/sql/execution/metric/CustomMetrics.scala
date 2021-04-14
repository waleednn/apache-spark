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

package org.apache.spark.sql.execution.metric

import org.apache.spark.sql.connector.CustomMetric

object CustomMetrics {
  private val V2_CUSTOM = "v2Custom"

  /**
   * Given a class name, builds and returns a metric type for a V2 custom metric class
   * `CustomMetric`.
   */
  def buildV2CustomMetricTypeName(customMetric: CustomMetric): String = {
    s"${V2_CUSTOM}_${customMetric.getClass.getCanonicalName}"
  }

  /**
   * Given a V2 custom metric type name, this method parses it and returns the corresponding
   * `CustomMetric` class name.
   */
  def parseV2CustomMetricType(metricType: String): String = {
    val className = metricType.stripPrefix(s"${V2_CUSTOM}_")

    if (className == metricType) {
      throw new IllegalStateException(s"Metric type $metricType is not a V2 custom metric type.")
    } else {
      className
    }
  }
}

class CustomSumMetric extends CustomMetric {
  override def name(): String = "CustomSumMetric"

  override def description(): String = "Sum up CustomMetric"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    taskMetrics.sum.toString
  }
}

class CustomAvgMetric extends CustomMetric {
  override def name(): String = "CustomAvgMetric"

  override def description(): String = "Average CustomMetric"

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {
    val average = if (taskMetrics.isEmpty) {
      0L
    } else {
      taskMetrics.sum / taskMetrics.length
    }
    average.toString
  }
}
