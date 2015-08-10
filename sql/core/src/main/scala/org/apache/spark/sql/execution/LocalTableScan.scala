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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{InternalRow, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.metric.SQLMetrics


/**
 * Physical plan node for scanning data from a local collection.
 */
private[sql] case class LocalTableScan(
    output: Seq[Attribute],
    rows: Seq[InternalRow]) extends LeafNode {

  override private[sql] lazy val metrics = Map(
    "numRows" -> SQLMetrics.createLongMetric(sparkContext, "number of rows"))

  private lazy val rdd = sqlContext.sparkContext.parallelize(rows)

  protected override def doExecute(): RDD[InternalRow] = {
    val numRows = longMetric("numRows")
    rdd.map { row =>
      numRows += 1
      row
    }
  }

  override def executeCollect(): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    rows.map(converter(_).asInstanceOf[Row]).toArray
  }

  override def executeTake(limit: Int): Array[Row] = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    rows.map(converter(_).asInstanceOf[Row]).take(limit).toArray
  }
}
