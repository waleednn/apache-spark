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

package org.apache.spark.sql.execution.stat

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

object FrequentItems extends Logging {

  /** A helper class wrapping `MutableMap[Any, Long]` for simplicity. */
  private class FreqItemCounter(size: Int) extends Serializable {
    val baseMap: MutableMap[Any, Long] = MutableMap.empty[Any, Long]
    /**
     * Add a new example to the counts if it exists, otherwise deduct the count
     * from existing items.
     */
    def add(key: Any, count: Long): this.type = {
      if (baseMap.contains(key))  {
        baseMap(key) += count
      } else {
        if (baseMap.size < size) {
          baseMap += key -> count
        } else {
          val minCount = if (baseMap.values.isEmpty) 0 else baseMap.values.min
          val remainder = count - minCount
          if (remainder >= 0) {
            baseMap += key -> count // something will get kicked out, so we can add this
            baseMap.retain((k, v) => v > minCount)
            baseMap.transform((k, v) => v - minCount)
          } else {
            baseMap.transform((k, v) => v - count)
          }
        }
      }
      this
    }

    /**
     * Merge two maps of counts.
     * @param other The map containing the counts for that partition
     */
    def merge(other: FreqItemCounter): this.type = {
      other.baseMap.foreach { case (k, v) =>
        add(k, v)
      }
      this
    }
  }

  /** Helper function to resolve column to expr (if not yet) */
  // TODO: it might be helpful to have this helper in Dataset.scala,
  // e.g. `drop` function uses exactly the same flow to deal with
  // `Column` arguments
  private def resolveColumn(df: DataFrame, col: Column): Column = {
    col match {
      case Column(u: UnresolvedAttribute) =>
        Column(df.queryExecution.analyzed.resolveQuoted(
          u.name, df.sparkSession.sessionState.analyzer.resolver).getOrElse(u))
      case Column(_expr: Expression) => col
    }
  }

  /**
   * Finding frequent items for columns, possibly with false positives. Using the
   * frequent element count algorithm described in
   * <a href="https://doi.org/10.1145/762471.762473">here</a>, proposed by Karp, Schenker,
   * and Papadimitriou.
   * The `support` should be greater than 1e-4.
   * For Internal use only.
   *
   * @param df The input DataFrame
   * @param cols the names of the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def singlePassFreqItems(
      df: DataFrame,
      cols: Seq[String],
      support: Double): DataFrame = {
    singlePassFreqItemsByColumns(df, cols.map(df.col), support)
  }

  /**
   * Finding frequent items for columns, possibly with false positives.
   * This version of singlePassFreqItems except [[Column]] rather than names.
   *
   * @param df The input DataFrame
   * @param cols the columns to search frequent items in
   * @param support The minimum frequency for an item to be considered `frequent`. Should be greater
   *                than 1e-4.
   * @return A Local DataFrame with the Array of frequent items for each column.
   */
  def singlePassFreqItemsByColumns(
      df: DataFrame,
      cols: Seq[Column],
      support: Double): DataFrame = {
    require(support >= 1e-4 && support <= 1.0, s"Support must be in [1e-4, 1], but got $support.")
    val numCols = cols.length
    // number of max items to keep counts for
    val sizeOfMap = (1 / support).toInt
    val countMaps = Seq.tabulate(numCols)(i => new FreqItemCounter(sizeOfMap))

    val freqItems = df.select(cols: _*).rdd.treeAggregate(countMaps)(
      seqOp = (counts, row) => {
        var i = 0
        while (i < numCols) {
          val thisMap = counts(i)
          val key = row.get(i)
          thisMap.add(key, 1L)
          i += 1
        }
        counts
      },
      combOp = (baseCounts, counts) => {
        var i = 0
        while (i < numCols) {
          baseCounts(i).merge(counts(i))
          i += 1
        }
        baseCounts
      }
    )
    val justItems = freqItems.map(m => m.baseMap.keys.toArray)
    val resultRow = Row(justItems : _*)

    val outputCols = cols.map { col =>
      val originalColumn = resolveColumn(df, col)
      val originalField = originalColumn.expr
      val name = originalColumn.named.name

      // append frequent Items to the column name for easy debugging
      StructField(name + "_freqItems", ArrayType(originalField.dataType, originalField.nullable))
    }.toArray

    val schema = StructType(outputCols).toAttributes
    Dataset.ofRows(df.sparkSession, LocalRelation.fromExternalRows(schema, Seq(resultRow)))
  }
}
