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

package org.apache.spark.mllib.stat.correlation

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Logging, HashPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{DenseVector, Matrix, Vector}
import org.apache.spark.rdd.{CoGroupedRDD, RDD}

/**
 * Compute Spearman's correlation for two RDDs of the type RDD[Double] or the correlation matrix
 * for an RDD of the type RDD[Vector].
 *
 * Definition of Spearman's correlation can be found at
 * http://en.wikipedia.org/wiki/Spearman's_rank_correlation_coefficient
 */
private[stat] object SpearmansCorrelation extends Correlation with Logging {

  /**
   * Compute Spearman's correlation for two datasets.
   */
  override def computeCorrelation(x: RDD[Double], y: RDD[Double]): Double = {
    computeCorrelationWithMatrixImpl(x, y)
  }

  /**
   * Compute Spearman's correlation matrix S, for the input matrix, where S(i, j) is the
   * correlation between column i and j.
   *
   * Input RDD[Vector] should be cached or checkpointed if possible since it would be split into
   * numCol RDD[Double]s, each of which sorted, and the joined back into a single RDD[Vector].
   */
  override def computeCorrelationMatrix(X: RDD[Vector]): Matrix = {
    val indexed = X.zipWithUniqueId()

    val numCols = X.first.size
    if (numCols > 50) {
      logWarning("Computing the Spearman correlation matrix can be slow for large RDDs with more"
        + " than 50 columns.")
    }
    val ranks = new Array[RDD[(Long, Double)]](numCols)

    // Note: we use a for loop here instead of a while loop with a single index variable
    // to avoid race condition caused by closure serialization
    for (k <- 0 until numCols) {
      val column = indexed.map { case (vector, index) => (vector(k), index) }
      ranks(k) = getRanks(column)
    }

    val ranksMat: RDD[Vector] = makeRankMatrix(ranks, X)
    PearsonCorrelation.computeCorrelationMatrix(ranksMat)
  }

  /**
   * Compute the ranks for elements in the input RDD, using the average method for ties.
   *
   * With the average method, elements with the same value receive the same rank that's computed
   * by taking the average of their positions in the sorted list.
   * e.g. ranks([2, 1, 0, 2]) = [2.5, 1.0, 0.0, 2.5]
   * Note that positions here are 0-indexed, instead of the 1-indexed as in the definition for
   * ranks in the standard definition for Spearman's correlation. This does not affect the final
   * results and is slightly more performant.
   *
   * @param indexed RDD[(Double, Long)] containing pairs of the format (originalValue, uniqueId)
   * @return RDD[(Long, Double)] containing pairs of the format (uniqueId, rank), where uniqueId is
   *         copied from the input RDD.
   */
  private def getRanks(indexed: RDD[(Double, Long)]): RDD[(Long, Double)] = {
    // Get elements' positions in the sorted list for computing average rank for duplicate values
    val sorted = indexed.sortByKey().zipWithIndex()

    val ranks: RDD[(Long, Double)] = sorted.mapPartitions { iter =>
      // add an extra element to signify the end of the list so that flatMap can flush the last
      // batch of duplicates
      val padded = iter ++
        Iterator[((Double, Long), Long)](((Double.NaN, Long.MinValue), Long.MinValue))
      var lastVal = 0.0
      var firstRank = 0.0
      val IDBuffer = new ArrayBuffer[Long]()
      padded.flatMap { item =>
        val rank = item._2
        if (item._1._1  == lastVal && item._2 != Long.MinValue) {
          IDBuffer += item._1._2
          Iterator.empty
        } else {
          val entries = if (IDBuffer.size == 0) {
            Iterator.empty
          } else if (IDBuffer.size == 1) {
            Iterator((IDBuffer(0), firstRank))
          } else {
            // averageRank = ((firstRank + IDBuffer.size) / 2.0) / IDBuffer.size
            val averageRank = firstRank / (2 * IDBuffer.size) + 0.5
            IDBuffer.map(id => (id, averageRank))
          }
          lastVal = item._1._1
          firstRank = rank
          IDBuffer.clear()
          IDBuffer += item._1._2
          entries
        }
      }
    }
    ranks
  }

  private def makeRankMatrix(ranks: Array[RDD[(Long, Double)]], input: RDD[Vector]): RDD[Vector] = {
    val partitioner = new HashPartitioner(input.partitions.size)
    val cogrouped = new CoGroupedRDD[Long](ranks, partitioner)
    cogrouped.map { case (_, values: Seq[Seq[Double]]) => new DenseVector(values.flatten.toArray) }
  }
}
