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

package org.apache.spark.util.random

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.commons.math3.distribution.PoissonDistribution

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

/**
 * Auxiliary functions and data structures for the sampleByKey method in PairRDDFunctions.
 *
 * Essentially, when exact sample size is necessary, we make additional passes over the RDD to
 * compute the exact threshold value to use for each stratum to guarantee exact sample size with
 * high probability. This is achieved by maintaining a waitlist of size O(log(s)), where s is the
 * desired sample size for each stratum.
 *
 * Like in simple random sampling, we generate a random value for each item from the
 * uniform  distribution [0.0, 1.0]. All items with values <= min(values of items in the waitlist)
 * are accepted into the sample instantly. The threshold for instant accept is designed so that
 * s - numAccepted = O(sqrt(s)), where s is again the desired sample size. Thus, by maintaining a
 * waitlist size = O(sqrt(s)), we will be able to create a sample of the exact size s by adding
 * a portion of the waitlist to the set of items that are instantly accepted. The exact threshold
 * is computed by sorting the values in the waitlist and picking the value at (s - numAccepted).
 *
 * Note that since we use the same seed for the RNG when computing the thresholds and the actual
 * sample, our computed thresholds are guaranteed to produce the desired sample size.
 *
 * For more theoretical background on the sampling techniques used here, please refer to
 * http://jmlr.org/proceedings/papers/v28/meng13a.html
 */

private[spark] object StratifiedSamplingUtils extends Logging {

  type StratifiedSamplingFunc[K, V] = (Int, Iterator[(K, V)]) => Iterator[(K, V)]

  /**
   * Count the number of items instantly accepted and generate the waitlist for each stratum.
   *
   * This is only invoked when exact sample size is required.
   */
  def getAcceptanceResults[K, V](rdd: RDD[(K, V)],
      withReplacement: Boolean,
      fractions: Seq[Map[K, Double]],
      counts: Option[Map[K, Long]],
      seed: Long): Seq[mutable.Map[K, AcceptanceResult]] = {
    val combOp = getCombOp[K]
    val mappedPartitionRDD = rdd.mapPartitionsWithIndex { case (partition, iter) =>
      val zeroU: Array[mutable.Map[K, AcceptanceResult]] = Array.fill(fractions.length) {
        new mutable.HashMap[K, AcceptanceResult]()
      }
      val rngs = Array.fill(fractions.length) {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + partition)
        rng
      }
      val seqOp = getSeqOp(withReplacement, fractions, rngs, counts)
      Iterator(iter.aggregate(zeroU)(seqOp, combOp))
    }
    mappedPartitionRDD.reduce(combOp)
  }

  /**
   * Convenience version of [[getAcceptanceResults()]] for a single sample.
   */
  def getAcceptanceResults[K, V](
      rdd: RDD[(K, V)],
      withReplacement: Boolean,
      fractions: Map[K, Double],
      counts: Option[Map[K, Long]],
      seed: Long): mutable.Map[K, AcceptanceResult] = {
    getAcceptanceResults(rdd, withReplacement, Seq(fractions), counts, seed).head
  }

  /**
   * Returns the function used by aggregate to collect sampling statistics for each partition.
   */
  def getSeqOp[K, V](withReplacement: Boolean,
      fractions: Seq[Map[K, Double]],
      rngs: Array[RandomDataGenerator],
      counts: Option[Map[K, Long]]):
    (Array[mutable.Map[K, AcceptanceResult]], (K, V)) => Array[mutable.Map[K, AcceptanceResult]] = {
    val delta = 5e-5
    (results: Array[mutable.Map[K, AcceptanceResult]], item: (K, V)) => {
      val key = item._1

      var j = 0
      while (j < fractions.length) {
        val fraction = fractions(j)(key)
        if (!results(j).contains(key)) {
          results(j) += (key -> new AcceptanceResult())
        }
        val acceptResult = results(j)(key)

        if (withReplacement) {
          // compute acceptBound and waitListBound only if they haven't been computed already
          // since they don't change from iteration to iteration.
          // TODO change this to the streaming version
          if (acceptResult.areBoundsEmpty) {
            val n = counts.get(key)
            val sampleSize = math.ceil(n * fraction).toLong
            val lmbd1 = PoissonBounds.getLowerBound(sampleSize)
            val lmbd2 = PoissonBounds.getUpperBound(sampleSize)
            acceptResult.acceptBound = lmbd1 / n
            acceptResult.waitListBound = (lmbd2 - lmbd1) / n
          }
          val acceptBound = acceptResult.acceptBound
          val copiesAccepted = if (acceptBound == 0.0) 0L else rngs(j).nextPoisson(acceptBound)
          if (copiesAccepted > 0) {
            acceptResult.numAccepted += copiesAccepted
          }
          val copiesWaitlisted = rngs(j).nextPoisson(acceptResult.waitListBound)
          if (copiesWaitlisted > 0) {
            acceptResult.waitList ++= ArrayBuffer.fill(copiesWaitlisted)(rngs(j).nextUniform())
          }
        } else {
          // We use the streaming version of the algorithm for sampling without replacement to avoid
          // using an extra pass over the RDD for computing the count.
          // Hence, acceptBound and waitListBound change on every iteration.
          acceptResult.acceptBound =
            BinomialBounds.getLowerBound(delta, acceptResult.numItems, fraction)
          acceptResult.waitListBound =
            BinomialBounds.getUpperBound(delta, acceptResult.numItems, fraction)

          val x = rngs(j).nextUniform()
          if (x < acceptResult.acceptBound) {
            acceptResult.numAccepted += 1
          } else if (x < acceptResult.waitListBound) {
            acceptResult.waitList += x
          }
        }
        acceptResult.numItems += 1

        j += 1
      }
      results
    }
  }

  /**
   * Returns the function used combine results returned by seqOp from different partitions.
   */
  def getCombOp[K]: (Array[mutable.Map[K, AcceptanceResult]],
    Array[mutable.Map[K, AcceptanceResult]]) => Array[mutable.Map[K, AcceptanceResult]] = {
    (result1: Array[mutable.Map[K, AcceptanceResult]],
     result2: Array[mutable.Map[K, AcceptanceResult]]) => {
      var j = 0
      while (j < result1.length) {
        // take union of both key sets in case one partition doesn't contain all keys
        result1(j).keySet.union(result2(j).keySet).foreach { key =>
          // Use result2 to keep the combined result since r1 is usual empty
          val entry1 = result1(j).get(key)
          if (result2(j).contains(key)) {
            result2(j)(key).merge(entry1)
          } else {
            if (entry1.isDefined) {
              result2(j) += (key -> entry1.get)
            }
          }
        }
        j += 1
      }
      result2
    }
  }

  /**
   * Given the result returned by getCounts, determine the threshold for accepting items to
   * generate exact sample size.
   *
   * To do so, we compute sampleSize = math.ceil(size * samplingRate) for each stratum and compare
   * it to the number of items that were accepted instantly and the number of items in the waitlist
   * for that stratum. Most of the time, numAccepted <= sampleSize <= (numAccepted + numWaitlisted),
   * which means we need to sort the elements in the waitlist by their associated values in order
   * to find the value T s.t. |{elements in the stratum whose associated values <= T}| = sampleSize.
   * Note that all elements in the waitlist have values >= bound for instant accept, so a T value
   * in the waitlist range would allow all elements that were instantly accepted on the first pass
   * to be included in the sample.
   */
  def computeThresholdByKey[K](finalResult: Map[K, AcceptanceResult],
      fractions: Map[K, Double]): Map[K, Double] = {
    val thresholdByKey = new mutable.HashMap[K, Double]()
    for ((key, acceptResult) <- finalResult) {
      val sampleSize = math.ceil(acceptResult.numItems * fractions(key)).toLong
      if (acceptResult.numAccepted > sampleSize) {
        logWarning("Pre-accepted too many")
        thresholdByKey += (key -> acceptResult.acceptBound)
      } else {
        val numWaitListAccepted = (sampleSize - acceptResult.numAccepted).toInt
        if (numWaitListAccepted >= acceptResult.waitList.size) {
          logWarning("WaitList too short")
          thresholdByKey += (key -> acceptResult.waitListBound)
        } else {
          thresholdByKey += (key -> acceptResult.waitList.sorted.apply(numWaitListAccepted))
        }
      }
    }
    thresholdByKey
  }

  /**
   * Convenience version of [[getBernoulliSamplingFunction()]] for a single split.
   */
  def getBernoulliSamplingFunction[K: ClassTag, V: ClassTag](
      rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): (StratifiedSamplingFunc[K, V], StratifiedSamplingFunc[K, V]) = {
    val complementFractions = fractions.map { case (k, v) => k -> (1.0 - v) }
    getBernoulliCellSamplingFunctions(rdd, Seq(fractions, complementFractions), exact, seed).head
  }

  /**
   * Return the per partition sampling function used for sampling without replacement.
   *
   * When exact sample size is required, we make an additional pass over the RDD to determine the
   * exact sampling rate that guarantees sample size with high confidence.
   *
   * The sampling function has a unique seed per partition.
   */
  def getBernoulliCellSamplingFunctions[K: ClassTag, V: ClassTag](
      rdd: RDD[(K, V)],
      fractions: Seq[Map[K, Double]],
      exact: Boolean,
      seed: Long): Seq[(StratifiedSamplingFunc[K, V], StratifiedSamplingFunc[K, V])] = {
    val thresholds = splitFractionsToSplitPoints(fractions)
    val innerThresholds = if (exact) {
      val finalResults =
        getAcceptanceResults(rdd, withReplacement = false, thresholds, None, seed)
      finalResults.zip(thresholds).map { case (finalResult, thresh) =>
        computeThresholdByKey(finalResult, thresh)
      }
    } else {
      thresholds
    }
    val leftBound = fractions.head.map { case (k, v) => (k, 0.0)}
    val rightBound = fractions.head.map { case (k, v) => (k, 1.0)}
    val outerThresholds = leftBound +: innerThresholds :+ rightBound
    outerThresholds.sliding(2).map { case Seq(lb, ub) =>
      (getBernoulliCellSamplingFunction[K, V](lb, ub, seed),
        getBernoulliCellSamplingFunction[K, V](lb, ub, seed, complement = true))
    }.toSeq
  }

  /**
   * Helper function to cumulative sum a sequence of Maps.
   */
  private def splitFractionsToSplitPoints[K](
      fractions: Seq[Map[K, Double]]): Seq[Map[K, Double]] = {
    val acc = new mutable.HashMap[K, Double]()
    fractions.map { case splitWeights =>
      splitWeights.map { case (k, v) =>
        val thisKeySum = acc.getOrElseUpdate(k, 0.0)
        acc(k) += v
        k -> (v + thisKeySum)
      }
    }.dropRight(1)
  }

  /**
   * Return the per partition sampling function used for partitioning a dataset without
   * replacement.
   *
   * The sampling function has a unique seed per partition.
   */
  def getBernoulliCellSamplingFunction[K, V](
      lb: Map[K, Double],
      ub: Map[K, Double],
      seed: Long,
      complement: Boolean = false): StratifiedSamplingFunc[K, V] = {
    (idx: Int, iter: Iterator[(K, V)]) => {
      val rng = new RandomDataGenerator()
      rng.reSeed(seed + idx)

      if (complement) {
        iter.filter { case (k, _) =>
          val x = rng.nextUniform()
          (x < lb(k)) || (x >= ub(k))
        }
      } else {
        iter.filter { case (k, _) =>
          val x = rng.nextUniform()
          (x >= lb(k)) && (x < ub(k))
        }
      }
    }
  }

  /**
   * Return the per partition sampling function used for sampling with replacement.
   *
   * When exact sample size is required, we make two additional passed over the RDD to determine
   * the exact sampling rate that guarantees sample size with high confidence. The first pass
   * counts the number of items in each stratum (group of items with the same key) in the RDD, and
   * the second pass uses the counts to determine exact sampling rates.
   *
   * The sampling function has a unique seed per partition.
   */
  def getPoissonSamplingFunction[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)],
      fractions: Map[K, Double],
      exact: Boolean,
      seed: Long): StratifiedSamplingFunc[K, V] = {
    // TODO implement the streaming version of sampling w/ replacement that doesn't require counts
    if (exact) {
      val counts = Some(rdd.countByKey())
      val finalResult = getAcceptanceResults(rdd, true, fractions, counts, seed)
      val thresholdByKey = computeThresholdByKey(finalResult, fractions)
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val key = item._1
          val acceptBound = finalResult(key).acceptBound
          // Must use the same invoke pattern on the rng as in getSeqOp for with replacement
          // in order to generate the same sequence of random numbers when creating the sample
          val copiesAccepted = if (acceptBound == 0) 0L else rng.nextPoisson(acceptBound)
          val copiesWaitlisted = rng.nextPoisson(finalResult(key).waitListBound)
          val copiesInSample = copiesAccepted +
            (0 until copiesWaitlisted).count(i => rng.nextUniform() < thresholdByKey(key))
          if (copiesInSample > 0) {
            Iterator.fill(copiesInSample.toInt)(item)
          } else {
            Iterator.empty
          }
        }
      }
    } else {
      (idx: Int, iter: Iterator[(K, V)]) => {
        val rng = new RandomDataGenerator()
        rng.reSeed(seed + idx)
        iter.flatMap { item =>
          val count = rng.nextPoisson(fractions(item._1))
          if (count == 0) {
            Iterator.empty
          } else {
            Iterator.fill(count)(item)
          }
        }
      }
    }
  }

  /** A random data generator that generates both uniform values and Poisson values. */
  private class RandomDataGenerator {
    val uniform = new XORShiftRandom()
    // commons-math3 doesn't have a method to generate Poisson from an arbitrary mean;
    // maintain a cache of Poisson(m) distributions for various m
    val poissonCache = mutable.Map[Double, PoissonDistribution]()
    var poissonSeed = 0L

    def reSeed(seed: Long): Unit = {
      uniform.setSeed(seed)
      poissonSeed = seed
      poissonCache.clear()
    }

    def nextPoisson(mean: Double): Int = {
      val poisson = poissonCache.getOrElseUpdate(mean, {
        val newPoisson = new PoissonDistribution(mean)
        newPoisson.reseedRandomGenerator(poissonSeed)
        newPoisson
      })
      poisson.sample()
    }

    def nextUniform(): Double = {
      uniform.nextDouble()
    }
  }
}

/**
 * Object used by seqOp to keep track of the number of items accepted and items waitlisted per
 * stratum, as well as the bounds for accepting and waitlisting items.
 *
 * `[random]` here is necessary since it's in the return type signature of seqOp defined above
 */
private[random] class AcceptanceResult(var numItems: Long = 0L, var numAccepted: Long = 0L)
  extends Serializable {

  val waitList = new ArrayBuffer[Double]
  var acceptBound: Double = Double.NaN // upper bound for accepting item instantly
  var waitListBound: Double = Double.NaN // upper bound for adding item to waitlist

  def areBoundsEmpty: Boolean = acceptBound.isNaN || waitListBound.isNaN

  def merge(other: Option[AcceptanceResult]): Unit = {
    if (other.isDefined) {
      waitList ++= other.get.waitList
      numAccepted += other.get.numAccepted
      numItems += other.get.numItems
    }
  }
}
