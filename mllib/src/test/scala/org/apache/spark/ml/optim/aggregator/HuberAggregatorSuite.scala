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
package org.apache.spark.ml.optim.aggregator

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext

class HuberAggregatorSuite extends SparkFunSuite with MLlibTestSparkContext {

  import DifferentiableLossAggregatorSuite.getRegressionSummarizers

  @transient var instances: Array[Instance] = _
  @transient var instancesConstantFeature: Array[Instance] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    instances = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.5, 1.0)),
      Instance(2.0, 0.3, Vectors.dense(4.0, 0.5))
    )
    instancesConstantFeature = Array(
      Instance(0.0, 0.1, Vectors.dense(1.0, 2.0)),
      Instance(1.0, 0.5, Vectors.dense(1.0, 1.0)),
      Instance(2.0, 0.3, Vectors.dense(1.0, 0.5))
    )
  }

  /** Get summary statistics for some data and create a new HuberAggregator. */
  private def getNewAggregator(
      instances: Array[Instance],
      parameters: Vector,
      fitIntercept: Boolean,
      m: Double): HuberAggregator = {
    val (featuresSummarizer, _) = getRegressionSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val bcFeaturesStd = spark.sparkContext.broadcast(featuresStd)
    val bcParameters = spark.sparkContext.broadcast(parameters)
    new HuberAggregator(fitIntercept, m, bcFeaturesStd)(bcParameters)
  }

  test("aggregator add method should check input size") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val agg = getNewAggregator(instances, parameters, fitIntercept = true, m = 1.35)
    withClue("HuberAggregator features dimension must match parameters dimension") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(2.0)))
      }
    }
  }

  test("negative weight") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val agg = getNewAggregator(instances, parameters, fitIntercept = true, m = 1.35)
    withClue("HuberAggregator does not support negative instance weights.") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, -1.0, Vectors.dense(2.0, 1.0)))
      }
    }
  }

  test("check sizes") {
    val paramWithIntercept = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val paramWithoutIntercept = Vectors.dense(1.0, 2.0, 4.0)
    val aggIntercept = getNewAggregator(instances, paramWithIntercept,
      fitIntercept = true, m = 1.35)
    val aggNoIntercept = getNewAggregator(instances, paramWithoutIntercept,
      fitIntercept = false, m = 1.35)
    instances.foreach(aggIntercept.add)
    instances.foreach(aggNoIntercept.add)

    assert(aggIntercept.gradient.size === 4)
    assert(aggNoIntercept.gradient.size === 3)
  }

  test("check correctness") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val numFeatures = 2
    val (featuresSummarizer, _) = getRegressionSummarizers(instances)
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val m = 1.35
    val weightSum = instances.map(_.weight).sum

    val agg = getNewAggregator(instances, parameters, fitIntercept = true, m)
    instances.foreach(agg.add)

    // compute expected loss sum
    val coefficients = parameters.toArray.slice(0, 2)
    val intercept = parameters(2)
    val sigma = parameters(3)
    val stdCoef = coefficients.indices.map(i => coefficients(i) / featuresStd(i)).toArray
    val lossSum = instances.map { case Instance(label, weight, features) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), features) + intercept
      val linearLoss = label - margin
      if (math.abs(linearLoss) <= sigma * m) {
        0.5 * weight * (sigma +  math.pow(linearLoss, 2.0) / sigma)
      } else {
        0.5 * weight * (sigma + 2.0 * m * math.abs(linearLoss) - sigma * m * m)
      }
    }.sum
    val loss = lossSum / weightSum

    // compute expected gradients
    val gradientCoef = new Array[Double](numFeatures + 2)
    instances.foreach { case Instance(label, weight, features) =>
      val margin = BLAS.dot(Vectors.dense(stdCoef), features) + intercept
      val linearLoss = label - margin
      if (math.abs(linearLoss) <= sigma * m) {
        features.toArray.indices.foreach { i =>
          gradientCoef(i) +=
            0.5 * weight * -2.0 * linearLoss / sigma * (features(i) / featuresStd(i))
        }
        gradientCoef(2) += 0.5 * weight * -2.0 * linearLoss / sigma
        gradientCoef(3) += 0.5 * weight * (1.0 - math.pow(linearLoss / sigma, 2.0))
      } else {
        val sign = if (linearLoss >= 0) -1.0 else 1.0
        features.toArray.indices.foreach { i =>
          gradientCoef(i) += 0.5 * weight * sign * 2.0 * m * (features(i) / featuresStd(i))
        }
        gradientCoef(2) += 0.5 * weight * (sign * 2.0 * m)
        gradientCoef(3) += 0.5 * weight * (1.0 - m * m)
      }
    }
    val gradient = Vectors.dense(gradientCoef.map(_ / weightSum))

    assert(loss ~== agg.loss relTol 0.01)
    assert(gradient ~== agg.gradient relTol 0.01)
  }

  test("check with zero standard deviation") {
    val parameters = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    val aggConstantFeature = getNewAggregator(instancesConstantFeature, parameters,
      fitIntercept = true, m = 1.35)
    instances.foreach(aggConstantFeature.add)
    // constant features should not affect gradient
    assert(aggConstantFeature.gradient(0) === 0.0)
    assert(!aggConstantFeature.gradient(1).isNaN && !aggConstantFeature.gradient(1).isInfinity)
  }
}
