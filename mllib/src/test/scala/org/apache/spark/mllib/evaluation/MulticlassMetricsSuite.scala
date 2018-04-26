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

package org.apache.spark.mllib.evaluation

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MulticlassMetricsSuite extends SparkFunSuite with MLlibTestSparkContext {

  import testImplicits._

  val delta = 1e-7

  test("Multiclass evaluation metrics") {
    /*
     * Confusion matrix for 3-class classification with total 9 instances:
     * |2|1|1| true class0 (4 instances)
     * |1|3|0| true class1 (4 instances)
     * |0|0|1| true class2 (1 instance)
     */
    val confusionMatrix = Matrices.dense(3, 3, Array(2, 1, 0, 1, 3, 0, 1, 0, 1))
    val labels = Array(0.0, 1.0, 2.0)
    val predictionAndLabels = sc.parallelize(
      Seq((0.0, 0.0), (0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0),
        (1.0, 1.0), (1.0, 1.0), (2.0, 2.0), (2.0, 0.0)), 2)
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val tpRate0 = 2.0 / (2 + 2)
    val tpRate1 = 3.0 / (3 + 1)
    val tpRate2 = 1.0 / (1 + 0)
    val fpRate0 = 1.0 / (9 - 4)
    val fpRate1 = 1.0 / (9 - 4)
    val fpRate2 = 1.0 / (9 - 1)
    val precision0 = 2.0 / (2 + 1)
    val precision1 = 3.0 / (3 + 1)
    val precision2 = 1.0 / (1 + 1)
    val recall0 = 2.0 / (2 + 2)
    val recall1 = 3.0 / (3 + 1)
    val recall2 = 1.0 / (1 + 0)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val f2measure0 = (1 + 2 * 2) * precision0 * recall0 / (2 * 2 * precision0 + recall0)
    val f2measure1 = (1 + 2 * 2) * precision1 * recall1 / (2 * 2 * precision1 + recall1)
    val f2measure2 = (1 + 2 * 2) * precision2 * recall2 / (2 * 2 * precision2 + recall2)

    assert(metrics.confusionMatrix.asML ~== confusionMatrix.asML relTol delta)
    assert(metrics.truePositiveRate(0.0) ~== tpRate0 absTol delta)
    assert(metrics.truePositiveRate(1.0) ~== tpRate1 absTol delta)
    assert(metrics.truePositiveRate(2.0) ~== tpRate2 absTol delta)
    assert(metrics.falsePositiveRate(0.0) ~== fpRate0 absTol delta)
    assert(metrics.falsePositiveRate(1.0) ~== fpRate1 absTol delta)
    assert(metrics.falsePositiveRate(2.0) ~== fpRate2 absTol delta)
    assert(metrics.precision(0.0) ~== precision0 absTol delta)
    assert(metrics.precision(1.0) ~== precision1 absTol delta)
    assert(metrics.precision(2.0) ~== precision2 absTol delta)
    assert(metrics.recall(0.0) ~== recall0 absTol delta)
    assert(metrics.recall(1.0) ~== recall1 absTol delta)
    assert(metrics.recall(2.0) ~== recall2 absTol delta)
    assert(metrics.fMeasure(0.0) ~== f1measure0 absTol delta)
    assert(metrics.fMeasure(1.0) ~== f1measure1 absTol delta)
    assert(metrics.fMeasure(2.0) ~== f1measure2 absTol delta)
    assert(metrics.fMeasure(0.0, 2.0) ~== f2measure0 absTol delta)
    assert(metrics.fMeasure(1.0, 2.0) ~== f2measure1 absTol delta)
    assert(metrics.fMeasure(2.0, 2.0) ~== f2measure2 absTol delta)

    assert(metrics.accuracy ~==
      (2.0 + 3.0 + 1.0) / ((2 + 3 + 1) + (1 + 1 + 1)) absTol delta)
    assert(metrics.accuracy ~== metrics.weightedRecall absTol delta)
    val weight0 = 4.0 / 9
    val weight1 = 4.0 / 9
    val weight2 = 1.0 / 9
    assert(metrics.weightedTruePositiveRate ~==
      (weight0 * tpRate0 + weight1 * tpRate1 + weight2 * tpRate2) absTol delta)
    assert(metrics.weightedFalsePositiveRate ~==
      (weight0 * fpRate0 + weight1 * fpRate1 + weight2 * fpRate2) absTol delta)
    assert(metrics.weightedPrecision ~==
      (weight0 * precision0 + weight1 * precision1 + weight2 * precision2) absTol delta)
    assert(metrics.weightedRecall ~==
      (weight0 * recall0 + weight1 * recall1 + weight2 * recall2) absTol delta)
    assert(metrics.weightedFMeasure ~==
      (weight0 * f1measure0 + weight1 * f1measure1 + weight2 * f1measure2) absTol delta)
    assert(metrics.weightedFMeasure(2.0) ~==
      (weight0 * f2measure0 + weight1 * f2measure1 + weight2 * f2measure2) absTol delta)
    assert(metrics.labels === labels)
  }

  test("Multiclass evaluation metrics with weights") {
    /*
     * Confusion matrix for 3-class classification with total 9 instances with 2 weights:
     * |2 * w1|1 * w2         |1 * w1| true class0 (4 instances)
     * |1 * w2|2 * w1 + 1 * w2|0     | true class1 (4 instances)
     * |0     |0              |1 * w2| true class2 (1 instance)
     */
    val w1 = 2.2
    val w2 = 1.5
    val tw = 2.0 * w1 + 1.0 * w2 + 1.0 * w1 + 1.0 * w2 + 2.0 * w1 + 1.0 * w2 + 1.0 * w2
    val confusionMatrix = Matrices.dense(3, 3,
      Array(2 * w1, 1 * w2, 0, 1 * w2, 2 * w1 + 1 * w2, 0, 1 * w1, 0, 1 * w2))
    val labels = Array(0.0, 1.0, 2.0)
    val predictionAndLabelsWithWeights = sc.parallelize(
      Seq((0.0, 0.0, w1), (0.0, 1.0, w2), (0.0, 0.0, w1), (1.0, 0.0, w2),
        (1.0, 1.0, w1), (1.0, 1.0, w2), (1.0, 1.0, w1), (2.0, 2.0, w2),
        (2.0, 0.0, w1)), 2)
    val metrics = new MulticlassMetrics(predictionAndLabelsWithWeights)
    val tpRate0 = (2.0 * w1) / (2.0 * w1 + 1.0 * w2 + 1.0 * w1)
    val tpRate1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val tpRate2 = (1.0 * w2) / (1.0 * w2 + 0)
    val fpRate0 = (1.0 * w2) / (tw - (2.0 * w1 + 1.0 * w2 + 1.0 * w1))
    val fpRate1 = (1.0 * w2) / (tw - (1.0 * w2 + 2.0 * w1 + 1.0 * w2))
    val fpRate2 = (1.0 * w1) / (tw - (1.0 * w2))
    val precision0 = (2.0 * w1) / (2 * w1 + 1 * w2)
    val precision1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val precision2 = (1.0 * w2) / (1 * w1 + 1 * w2)
    val recall0 = (2.0 * w1) / (2.0 * w1 + 1.0 * w2 + 1.0 * w1)
    val recall1 = (2.0 * w1 + 1.0 * w2) / (2.0 * w1 + 1.0 * w2 + 1.0 * w2)
    val recall2 = (1.0 * w2) / (1.0 * w2 + 0)
    val f1measure0 = 2 * precision0 * recall0 / (precision0 + recall0)
    val f1measure1 = 2 * precision1 * recall1 / (precision1 + recall1)
    val f1measure2 = 2 * precision2 * recall2 / (precision2 + recall2)
    val f2measure0 = (1 + 2 * 2) * precision0 * recall0 / (2 * 2 * precision0 + recall0)
    val f2measure1 = (1 + 2 * 2) * precision1 * recall1 / (2 * 2 * precision1 + recall1)
    val f2measure2 = (1 + 2 * 2) * precision2 * recall2 / (2 * 2 * precision2 + recall2)

    assert(metrics.confusionMatrix.asML ~== confusionMatrix.asML relTol delta)
    assert(metrics.truePositiveRate(0.0) ~== tpRate0 absTol delta)
    assert(metrics.truePositiveRate(1.0) ~== tpRate1 absTol delta)
    assert(metrics.truePositiveRate(2.0)  ~==  tpRate2 absTol delta)
    assert(metrics.falsePositiveRate(0.0)  ~==  fpRate0 absTol delta)
    assert(metrics.falsePositiveRate(1.0)  ~==  fpRate1 absTol delta)
    assert(metrics.falsePositiveRate(2.0)  ~==  fpRate2 absTol delta)
    assert(metrics.precision(0.0)  ~==  precision0 absTol delta)
    assert(metrics.precision(1.0)  ~==  precision1 absTol delta)
    assert(metrics.precision(2.0)  ~==  precision2 absTol delta)
    assert(metrics.recall(0.0)  ~==  recall0 absTol delta)
    assert(metrics.recall(1.0)  ~==  recall1 absTol delta)
    assert(metrics.recall(2.0)  ~==  recall2 absTol delta)
    assert(metrics.fMeasure(0.0)  ~==  f1measure0 absTol delta)
    assert(metrics.fMeasure(1.0)  ~==  f1measure1 absTol delta)
    assert(metrics.fMeasure(2.0)  ~==  f1measure2 absTol delta)
    assert(metrics.fMeasure(0.0, 2.0)  ~==  f2measure0 absTol delta)
    assert(metrics.fMeasure(1.0, 2.0)  ~==  f2measure1 absTol delta)
    assert(metrics.fMeasure(2.0, 2.0)  ~==  f2measure2 absTol delta)

    assert(metrics.accuracy  ~==
      (2.0 * w1 + 2.0 * w1 + 1.0 * w2 + 1.0 * w2) / tw absTol delta)
    assert(metrics.accuracy  ~==  metrics.precision absTol delta)
    assert(metrics.accuracy  ~==  metrics.recall absTol delta)
    assert(metrics.accuracy  ~==  metrics.fMeasure absTol delta)
    assert(metrics.accuracy  ~==  metrics.weightedRecall absTol delta)
    val weight0 = (2 * w1 + 1 * w2 + 1 * w1) / tw
    val weight1 = (1 * w2 + 2 * w1 + 1 * w2) / tw
    val weight2 = 1 * w2 / tw
    assert(metrics.weightedTruePositiveRate  ~==
      (weight0 * tpRate0 + weight1 * tpRate1 + weight2 * tpRate2) absTol delta)
    assert(metrics.weightedFalsePositiveRate  ~==
      (weight0 * fpRate0 + weight1 * fpRate1 + weight2 * fpRate2) absTol delta)
    assert(metrics.weightedPrecision  ~==
      (weight0 * precision0 + weight1 * precision1 + weight2 * precision2) absTol delta)
    assert(metrics.weightedRecall  ~==
      (weight0 * recall0 + weight1 * recall1 + weight2 * recall2) absTol delta)
    assert(metrics.weightedFMeasure  ~==
      (weight0 * f1measure0 + weight1 * f1measure1 + weight2 * f1measure2) absTol delta)
    assert(metrics.weightedFMeasure(2.0)  ~==
      (weight0 * f2measure0 + weight1 * f2measure1 + weight2 * f2measure2) absTol delta)
    assert(metrics.labels === labels)
  }
}
