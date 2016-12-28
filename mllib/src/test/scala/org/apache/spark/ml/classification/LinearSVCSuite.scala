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

package org.apache.spark.ml.classification

import scala.util.Random

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.LinearSVCSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}


class LinearSVCSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  private val nPoints = 50
  @transient var smallBinaryDataset: Dataset[_] = _
  @transient var smallValidationDataset: Dataset[_] = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0
    smallBinaryDataset = generateSVMInput(A, Array[Double](B, C), nPoints, 42).toDF()
    smallValidationDataset = generateSVMInput(A, Array[Double](B, C), nPoints, 17).toDF()
  }

  test("Linear SVC binary classification") {
    val svm = new LinearSVC()
    val model = svm.fit(smallBinaryDataset)
    assert(model.transform(smallValidationDataset)
      .where("prediction=label").count() > nPoints * 0.8)
  }

  test("Linear SVC binary classification with regularization") {
    val svm = new LinearSVC()
    val model = svm.setRegParam(0.1).fit(smallBinaryDataset)
    assert(model.transform(smallValidationDataset)
      .where("prediction=label").count() > nPoints * 0.8)
  }

  test("params") {
    ParamsSuite.checkParams(new LinearSVC)
    val model = new LinearSVCModel("linearSVC", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("linear svc: default params") {
    val lsvc = new LinearSVC()
    assert(lsvc.getLabelCol === "label")
    assert(lsvc.getFeaturesCol === "features")
    assert(lsvc.getPredictionCol === "prediction")
    assert(lsvc.getRawPredictionCol === "rawPrediction")
    assert(!lsvc.isDefined(lsvc.weightCol))
    assert(lsvc.getFitIntercept)
    assert(lsvc.getStandardization)
    val model = lsvc.setMaxIter(2).fit(smallBinaryDataset)
    model.transform(smallBinaryDataset)
      .select("label", "prediction", "rawPrediction")
      .collect()
    assert(model.getThreshold === 0.0)
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
    assert(model.numFeatures === 2)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("linear svc doesn't fit intercept when fitIntercept is off") {
    val lsvc = new LinearSVC().setFitIntercept(false)
    val model = lsvc.fit(smallBinaryDataset)
    assert(model.intercept === 0.0)
  }

  test("linearSVC with sample weights") {
    def modelEquals(m1: LinearSVCModel, m2: LinearSVCModel): Unit = {
      assert(m1.coefficients ~== m2.coefficients absTol 0.05)
      assert(m1.intercept ~== m2.intercept absTol 0.05)
    }

    val estimator = new LinearSVC().setRegParam(0.1)
    val dataset = smallBinaryDataset
    MLTestingUtils.testArbitrarilyScaledWeights[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, modelEquals)
    MLTestingUtils.testOutliersWithSmallWeights[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, 2, modelEquals)
    MLTestingUtils.testOversamplingVsWeighting[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, modelEquals, 42L)
  }

  test("read/write: SVM") {
    def checkModelData(model: LinearSVCModel, model2: LinearSVCModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients === model2.coefficients)
      assert(model.numFeatures === model2.numFeatures)
    }
    val svm = new LinearSVC()
    testEstimatorAndModelReadWrite(svm, smallBinaryDataset, LinearSVCSuite.allParamSettings,
      checkModelData)
  }
}

object LinearSVCSuite {

  val allParamSettings: Map[String, Any] = Map(
    "regParam" -> 0.01,
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "threshold" -> 0.6,
    "predictionCol" -> "myPredict",
    "rawPredictionCol" -> "myRawPredict",
    "aggregationDepth" -> 3
  )

    // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  def generateSVMInput(
    intercept: Double,
    weights: Array[Double],
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new BDV(weights)
    val x = Array.fill[Array[Double]](nPoints)(
        Array.fill[Double](weights.length)(rnd.nextDouble() * 2.0 - 1.0))
    val y = x.map { xi =>
      val yD = new BDV(xi).dot(weightsMat) + intercept + 0.01 * rnd.nextGaussian()
      if (yD < 0) 0.0 else 1.0
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

}

