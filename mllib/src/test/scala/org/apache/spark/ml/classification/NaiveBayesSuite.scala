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

import breeze.linalg.{DenseVector => BDV, Vector => BV}
import breeze.stats.distributions.{Multinomial => BrzMultinomial}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.classification.NaiveBayes.{Bernoulli, Gaussian, Multinomial}
import org.apache.spark.ml.classification.NaiveBayesSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class NaiveBayesSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _
  @transient var bernoulliDataset: Dataset[_] = _
  @transient var gaussianDataset: Dataset[_] = _

  private val seed = 42

  override def beforeAll(): Unit = {
    super.beforeAll()

    val pi = Array(0.3, 0.3, 0.4).map(math.log)
    val theta = Array(
      Array(0.30, 0.30, 0.30, 0.30), // label 0
      Array(0.30, 0.30, 0.30, 0.30), // label 1
      Array(0.40, 0.40, 0.40, 0.40)  // label 2
    ).map(_.map(math.log))

    // theta for gaussian nb
    val theta2 = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0: mean
      Array(0.10, 0.70, 0.10, 0.10), // label 1: mean
      Array(0.10, 0.10, 0.70, 0.10)  // label 2: mean
    )

    // sigma for gaussian nb
    val sigma = Array(
      Array(0.10, 0.10, 0.50, 0.10), // label 0: variance
      Array(0.50, 0.10, 0.10, 0.10), // label 1: variance
      Array(0.10, 0.10, 0.10, 0.50)  // label 2: variance
    )

    dataset = generateNaiveBayesInput(pi, theta, 100, seed).toDF()
    bernoulliDataset = generateNaiveBayesInput(pi, theta, 100, seed, "bernoulli").toDF()
    gaussianDataset = generateGaussianNaiveBayesInput(pi, theta2, sigma, 100, seed).toDF()
  }

  def validatePrediction(predictionAndLabels: DataFrame): Unit = {
    val numOfErrorPredictions = predictionAndLabels.collect().count {
      case Row(prediction: Double, label: Double) =>
        prediction != label
    }
    // At least 80% of the predictions should be on.
    assert(numOfErrorPredictions < predictionAndLabels.count() / 5)
  }

  def validateModelFit(
      piData: Vector,
      thetaData: Matrix,
      sigmaData: Option[Matrix],
      model: NaiveBayesModel): Unit = {
    assert(Vectors.dense(model.pi.toArray.map(math.exp)) ~==
      Vectors.dense(piData.toArray.map(math.exp)) absTol 0.05, "pi mismatch")
    assert(model.theta.map(math.exp) ~== thetaData.map(math.exp) absTol 0.05, "theta mismatch")
    if (sigmaData.isEmpty) {
      assert(model.sigma.isEmpty, "sigma mismatch")
    } else {
      assert(model.sigma.get.map(math.exp) ~== sigmaData.get.map(math.exp) absTol 0.05,
        "sigma mismatch")
    }
  }

  def expectedMultinomialProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
    val logClassProbs: BV[Double] = model.pi.asBreeze + model.theta.multiply(feature).asBreeze
    val classProbs = logClassProbs.toArray.map(math.exp)
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def expectedBernoulliProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
    val negThetaMatrix = model.theta.map(v => math.log(1.0 - math.exp(v)))
    val negFeature = Vectors.dense(feature.toArray.map(v => 1.0 - v))
    val piTheta: BV[Double] = model.pi.asBreeze + model.theta.multiply(feature).asBreeze
    val logClassProbs: BV[Double] = piTheta + negThetaMatrix.multiply(negFeature).asBreeze
    val classProbs = logClassProbs.toArray.map(math.exp)
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def expectedGaussianProbabilities(model: NaiveBayesModel, feature: Vector): Vector = {
    val pi = model.pi.toArray.map(math.exp)
    val classProbs = pi.indices.map { i =>
      feature.toArray.zipWithIndex.map { case (v, j) =>
        val mean = model.theta(i, j)
        val variance = model.sigma.get(i, j)
        math.exp(- (v - mean) * (v - mean) / variance / 2) / math.sqrt(variance * math.Pi * 2)
      }.product * pi(i)
    }.toArray
    val classProbsSum = classProbs.sum
    Vectors.dense(classProbs.map(_ / classProbsSum))
  }

  def validateProbabilities(
      featureAndProbabilities: DataFrame,
      model: NaiveBayesModel,
      modelType: String): Unit = {
    featureAndProbabilities.collect().foreach {
      case Row(features: Vector, probability: Vector) =>
        assert(probability.toArray.sum ~== 1.0 relTol 1.0e-10)
        val expected = modelType match {
          case Multinomial =>
            expectedMultinomialProbabilities(model, features)
          case Bernoulli =>
            expectedBernoulliProbabilities(model, features)
          case Gaussian =>
            expectedGaussianProbabilities(model, features)
          case _ =>
            throw new UnknownError(s"Invalid modelType: $modelType.")
        }
        assert(probability ~== expected relTol 1.0e-10)
    }
  }

  test("model types") {
    assert(Multinomial === "multinomial")
    assert(Bernoulli === "bernoulli")
    assert(Gaussian === "gaussian")
  }

  test("params") {
    ParamsSuite.checkParams(new NaiveBayes)
    val model = new NaiveBayesModel("nb", pi = Vectors.dense(Array(0.2, 0.8)),
      theta = new DenseMatrix(2, 3, Array(0.1, 0.2, 0.3, 0.4, 0.6, 0.4)),
      sigma = None)
    ParamsSuite.checkParams(model)
  }

  test("naive bayes: default params") {
    val nb = new NaiveBayes
    assert(nb.getLabelCol === "label")
    assert(nb.getFeaturesCol === "features")
    assert(nb.getPredictionCol === "prediction")
    assert(nb.getSmoothing === 1.0)
    assert(nb.getModelType === "multinomial")
  }

  test("Naive Bayes Multinomial") {
    val nPoints = 1000
    val piArray = Array(0.5, 0.1, 0.4).map(math.log)
    val thetaArray = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0
      Array(0.10, 0.70, 0.10, 0.10), // label 1
      Array(0.10, 0.10, 0.70, 0.10)  // label 2
    ).map(_.map(math.log))
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 4, thetaArray.flatten, true)

    val testDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, seed, "multinomial").toDF()
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("multinomial")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, None, model)
    assert(model.hasParent)
    MLTestingUtils.checkCopyAndUids(nb, model)

    val validationDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 17, "multinomial").toDF()

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")
    validatePrediction(predictionAndLabels)

    val featureAndProbabilities = model.transform(validationDataset)
      .select("features", "probability")
    validateProbabilities(featureAndProbabilities, model, "multinomial")
  }

  test("Naive Bayes with weighted samples") {
    val numClasses = 3
    def modelEquals(m1: NaiveBayesModel, m2: NaiveBayesModel): Unit = {
      assert(m1.getModelType === m2.getModelType)
      assert(m1.pi ~== m2.pi relTol 0.01)
      assert(m1.theta ~== m2.theta relTol 0.01)
      if (m1.getModelType == Gaussian) {
        assert(m1.sigma.get ~== m2.sigma.get relTol 0.01)
      }
    }
    val testParams = Seq[(String, Dataset[_])](
      ("bernoulli", bernoulliDataset),
      ("multinomial", dataset),
      ("gaussian", gaussianDataset)
    )
    testParams.foreach { case (family, dataset) =>
      // NaiveBayes is sensitive to constant scaling of the weights unless smoothing is set to 0
      val estimatorNoSmoothing = new NaiveBayes().setSmoothing(0.0).setModelType(family)
      val estimatorWithSmoothing = new NaiveBayes().setModelType(family)
      MLTestingUtils.testArbitrarilyScaledWeights[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorNoSmoothing, modelEquals)
      MLTestingUtils.testOutliersWithSmallWeights[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorWithSmoothing, numClasses, modelEquals, outlierRatio = 3)
      MLTestingUtils.testOversamplingVsWeighting[NaiveBayesModel, NaiveBayes](
        dataset.as[LabeledPoint], estimatorWithSmoothing, modelEquals, seed)
    }
  }

  test("Naive Bayes Bernoulli") {
    val nPoints = 10000
    val piArray = Array(0.5, 0.3, 0.2).map(math.log)
    val thetaArray = Array(
      Array(0.50, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.40), // label 0
      Array(0.02, 0.70, 0.10, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02), // label 1
      Array(0.02, 0.02, 0.60, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02, 0.30)  // label 2
    ).map(_.map(math.log))
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 12, thetaArray.flatten, true)

    val testDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 45, "bernoulli").toDF()
    val nb = new NaiveBayes().setSmoothing(1.0).setModelType("bernoulli")
    val model = nb.fit(testDataset)

    validateModelFit(pi, theta, None, model)
    assert(model.hasParent)

    val validationDataset =
      generateNaiveBayesInput(piArray, thetaArray, nPoints, 20, "bernoulli").toDF()

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")
    validatePrediction(predictionAndLabels)

    val featureAndProbabilities = model.transform(validationDataset)
      .select("features", "probability")
    validateProbabilities(featureAndProbabilities, model, "bernoulli")
  }

  test("detect negative values") {
    val dense = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(-1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0))))
    intercept[SparkException] {
      new NaiveBayes().fit(dense)
    }
    val sparse = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(-1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty))))
    intercept[SparkException] {
      new NaiveBayes().fit(sparse)
    }
    val nan = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(0.0, Vectors.sparse(1, Array(0), Array(Double.NaN))),
      LabeledPoint(1.0, Vectors.sparse(1, Array(0), Array(1.0))),
      LabeledPoint(1.0, Vectors.sparse(1, Array.empty, Array.empty))))
    intercept[SparkException] {
      new NaiveBayes().fit(nan)
    }
  }

  test("detect non zero or one values in Bernoulli") {
    val badTrain = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0))))

    intercept[SparkException] {
      new NaiveBayes().setModelType(Bernoulli).setSmoothing(1.0).fit(badTrain)
    }

    val okTrain = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(1.0))))

    val model = new NaiveBayes().setModelType(Bernoulli).setSmoothing(1.0).fit(okTrain)

    val badPredict = spark.createDataFrame(Seq(
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0)),
      LabeledPoint(1.0, Vectors.dense(1.0)),
      LabeledPoint(1.0, Vectors.dense(0.0))))

    intercept[SparkException] {
      model.transform(badPredict).collect()
    }
  }
  
  test("Naive Bayes Gaussian") {
    val piArray = Array(0.5, 0.1, 0.4).map(math.log)

    val thetaArray = Array(
      Array(0.70, 0.10, 0.10, 0.10), // label 0: mean
      Array(0.10, 0.70, 0.10, 0.10), // label 1: mean
      Array(0.10, 0.10, 0.70, 0.10)  // label 2: mean
    )

    val sigmaArray = Array(
      Array(0.10, 0.10, 0.50, 0.10), // label 0: variance
      Array(0.50, 0.10, 0.10, 0.10), // label 1: variance
      Array(0.10, 0.10, 0.10, 0.50)  // label 2: variance
    )

    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(3, 4, thetaArray.flatten, true)
    val sigma = new DenseMatrix(3, 4, sigmaArray.flatten, true)

    val nPoints = 10000
    val testDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 42).toDF()
    val gnb = new NaiveBayes().setModelType("gaussian")
    val model = gnb.fit(testDataset)

    validateModelFit(pi, theta, Some(sigma), model)
    assert(model.hasParent)

    val validationDataset =
      generateGaussianNaiveBayesInput(piArray, thetaArray, sigmaArray, nPoints, 17).toDF()

    val predictionAndLabels = model.transform(validationDataset).select("prediction", "label")
    validatePrediction(predictionAndLabels)

    val featureAndProbabilities = model.transform(validationDataset)
      .select("features", "probability")
    validateProbabilities(featureAndProbabilities, model, "gaussian")
  }

  test("read/write") {
    def checkModelData(model: NaiveBayesModel, model2: NaiveBayesModel): Unit = {
      assert(model.getModelType === model2.getModelType)
      assert(model.pi === model2.pi)
      assert(model.theta === model2.theta)
      if (model.getModelType == "gaussian") {
        assert(model.sigma.get === model2.sigma.get)
      } else {
        assert(model.sigma.isEmpty && model2.sigma.isEmpty)
      }
    }

    val nb = new NaiveBayes()
    testEstimatorAndModelReadWrite(nb, dataset, NaiveBayesSuite.allParamSettings,
      NaiveBayesSuite.allParamSettings, checkModelData)

    val gnb = new NaiveBayes().setModelType("gaussian")
    testEstimatorAndModelReadWrite(gnb, gaussianDataset,
      NaiveBayesSuite.allParamSettingsForGaussian,
      NaiveBayesSuite.allParamSettingsForGaussian, checkModelData)
  }

  test("should support all NumericType labels and weights, and not support other types") {
    val nb = new NaiveBayes()
    MLTestingUtils.checkNumericTypes[NaiveBayesModel, NaiveBayes](
      nb, spark) { (expected, actual) =>
        assert(expected.pi === actual.pi)
        assert(expected.theta === actual.theta)
        assert(expected.sigma.isEmpty && actual.sigma.isEmpty)
      }
  }
}

object NaiveBayesSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "smoothing" -> 0.1
  )

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettingsForGaussian: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "modelType" -> "gaussian"
  )

  private def calcLabel(p: Double, pi: Array[Double]): Int = {
    var sum = 0.0
    for (j <- 0 until pi.length) {
      sum += pi(j)
      if (p < sum) return j
    }
    -1
  }

  // Generate input of the form Y = (theta * x).argmax()
  def generateNaiveBayesInput(
    pi: Array[Double],            // 1XC
    theta: Array[Array[Double]],  // CXD
    nPoints: Int,
    seed: Int,
    modelType: String = Multinomial,
    sample: Int = 10): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.exp)
    val _theta = theta.map(row => row.map(math.exp))

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = modelType match {
        case Bernoulli => Array.tabulate[Double] (D) { j =>
            if (rnd.nextDouble () < _theta(y)(j) ) 1 else 0
        }
        case Multinomial =>
          val mult = BrzMultinomial(BDV(_theta(y)))
          val emptyMap = (0 until D).map(x => (x, 0.0)).toMap
          val counts = emptyMap ++ mult.sample(sample).groupBy(x => x).map {
            case (index, reps) => (index, reps.size.toDouble)
          }
          counts.toArray.sortBy(_._1).map(_._2)
        case _ =>
          // This should never happen.
          throw new UnknownError(s"Invalid modelType: $modelType.")
      }

      LabeledPoint(y, Vectors.dense(xi))
    }
  }

  // Generate input
  def generateGaussianNaiveBayesInput(
    pi: Array[Double],            // 1XC
    theta: Array[Array[Double]],  // CXD
    sigma: Array[Array[Double]],  // CXD
    nPoints: Int,
    seed: Int): Seq[LabeledPoint] = {
    val D = theta(0).length
    val rnd = new Random(seed)
    val _pi = pi.map(math.exp)

    for (i <- 0 until nPoints) yield {
      val y = calcLabel(rnd.nextDouble(), _pi)
      val xi = Array.tabulate[Double] (D) { j =>
        val mean = theta(y)(j)
        val variance = sigma(y)(j)
        mean + rnd.nextGaussian() * math.sqrt(variance)
      }
      LabeledPoint(y, Vectors.dense(xi))
    }
  }
}
