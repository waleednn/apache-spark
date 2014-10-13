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

package org.apache.spark.mllib.tree

import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, Strategy}
import org.apache.spark.Logging
import org.apache.spark.mllib.tree.impl.TimeTracker
import org.apache.spark.mllib.tree.impurity.{Impurities, Impurity}
import org.apache.spark.mllib.tree.loss.{Losses, LeastSquaresError, Loss}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostingModel, DecisionTreeModel}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.storage.StorageLevel

/**
 * :: Experimental ::
 * A class that implements gradient boosting for regression problems.
 * @param strategy Parameters for the underlying decision tree estimators
 * @param boostingStrategy Parameters for the gradient boosting algorithm
 */
@Experimental
class GradientBoosting (
    private val strategy: Strategy,
    private val boostingStrategy: BoostingStrategy) extends Serializable with Logging {

  /**
   * Method to train a gradient boosting model
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): GradientBoostingModel = {
    val algo = strategy.algo
    algo match {
      case Regression => GradientBoosting.boost(input, strategy, boostingStrategy)
      case Classification =>
        val remappedInput = input.map(x => new LabeledPoint((x.label * 2) - 1, x.features))
        GradientBoosting.boost(remappedInput, strategy, boostingStrategy)
      case _ =>
        throw new IllegalArgumentException(s"$algo is not supported by the gradient boosting.")
    }
  }

}


object GradientBoosting extends Logging {

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting model.
   *
   * Note: Using [[org.apache.spark.mllib.tree.GradientBoosting#trainRegressor]]
   *       is recommended to clearly specify regression.
   *       Using [[org.apache.spark.mllib.tree.GradientBoosting#trainClassifier]]
   *       is recommended to clearly specify regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the underlying tree-based estimators
   *                 which specify the type of algorithm (classification, regression, etc.),
   *                 feature type (continuous, categorical), depth of the tree,
   *                 quantile calculation strategy, etc.
   * @return GradientBoostingModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      learningRate: Double,
      subsample: Double,
      loss: String,
      checkpointPeriod: Int,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      categoricalFeaturesInfo: Map[Int, Int]): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val boostingStrategy = new BoostingStrategy(numEstimators, learningRate, subsample, lossType,
      checkpointPeriod)
    // TODO: Remove tree strategy and merge it into boosting strategy
    val strategy = new Strategy(Regression, impurityType, maxDepth, 2, maxBins,
      Sort, categoricalFeaturesInfo, subsample = subsample)
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numEstimators: Int,
      learningRate: Double,
      subsample: Double,
      loss: String,
      checkpointPeriod: Int,
      impurity: String,
      maxDepth: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      categoricalFeaturesInfo: Map[Int, Int]): GradientBoostingModel = {
    val lossType = Losses.fromString(loss)
    val impurityType = Impurities.fromString(impurity)
    val boostingStrategy = new BoostingStrategy(numEstimators, learningRate, subsample, lossType,
      checkpointPeriod)
    // TODO: Remove tree strategy and merge it into boosting strategy
    val strategy = new Strategy(Classification, impurityType, maxDepth, numClassesForClassification, maxBins,
      Sort, categoricalFeaturesInfo, subsample = subsample)
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting regression model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the underlying tree-based estimators
   *                 which specify the type of algorithm (classification, regression, etc.),
   *                 feature type (continuous, categorical), depth of the tree,
   *                 quantile calculation strategy, etc.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    // TODO: Add require for algo
    // TODO: Remove tree strategy and merge it into boosting strategy
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  // TODO: Add javadoc
  /**
   * Method to train a gradient boosting classification model.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the underlying tree-based estimators
   *                 which specify the type of algorithm (classification, regression, etc.),
   *                 feature type (continuous, categorical), depth of the tree,
   *                 quantile calculation strategy, etc.
   * @return GradientBoostingModel that can be used for prediction
   */
  def trainClassification(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {
    // TODO: Add require for algo
    // TODO: Remove tree strategy and merge it into boosting strategy
    new GradientBoosting(strategy, boostingStrategy).train(input)
  }

  // TODO: java friendly API for classification and regression


  /**
   * Internal method for performing regression using trees as base learners.
   * @param input training dataset
   * @param strategy tree parameters
   * @param boostingStrategy boosting parameters
   * @return
   */
  private def boost(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      boostingStrategy: BoostingStrategy): GradientBoostingModel = {

    val timer = new TimeTracker()

    timer.start("total")
    timer.start("init")


    // Initialize gradient boosting parameters
    val numEstimators = boostingStrategy.numEstimators
    val trees = new Array[DecisionTreeModel](numEstimators + 1)
    val loss = boostingStrategy.loss
    val learningRate = boostingStrategy.learningRate
    // TODO: Implement Stochastic gradient boosting using BaggedPoint
    val subsample = boostingStrategy.subsample
    val checkpointingPeriod = boostingStrategy.checkpointPeriod

    // Cache input
    input.persist(StorageLevel.MEMORY_AND_DISK)
    var lastCachedData = input

    timer.stop("init")

    logDebug("##########")
    logDebug("Building tree 0")
    logDebug("##########")
    var data = input

    // 1. Initialize tree
    timer.start("building tree 0")
    val firstModel = new DecisionTree(strategy).train(data)
    timer.stop("building tree 0")
    trees(0) = firstModel
    logDebug("error of tree = " + meanSquaredError(firstModel, data))

    // psuedo-residual for second iteration
    data = data.map(point => LabeledPoint(loss.lossGradient(firstModel, point,
      learningRate), point.features))


    var m = 1
    while (m <= numEstimators) {
      timer.start(s"building tree $m")
      logDebug("###################################################")
      logDebug("Gradient boosting tree iteration " + m)
      logDebug("###################################################")
      val model = new DecisionTree(strategy).train(data)
      timer.stop(s"building tree $m")
      trees(m) = model
      logDebug("error of tree = " + meanSquaredError(model, data))
      // Update data with pseudo-residuals
      data = data.map(point => LabeledPoint(loss.lossGradient(model, point, learningRate),
        point.features))
      if (m % checkpointingPeriod == 1 && m != 1) {
        lastCachedData.unpersist()
      }
      // Checkpoint
      if (m % checkpointingPeriod == 0) {
        data = data.persist(StorageLevel.MEMORY_AND_DISK)
        lastCachedData = data
      }
      m += 1
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")


    // 3. Output classifier
    new GradientBoostingModel(trees, strategy.algo)

  }

  /**
   * Calculates the mean squared error for regression.
   */
  private def meanSquaredError(tree: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
    data.map { y =>
      val err = tree.predict(y.features) - y.label
      err * err
    }.mean()
  }


}
