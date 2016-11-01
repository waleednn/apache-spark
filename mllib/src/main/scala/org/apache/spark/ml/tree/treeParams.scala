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

package org.apache.spark.ml.tree

import scala.util.Try

import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.tree.impurity._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Entropy => OldEntropy, Gini => OldGini, Impurity => OldImpurity, Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, LogLoss => OldLogLoss, Loss => OldLoss, SquaredError => OldSquaredError}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * Parameters for Decision Tree-based algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait DecisionTreeParams extends PredictorParams
  with HasCheckpointInterval with HasSeed {

  /**
   * Maximum depth of the tree (>= 0).
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   *
   * @group param
   */
  final val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "Maximum depth of the tree. (>= 0)" +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.",
      ParamValidators.gtEq(0))

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be >= 2 and >= number of categories in any categorical feature.
   * (default = 32)
   *
   * @group param
   */
  final val maxBins: IntParam = new IntParam(this, "maxBins", "Max number of bins for" +
    " discretizing continuous features.  Must be >=2 and >= number of categories for any" +
    " categorical feature.", ParamValidators.gtEq(2))

  /**
   * Minimum number of instances each child must have after split.
   * If a split causes the left or right child to have fewer than minInstancesPerNode,
   * the split will be discarded as invalid.
   * Should be >= 1.
   * (default = 1)
   *
   * @group param
   */
  final val minInstancesPerNode: IntParam = new IntParam(this, "minInstancesPerNode", "Minimum" +
    " number of instances each child must have after split.  If a split causes the left or right" +
    " child to have fewer than minInstancesPerNode, the split will be discarded as invalid." +
    " Should be >= 1.", ParamValidators.gtEq(1))

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * (default = 0.0)
   *
   * @group param
   */
  final val minInfoGain: DoubleParam = new DoubleParam(this, "minInfoGain",
    "Minimum information gain for a split to be considered at a tree node.")

  /**
   * Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be
   * split per iteration, and its aggregates may exceed this size.
   * (default = 256 MB)
   *
   * @group expertParam
   */
  final val maxMemoryInMB: IntParam = new IntParam(this, "maxMemoryInMB",
    "Maximum memory in MB allocated to histogram aggregation.",
    ParamValidators.gtEq(0))

  /**
   * If false, the algorithm will pass trees to executors to match instances with nodes.
   * If true, the algorithm will cache node IDs for each instance.
   * Caching can speed up training of deeper trees. Users can set how often should the
   * cache be checkpointed or disable it by setting checkpointInterval.
   * (default = false)
   *
   * @group expertParam
   */
  final val cacheNodeIds: BooleanParam = new BooleanParam(this, "cacheNodeIds", "If false, the" +
    " algorithm will pass trees to executors to match instances with nodes. If true, the" +
    " algorithm will cache node IDs for each instance. Caching can speed up training of deeper" +
    " trees.")

  setDefault(maxDepth -> 5, maxBins -> 32, minInstancesPerNode -> 1, minInfoGain -> 0.0,
    maxMemoryInMB -> 256, cacheNodeIds -> false, checkpointInterval -> 10)

  /** @group setParam */
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group getParam */
  final def getMaxDepth: Int = $(maxDepth)

  /** @group setParam */
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group getParam */
  final def getMaxBins: Int = $(maxBins)

  /** @group setParam */
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group getParam */
  final def getMinInstancesPerNode: Int = $(minInstancesPerNode)

  /** @group setParam */
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group getParam */
  final def getMinInfoGain: Double = $(minInfoGain)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group expertSetParam */
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertGetParam */
  final def getMaxMemoryInMB: Int = $(maxMemoryInMB)

  /** @group expertSetParam */
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /** @group expertGetParam */
  final def getCacheNodeIds: Boolean = $(cacheNodeIds)

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be >= 1.
   * (default = 10)
   *
   * @group setParam
   */
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity,
      subsamplingRate: Double): OldStrategy = {
    val strategy = OldStrategy.defaultStrategy(oldAlgo)
    strategy.impurity = oldImpurity
    strategy.checkpointInterval = getCheckpointInterval
    strategy.maxBins = getMaxBins
    strategy.maxDepth = getMaxDepth
    strategy.maxMemoryInMB = getMaxMemoryInMB
    strategy.minInfoGain = getMinInfoGain
    strategy.minInstancesPerNode = getMinInstancesPerNode
    strategy.useNodeIdCache = getCacheNodeIds
    strategy.numClasses = numClasses
    strategy.categoricalFeaturesInfo = categoricalFeatures
    strategy.subsamplingRate = subsamplingRate
    strategy
  }
}

/**
 * Parameters for Decision Tree-based classification algorithms.
 */
private[ml] trait TreeClassifierParams extends Params {

  // Impurity should be overridden when setting a default. This should be a def, but has
  // to be a val to maintain the proper documentation.
  /**
   * @group param
   */
  val impurity: Param[String] = new Param(this, "", "")

  /** @group setParam */
  def setImpurity(value: String): this.type = set(impurity, value)

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase

  /** Convert new impurity to old impurity. */
  private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "entropy" => OldEntropy
      case "gini" => OldGini
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeClassifierParams was given unrecognized impurity: $impurity.")
    }
  }
}

private[ml] object TreeClassifierParams {
  // These options should be lowercase.
  final val supportedImpurities: Array[String] = Array("entropy", "gini").map(_.toLowerCase)
}

private[ml] trait TreeClassifierParamsWithDefault extends TreeClassifierParams {
  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Also used for terminal leaf value prediction.
   * Supported: "gini" (default) and "entropy"
   *
   * @group param
   */
  override val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeClassifierParams.supportedImpurities.mkString(", ")}",
    (value: String) => TreeClassifierParams.supportedImpurities.contains(value.toLowerCase))

  setDefault(impurity -> "gini")
}

private[ml] trait DecisionTreeClassifierParams
  extends DecisionTreeParams with TreeClassifierParamsWithDefault

/**
 * Parameters for Decision Tree-based regression algorithms.
 */
private[ml] trait TreeRegressorParams extends Params {

  // Impurity should be overridden when setting a default. This should be a def, but has
  // to be a val to maintain the proper documentation.
  /**
   * @group param
   */
  val impurity: Param[String] = new Param(this, "", "")

  /** @group setParam */
  def setImpurity(value: String): this.type = set(impurity, value)

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase

  /** Convert new impurity to old impurity. */
  private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "variance" => OldVariance
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeRegressorParams was given unrecognized impurity: $impurity")
    }
  }
}

private[ml] object TreeRegressorParams {
  // These options should be lowercase.
  final val supportedImpurities: Array[String] = Array("variance").map(_.toLowerCase)
}

private[ml] trait TreeRegressorParamsWithDefault extends TreeRegressorParams {
  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Supported: "variance" (default)
   *
   * @group param
   */
  override val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeRegressorParams.supportedImpurities.mkString(", ")}",
    (value: String) => TreeRegressorParams.supportedImpurities
      .contains(value.toLowerCase))

  setDefault(impurity -> "variance")
}

private[ml] trait DecisionTreeRegressorParams extends DecisionTreeParams
  with TreeRegressorParamsWithDefault with HasVarianceCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val newSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if (isDefined(varianceCol) && $(varianceCol).nonEmpty) {
      SchemaUtils.appendColumn(newSchema, $(varianceCol), DoubleType)
    } else {
      newSchema
    }
  }
}

/**
 * Parameters for Decision Tree-based ensemble algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait TreeEnsembleParams extends DecisionTreeParams {

  /**
   * Fraction of the training data used for learning each decision tree, in range (0, 1].
   * (default = 1.0)
   *
   * @group param
   */
  final val subsamplingRate: DoubleParam = new DoubleParam(this, "subsamplingRate",
    "Fraction of the training data used for learning each decision tree, in range (0, 1].",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  setDefault(subsamplingRate -> 1.0)

  /** @group setParam */
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group getParam */
  final def getSubsamplingRate: Double = $(subsamplingRate)

  /**
   * Create a Strategy instance to use with the old API.
   * NOTE: The caller should set impurity and seed.
   */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, oldAlgo, oldImpurity, getSubsamplingRate)
  }
}

/** Used for [[RandomForestParams]] */
private[ml] trait HasFeatureSubsetStrategy extends Params {

  /**
   * The number of features to consider for splits at each tree node.
   * Supported options:
   *  - "auto": Choose automatically for task:
   *            If numTrees == 1, set to "all."
   *            If numTrees > 1 (forest), set to "sqrt" for classification and
   *              to "onethird" for regression.
   *  - "all": use all features
   *  - "onethird": use 1/3 of the features
   *  - "sqrt": use sqrt(number of features)
   *  - "log2": use log2(number of features)
   *  - "n": when n is in the range (0, 1.0], use n * number of features. When n
   *         is in the range (1, number of features), use n features.
   * (default = "auto")
   *
   * These various settings are based on the following references:
   *  - log2: tested in Breiman (2001)
   *  - sqrt: recommended by Breiman manual for random forests
   *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
   *    package.
   *
   * @see [[http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf  Breiman (2001)]]
   * @see [[http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf  Breiman manual for
   *     random forests]]
   * @group param
   */
  final val featureSubsetStrategy: Param[String] = new Param[String](this, "featureSubsetStrategy",
    "The number of features to consider for splits at each tree node." +
      s" Supported options: ${RandomForestParams.supportedFeatureSubsetStrategies.mkString(", ")}" +
      s", (0.0-1.0], [1-n].",
    (value: String) =>
      RandomForestParams.supportedFeatureSubsetStrategies.contains(value.toLowerCase)
      || Try(value.toInt).filter(_ > 0).isSuccess
      || Try(value.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess)

  setDefault(featureSubsetStrategy -> "auto")

  /** @group setParam */
  def setFeatureSubsetStrategy(value: String): this.type = set(featureSubsetStrategy, value)

  /** @group getParam */
  final def getFeatureSubsetStrategy: String = $(featureSubsetStrategy).toLowerCase
}

/**
 * Used for [[RandomForestParams]].
 * This is separated out from [[RandomForestParams]] because of an issue with the
 * `numTrees` method conflicting with this Param in the Estimator.
 */
private[ml] trait HasNumTrees extends Params {

  /**
   * Number of trees to train (>= 1).
   * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).  SPARK-7130
   * (default = 20)
   *
   * @group param
   */
  final val numTrees: IntParam = new IntParam(this, "numTrees", "Number of trees to train (>= 1)",
    ParamValidators.gtEq(1))

  setDefault(numTrees -> 20)

  /** @group setParam */
  def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group getParam */
  final def getNumTrees: Int = $(numTrees)
}

/**
 * Parameters for Random Forest algorithms.
 */
private[ml] trait RandomForestParams extends TreeEnsembleParams
  with HasFeatureSubsetStrategy with HasNumTrees

private[spark] object RandomForestParams {
  // These options should be lowercase.
  final val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "onethird", "sqrt", "log2").map(_.toLowerCase)
}

private[ml] trait RandomForestClassifierParams
  extends RandomForestParams with TreeClassifierParamsWithDefault

private[ml] trait RandomForestClassificationModelParams extends TreeEnsembleParams
  with HasFeatureSubsetStrategy with TreeClassifierParamsWithDefault

private[ml] trait RandomForestRegressorParams
  extends RandomForestParams with TreeRegressorParamsWithDefault

private[ml] trait RandomForestRegressionModelParams extends TreeEnsembleParams
  with HasFeatureSubsetStrategy with TreeRegressorParamsWithDefault

/**
 * Parameters for Gradient-Boosted Tree algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait GBTParams extends TreeEnsembleParams with HasMaxIter with HasStepSize {

  /* TODO: Add this doc when we add this param.  SPARK-7132
   * Threshold for stopping early when runWithValidation is used.
   * If the error rate on the validation input changes by less than the validationTol,
   * then learning will stop early (before [[numIterations]]).
   * This parameter is ignored when run is used.
   * (default = 1e-5)
   * @group param
   */
  // final val validationTol: DoubleParam = new DoubleParam(this, "validationTol", "")
  // validationTol -> 1e-5

  setDefault(maxIter -> 20, stepSize -> 0.1)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each
   * estimator.
   * (default = 0.1)
   *
   * @group setParam
   */
  def setStepSize(value: Double): this.type = set(stepSize, value)

  override def validateParams(): Unit = {
    require(ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true)(
      getStepSize), "GBT parameter stepSize should be in interval (0, 1], " +
      s"but it given invalid value $getStepSize.")
  }

  /** (private[ml]) Create a BoostingStrategy instance to use with the old API. */
  private[ml] def getOldBoostingStrategy(
      categoricalFeatures: Map[Int, Int],
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity): OldBoostingStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 2, oldAlgo, oldImpurity)
    // NOTE: The old API does not support "seed" so we ignore it.
    new OldBoostingStrategy(strategy, getOldLossType, getMaxIter, getStepSize)
  }

  /** Get old Gradient Boosting Loss type */
  private[ml] def getOldLossType: OldLoss
}

private[ml] object GBTClassifierParams {
  // The values below should be lowercase.
  /** Accessor for supported loss settings: logistic, bernoulli */
  final val supportedLossTypes: Array[String] = Array("logistic", "bernoulli")
  /** Accessor for support entropy settings: loss-based or variance */
  final val supportedImpurities: Array[String] = Array("loss-based", "variance")
  final def getLossBasedImpurity(loss: String): OldImpurity = loss match {
    case "logistic" | "bernoulli" => ApproxBernoulliImpurity
    case _ => throw new RuntimeException(
      s"GBTClassifier does not have loss-based impurity for loss ${loss}")
  }
}

private[ml] trait GBTClassifierParams extends GBTParams with TreeClassifierParams {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Also used for terminal leaf value prediction.
   * Supported: "loss-based" (default) and "variance"
   *
   * @group expertParam
   */
  @deprecated(
      "Control over impurity will be removed, as it is an implementation detail of GBTs",
      "2.0.2")
  override val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${GBTClassifierParams.supportedImpurities.mkString(", ")}",
    (value: String) => GBTClassifierParams.supportedImpurities.contains(value.toLowerCase))

  /** (private[ml]) convert new impurity to old impurity */
  override private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "loss-based" => GBTClassifierParams.getLossBasedImpurity($(lossType))
      case "variance" => OldVariance
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"GBTClassifier was given unrecognized impurity: $impurity")
    }
  }

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "logistic" (default), "bernoulli" (alias for "logistic")
   *
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifierParams.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTClassifierParams.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "logistic", impurity -> "loss-based")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "bernoulli" | "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier was given bad loss type: $getLossType")
    }
  }
}

private[ml] object GBTRegressorParams {
  // The losses below should be lowercase.
  /**
   * Accessor for supported loss settings: squared (L2), absolute (L1),
   * gaussian (alias for squared), laplace (alias for absolute)
   * */
  final val supportedLossTypes: Array[String] = Array(
    "squared", "absolute", "gaussian", "laplace")
  /** Accessor for supported impurity settings: loss-based or variance */
  final val supportedImpurities: Array[String] = Array("loss-based", "variance")

  final def getLossBasedImpurity(loss: String): OldImpurity = loss match {
    case "gaussian" | "squared" => OldVariance
    case "laplace" | "absolute" => throw new RuntimeException(
      "GBTRegressor does not yet support loss-based impurity for absolute or laplace loss")
    // TODO(SPARK-4240) use ApproxLaplaceImpurity here once it is implemented.
    case _ => throw new RuntimeException(
      s"GBTRegressor does not have loss-based impurity for loss ${loss}")
  }
}

private[ml] trait GBTRegressorParams extends GBTParams with TreeRegressorParams {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Also used for terminal leaf value prediction.
   * Supported: "loss-based" and "variance" (default)
   *
   * @group expertParam
   */
  @deprecated(
    "Control over impurity will be removed, as it is an implementation detail of GBTs",
    "2.0.2")
  override val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${GBTRegressorParams.supportedImpurities.mkString(", ")}",
    (value: String) => GBTRegressorParams.supportedImpurities.contains(value.toLowerCase))

  override private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "loss-based" => GBTRegressorParams.getLossBasedImpurity($(lossType))
      case "variance" => OldVariance
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"GBTRegressor was given unrecognized impurity: $impurity")
    }
  }

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "gaussian" (default), "squared" (alias for "gaussian"), "laplace",
   * "absolute" (alias for "laplace").
   *
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTRegressorParams.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTRegressorParams.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "gaussian", impurity -> "variance")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "squared" | "gaussian" => OldSquaredError
      case "absolute" | "laplace" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss type: $getLossType")
    }
  }
}
