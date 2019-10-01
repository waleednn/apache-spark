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

package org.apache.spark.ml.param.shared

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._

// DO NOT MODIFY THIS FILE! It was generated by SharedParamsCodeGen.

// scalastyle:off

/**
 * Trait for shared param regParam. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasRegParam extends Params {

  /**
   * Param for regularization parameter (&gt;= 0).
   * @group param
   */
  final val regParam: DoubleParam = new DoubleParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getRegParam: Double = $(regParam)
}

/**
 * Trait for shared param maxIter. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasMaxIter extends Params {

  /**
   * Param for maximum number of iterations (&gt;= 0).
   * @group param
   */
  final val maxIter: IntParam = new IntParam(this, "maxIter", "maximum number of iterations (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getMaxIter: Int = $(maxIter)
}

/**
 * Trait for shared param featuresCol (default: "features"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasFeaturesCol extends Params {

  /**
   * Param for features column name.
   * @group param
   */
  final val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  /** @group getParam */
  final def getFeaturesCol: String = $(featuresCol)
}

/**
 * Trait for shared param featuresCols. (default: Array.empty[String]) This trait may be changed or
 * removed between minor versions.
 *
 * This is to support multiple columns for features.
 */
@DeveloperApi
trait HasFeaturesCols extends Params {

  /**
   * Param for features columns names.
   * @group param
   */
  final val featuresCols: StringArrayParam = new StringArrayParam(this, "featuresCols",
    "names of the multiple columns for features")

  setDefault(featuresCols, Array.empty[String])

  /** @group getParam */
  final def getFeaturesCols: Array[String] = $(featuresCols)
}

/**
 * Trait for shared param labelCol (default: "label"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasLabelCol extends Params {

  /**
   * Param for label column name.
   * @group param
   */
  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")

  setDefault(labelCol, "label")

  /** @group getParam */
  final def getLabelCol: String = $(labelCol)
}

/**
 * Trait for shared param predictionCol (default: "prediction"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasPredictionCol extends Params {

  /**
   * Param for prediction column name.
   * @group param
   */
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  /** @group getParam */
  final def getPredictionCol: String = $(predictionCol)
}

/**
 * Trait for shared param rawPredictionCol (default: "rawPrediction"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasRawPredictionCol extends Params {

  /**
   * Param for raw prediction (a.k.a. confidence) column name.
   * @group param
   */
  final val rawPredictionCol: Param[String] = new Param[String](this, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")

  setDefault(rawPredictionCol, "rawPrediction")

  /** @group getParam */
  final def getRawPredictionCol: String = $(rawPredictionCol)
}

/**
 * Trait for shared param probabilityCol (default: "probability"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasProbabilityCol extends Params {

  /**
   * Param for Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities.
   * @group param
   */
  final val probabilityCol: Param[String] = new Param[String](this, "probabilityCol", "Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities")

  setDefault(probabilityCol, "probability")

  /** @group getParam */
  final def getProbabilityCol: String = $(probabilityCol)
}

/**
 * Trait for shared param varianceCol. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasVarianceCol extends Params {

  /**
   * Param for Column name for the biased sample variance of prediction.
   * @group param
   */
  final val varianceCol: Param[String] = new Param[String](this, "varianceCol", "Column name for the biased sample variance of prediction")

  /** @group getParam */
  final def getVarianceCol: String = $(varianceCol)
}

/**
 * Trait for shared param threshold. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasThreshold extends Params {

  /**
   * Param for threshold in binary classification prediction, in range [0, 1].
   * @group param
   */
  val threshold: DoubleParam = new DoubleParam(this, "threshold", "threshold in binary classification prediction, in range [0, 1]", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getThreshold: Double = $(threshold)
}

/**
 * Trait for shared param thresholds. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasThresholds extends Params {

  /**
   * Param for Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values &gt; 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold.
   * @group param
   */
  final val thresholds: DoubleArrayParam = new DoubleArrayParam(this, "thresholds", "Thresholds in multi-class classification to adjust the probability of predicting each class. Array must have length equal to the number of classes, with values > 0 excepting that at most one value may be 0. The class with largest value p/t is predicted, where p is the original probability of that class and t is the class's threshold", (t: Array[Double]) => t.forall(_ >= 0) && t.count(_ == 0) <= 1)

  /** @group getParam */
  def getThresholds: Array[Double] = $(thresholds)
}

/**
 * Trait for shared param inputCol. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasInputCol extends Params {

  /**
   * Param for input column name.
   * @group param
   */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)
}

/**
 * Trait for shared param inputCols. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasInputCols extends Params {

  /**
   * Param for input column names.
   * @group param
   */
  final val inputCols: StringArrayParam = new StringArrayParam(this, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = $(inputCols)
}

/**
 * Trait for shared param outputCol (default: uid + "__output"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasOutputCol extends Params {

  /**
   * Param for output column name.
   * @group param
   */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  setDefault(outputCol, uid + "__output")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)
}

/**
 * Trait for shared param outputCols. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasOutputCols extends Params {

  /**
   * Param for output column names.
   * @group param
   */
  final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "output column names")

  /** @group getParam */
  final def getOutputCols: Array[String] = $(outputCols)
}

/**
 * Trait for shared param numFeatures (default: 262144). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasNumFeatures extends Params {

  /**
   * Param for Number of features. Should be greater than 0.
   * @group param
   */
  final val numFeatures: IntParam = new IntParam(this, "numFeatures", "Number of features. Should be greater than 0", ParamValidators.gt(0))

  setDefault(numFeatures, 262144)

  /** @group getParam */
  final def getNumFeatures: Int = $(numFeatures)
}

/**
 * Trait for shared param checkpointInterval. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasCheckpointInterval extends Params {

  /**
   * Param for set checkpoint interval (&gt;= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext.
   * @group param
   */
  final val checkpointInterval: IntParam = new IntParam(this, "checkpointInterval", "set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations. Note: this setting will be ignored if the checkpoint directory is not set in the SparkContext", (interval: Int) => interval == -1 || interval >= 1)

  /** @group getParam */
  final def getCheckpointInterval: Int = $(checkpointInterval)
}

/**
 * Trait for shared param fitIntercept (default: true). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasFitIntercept extends Params {

  /**
   * Param for whether to fit an intercept term.
   * @group param
   */
  final val fitIntercept: BooleanParam = new BooleanParam(this, "fitIntercept", "whether to fit an intercept term")

  setDefault(fitIntercept, true)

  /** @group getParam */
  final def getFitIntercept: Boolean = $(fitIntercept)
}

/**
 * Trait for shared param handleInvalid. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasHandleInvalid extends Params {

  /**
   * Param for how to handle invalid entries. Options are skip (which will filter out rows with bad values), or error (which will throw an error). More options may be added later.
   * @group param
   */
  val handleInvalid: Param[String] = new Param[String](this, "handleInvalid", "how to handle invalid entries. Options are skip (which will filter out rows with bad values), or error (which will throw an error). More options may be added later", ParamValidators.inArray(Array("skip", "error")))

  /** @group getParam */
  final def getHandleInvalid: String = $(handleInvalid)
}

/**
 * Trait for shared param standardization (default: true). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasStandardization extends Params {

  /**
   * Param for whether to standardize the training features before fitting the model.
   * @group param
   */
  final val standardization: BooleanParam = new BooleanParam(this, "standardization", "whether to standardize the training features before fitting the model")

  setDefault(standardization, true)

  /** @group getParam */
  final def getStandardization: Boolean = $(standardization)
}

/**
 * Trait for shared param seed (default: this.getClass.getName.hashCode.toLong). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasSeed extends Params {

  /**
   * Param for random seed.
   * @group param
   */
  final val seed: LongParam = new LongParam(this, "seed", "random seed")

  setDefault(seed, this.getClass.getName.hashCode.toLong)

  /** @group getParam */
  final def getSeed: Long = $(seed)
}

/**
 * Trait for shared param elasticNetParam. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasElasticNetParam extends Params {

  /**
   * Param for the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   * @group param
   */
  final val elasticNetParam: DoubleParam = new DoubleParam(this, "elasticNetParam", "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty", ParamValidators.inRange(0, 1))

  /** @group getParam */
  final def getElasticNetParam: Double = $(elasticNetParam)
}

/**
 * Trait for shared param tol. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasTol extends Params {

  /**
   * Param for the convergence tolerance for iterative algorithms (&gt;= 0).
   * @group param
   */
  final val tol: DoubleParam = new DoubleParam(this, "tol", "the convergence tolerance for iterative algorithms (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getTol: Double = $(tol)
}

/**
 * Trait for shared param stepSize. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasStepSize extends Params {

  /**
   * Param for Step size to be used for each iteration of optimization (&gt; 0).
   * @group param
   */
  val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "Step size to be used for each iteration of optimization (> 0)", ParamValidators.gt(0))

  /** @group getParam */
  final def getStepSize: Double = $(stepSize)
}

/**
 * Trait for shared param weightCol. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasWeightCol extends Params {

  /**
   * Param for weight column name. If this is not set or empty, we treat all instance weights as 1.0.
   * @group param
   */
  final val weightCol: Param[String] = new Param[String](this, "weightCol", "weight column name. If this is not set or empty, we treat all instance weights as 1.0")

  /** @group getParam */
  final def getWeightCol: String = $(weightCol)
}

/**
 * Trait for shared param solver. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasSolver extends Params {

  /**
   * Param for the solver algorithm for optimization.
   * @group param
   */
  val solver: Param[String] = new Param[String](this, "solver", "the solver algorithm for optimization")

  /** @group getParam */
  final def getSolver: String = $(solver)
}

/**
 * Trait for shared param aggregationDepth (default: 2). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasAggregationDepth extends Params {

  /**
   * Param for suggested depth for treeAggregate (&gt;= 2).
   * @group expertParam
   */
  final val aggregationDepth: IntParam = new IntParam(this, "aggregationDepth", "suggested depth for treeAggregate (>= 2)", ParamValidators.gtEq(2))

  setDefault(aggregationDepth, 2)

  /** @group expertGetParam */
  final def getAggregationDepth: Int = $(aggregationDepth)
}

/**
 * Trait for shared param collectSubModels (default: false). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasCollectSubModels extends Params {

  /**
   * Param for whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver.
   * @group expertParam
   */
  final val collectSubModels: BooleanParam = new BooleanParam(this, "collectSubModels", "whether to collect a list of sub-models trained during tuning. If set to false, then only the single best sub-model will be available after fitting. If set to true, then all sub-models will be available. Warning: For large models, collecting all sub-models can cause OOMs on the Spark driver")

  setDefault(collectSubModels, false)

  /** @group expertGetParam */
  final def getCollectSubModels: Boolean = $(collectSubModels)
}

/**
 * Trait for shared param loss. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasLoss extends Params {

  /**
   * Param for the loss function to be optimized.
   * @group param
   */
  val loss: Param[String] = new Param[String](this, "loss", "the loss function to be optimized")

  /** @group getParam */
  final def getLoss: String = $(loss)
}

/**
 * Trait for shared param distanceMeasure (default: "euclidean"). This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasDistanceMeasure extends Params {

  /**
   * Param for The distance measure. Supported options: 'euclidean' and 'cosine'.
   * @group param
   */
  final val distanceMeasure: Param[String] = new Param[String](this, "distanceMeasure", "The distance measure. Supported options: 'euclidean' and 'cosine'", ParamValidators.inArray(Array("euclidean", "cosine")))

  setDefault(distanceMeasure, "euclidean")

  /** @group getParam */
  final def getDistanceMeasure: String = $(distanceMeasure)
}

/**
 * Trait for shared param validationIndicatorCol. This trait may be changed or
 * removed between minor versions.
 */
@DeveloperApi
trait HasValidationIndicatorCol extends Params {

  /**
   * Param for name of the column that indicates whether each row is for training or for validation. False indicates training; true indicates validation..
   * @group param
   */
  final val validationIndicatorCol: Param[String] = new Param[String](this, "validationIndicatorCol", "name of the column that indicates whether each row is for training or for validation. False indicates training; true indicates validation.")

  /** @group getParam */
  final def getValidationIndicatorCol: String = $(validationIndicatorCol)
}
// scalastyle:on
