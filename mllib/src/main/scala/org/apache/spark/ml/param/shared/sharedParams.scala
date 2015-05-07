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

import org.apache.spark.ml.param._
import org.apache.spark.util.Utils

// DO NOT MODIFY THIS FILE! It was generated by SharedParamsCodeGen.

// scalastyle:off

/**
 * (private[ml]) Trait for shared param regParam.
 */
private[ml] trait HasRegParam extends Params {

  /**
   * Param for regularization parameter (>= 0).
   * @group param
   */
  final val regParam: DoubleParam = new DoubleParam(uid, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getRegParam: Double = $(regParam)
}

/**
 * (private[ml]) Trait for shared param maxIter.
 */
private[ml] trait HasMaxIter extends Params {

  /**
   * Param for max number of iterations (>= 0).
   * @group param
   */
  final val maxIter: IntParam = new IntParam(uid, "maxIter", "max number of iterations (>= 0)", ParamValidators.gtEq(0))

  /** @group getParam */
  final def getMaxIter: Int = $(maxIter)
}

/**
 * (private[ml]) Trait for shared param featuresCol (default: "features").
 */
private[ml] trait HasFeaturesCol extends Params {

  /**
   * Param for features column name.
   * @group param
   */
  final val featuresCol: Param[String] = new Param[String](uid, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  /** @group getParam */
  final def getFeaturesCol: String = $(featuresCol)
}

/**
 * (private[ml]) Trait for shared param labelCol (default: "label").
 */
private[ml] trait HasLabelCol extends Params {

  /**
   * Param for label column name.
   * @group param
   */
  final val labelCol: Param[String] = new Param[String](uid, "labelCol", "label column name")

  setDefault(labelCol, "label")

  /** @group getParam */
  final def getLabelCol: String = $(labelCol)
}

/**
 * (private[ml]) Trait for shared param predictionCol (default: "prediction").
 */
private[ml] trait HasPredictionCol extends Params {

  /**
   * Param for prediction column name.
   * @group param
   */
  final val predictionCol: Param[String] = new Param[String](uid, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  /** @group getParam */
  final def getPredictionCol: String = $(predictionCol)
}

/**
 * (private[ml]) Trait for shared param rawPredictionCol (default: "rawPrediction").
 */
private[ml] trait HasRawPredictionCol extends Params {

  /**
   * Param for raw prediction (a.k.a. confidence) column name.
   * @group param
   */
  final val rawPredictionCol: Param[String] = new Param[String](uid, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")

  setDefault(rawPredictionCol, "rawPrediction")

  /** @group getParam */
  final def getRawPredictionCol: String = $(rawPredictionCol)
}

/**
 * (private[ml]) Trait for shared param probabilityCol (default: "probability").
 */
private[ml] trait HasProbabilityCol extends Params {

  /**
   * Param for Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities..
   * @group param
   */
  final val probabilityCol: Param[String] = new Param[String](uid, "probabilityCol", "Column name for predicted class conditional probabilities. Note: Not all models output well-calibrated probability estimates! These probabilities should be treated as confidences, not precise probabilities.")

  setDefault(probabilityCol, "probability")

  /** @group getParam */
  final def getProbabilityCol: String = $(probabilityCol)
}

/**
 * (private[ml]) Trait for shared param threshold.
 */
private[ml] trait HasThreshold extends Params {

  /**
   * Param for threshold in binary classification prediction, in range [0, 1].
   * @group param
   */
  final val threshold: DoubleParam = new DoubleParam(uid, "threshold", "threshold in binary classification prediction, in range [0, 1]", ParamValidators.inRange(0, 1))

  /** @group getParam */
  final def getThreshold: Double = $(threshold)
}

/**
 * (private[ml]) Trait for shared param inputCol.
 */
private[ml] trait HasInputCol extends Params {

  /**
   * Param for input column name.
   * @group param
   */
  final val inputCol: Param[String] = new Param[String](uid, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = $(inputCol)
}

/**
 * (private[ml]) Trait for shared param inputCols.
 */
private[ml] trait HasInputCols extends Params {

  /**
   * Param for input column names.
   * @group param
   */
  final val inputCols: Param[Array[String]] = new Param[Array[String]](uid, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = $(inputCols)
}

/**
 * (private[ml]) Trait for shared param outputCol.
 */
private[ml] trait HasOutputCol extends Params {

  /**
   * Param for output column name.
   * @group param
   */
  final val outputCol: Param[String] = new Param[String](uid, "outputCol", "output column name")

  /** @group getParam */
  final def getOutputCol: String = $(outputCol)
}

/**
 * (private[ml]) Trait for shared param checkpointInterval.
 */
private[ml] trait HasCheckpointInterval extends Params {

  /**
   * Param for checkpoint interval (>= 1).
   * @group param
   */
  final val checkpointInterval: IntParam = new IntParam(uid, "checkpointInterval", "checkpoint interval (>= 1)", ParamValidators.gtEq(1))

  /** @group getParam */
  final def getCheckpointInterval: Int = $(checkpointInterval)
}

/**
 * (private[ml]) Trait for shared param fitIntercept (default: true).
 */
private[ml] trait HasFitIntercept extends Params {

  /**
   * Param for whether to fit an intercept term.
   * @group param
   */
  final val fitIntercept: BooleanParam = new BooleanParam(uid, "fitIntercept", "whether to fit an intercept term")

  setDefault(fitIntercept, true)

  /** @group getParam */
  final def getFitIntercept: Boolean = $(fitIntercept)
}

/**
 * (private[ml]) Trait for shared param seed (default: Utils.random.nextLong()).
 */
private[ml] trait HasSeed extends Params {

  /**
   * Param for random seed.
   * @group param
   */
  final val seed: LongParam = new LongParam(uid, "seed", "random seed")

  setDefault(seed, Utils.random.nextLong())

  /** @group getParam */
  final def getSeed: Long = $(seed)
}

/**
 * (private[ml]) Trait for shared param elasticNetParam.
 */
private[ml] trait HasElasticNetParam extends Params {

  /**
   * Param for the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty..
   * @group param
   */
  final val elasticNetParam: DoubleParam = new DoubleParam(uid, "elasticNetParam", "the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.", ParamValidators.inRange(0, 1))

  /** @group getParam */
  final def getElasticNetParam: Double = $(elasticNetParam)
}

/**
 * (private[ml]) Trait for shared param tol.
 */
private[ml] trait HasTol extends Params {

  /**
   * Param for the convergence tolerance for iterative algorithms.
   * @group param
   */
  final val tol: DoubleParam = new DoubleParam(uid, "tol", "the convergence tolerance for iterative algorithms")

  /** @group getParam */
  final def getTol: Double = $(tol)
}

/**
 * (private[ml]) Trait for shared param stepSize.
 */
private[ml] trait HasStepSize extends Params {

  /**
   * Param for Step size to be used for each iteration of optimization..
   * @group param
   */
  final val stepSize: DoubleParam = new DoubleParam(uid, "stepSize", "Step size to be used for each iteration of optimization.")

  /** @group getParam */
  final def getStepSize: Double = $(stepSize)
}
// scalastyle:on
