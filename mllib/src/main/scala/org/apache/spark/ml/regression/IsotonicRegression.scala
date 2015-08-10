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

package org.apache.spark.ml.regression

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.regression.{IsotonicRegression => MLlibIsotonicRegression, IsotonicRegressionModel => MLlibIsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Params for isotonic regression.
 */
private[regression] trait IsotonicRegressionBase extends Params with HasFeaturesCol
  with HasLabelCol with HasPredictionCol with Logging {

  /**
   * Param for weight column name (default: none).
   * @group param
   */
  // TODO: Move weightCol to sharedParams.
  final val weightCol: Param[String] =
    new Param[String](this, "weightCol",
      "weight column name. If this is not set or empty, we treat all instance weights as 1.0.")

  /** @group getParam */
  final def getWeightCol: String = $(weightCol)

  /**
   * Param for whether the output sequence should be isotonic/increasing (true) or
   * antitonic/decreasing (false).
   * @group param
   */
  final val isotonic: BooleanParam =
    new BooleanParam(this, "isotonic",
      "whether the output sequence should be isotonic/increasing (true) or" +
        "antitonic/decreasing (false)")

  /** @group getParam */
  final def getIsotonic: Boolean = $(isotonic)

  /**
   * Param for the index of the feature if [[featuresCol]] is a vector column (default: `0`), no
   * effect otherwise.
   * @group param
   */
  final val featureIndex: IntParam = new IntParam(this, "featureIndex",
    "The index of the feature if featuresCol is a vector column, no effect otherwise.")

  /** @group getParam */
  final def getFeatureIndex: Int = $(featureIndex)

  setDefault(isotonic -> true, featureIndex -> 0)

  /** Checks whether the input has weight column. */
  protected[ml] def hasWeightCol: Boolean = {
    isDefined(weightCol) && $(weightCol) != ""
  }

  /**
   * Extracts (label, feature, weight) from input dataset.
   */
  protected[ml] def extractWeightedLabeledPoints(
      dataset: DataFrame): RDD[(Double, Double, Double)] = {
    val f = if (dataset.schema($(featuresCol)).dataType.isInstanceOf[VectorUDT]) {
      val idx = $(featureIndex)
      val extract = udf { v: Vector => v(idx) }
      extract(col($(featuresCol)))
    } else {
      col($(featuresCol))
    }
    val w = if (hasWeightCol) {
      col($(weightCol))
    } else {
      lit(1.0)
    }
    dataset.select(col($(labelCol)), f, w)
      .map { case Row(label: Double, feature: Double, weights: Double) =>
      (label, feature, weights)
    }
  }

  /**
   * Validates and transforms input schema.
   * @param schema input schema
   * @param fitting whether this is in fitting or prediction
   * @return output schema
   */
  protected[ml] def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean): StructType = {
    if (fitting) {
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
      if (hasWeightCol) {
        SchemaUtils.checkColumnType(schema, $(weightCol), DoubleType)
      } else {
        logInfo("The weight column is not defined. Treat all instance weights as 1.0.")
      }
    }
    val featuresType = schema($(featuresCol)).dataType
    require(featuresType == DoubleType || featuresType.isInstanceOf[VectorUDT])
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

/**
 * :: Experimental ::
 * Isotonic regression.
 *
 * Currently implemented using parallelized pool adjacent violators algorithm.
 * Only univariate (single feature) algorithm supported.
 *
 * Uses [[org.apache.spark.mllib.regression.IsotonicRegression]].
 */
@Experimental
class IsotonicRegression(override val uid: String) extends Estimator[IsotonicRegressionModel]
  with IsotonicRegressionBase {

  def this() = this(Identifiable.randomUID("isoReg"))

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setIsotonic(value: Boolean): this.type = set(isotonic, value)

  /** @group setParam */
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /** @group setParam */
  def setFeatureIndex(value: Int): this.type = set(featureIndex, value)

  override def copy(extra: ParamMap): IsotonicRegression = defaultCopy(extra)

  override def fit(dataset: DataFrame): IsotonicRegressionModel = {
    validateAndTransformSchema(dataset.schema, fitting = true)
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractWeightedLabeledPoints(dataset)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val isotonicRegression = new MLlibIsotonicRegression().setIsotonic($(isotonic))
    val oldModel = isotonicRegression.run(instances)

    copyValues(new IsotonicRegressionModel(uid, oldModel).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }
}

/**
 * :: Experimental ::
 * Model fitted by IsotonicRegression.
 * Predicts using a piecewise linear function.
 *
 * For detailed rules see [[org.apache.spark.mllib.regression.IsotonicRegressionModel.predict()]].
 *
 * @param oldModel A [[org.apache.spark.mllib.regression.IsotonicRegressionModel]]
 *                 model trained by [[org.apache.spark.mllib.regression.IsotonicRegression]].
 */
@Experimental
class IsotonicRegressionModel private[ml] (
    override val uid: String,
    private val oldModel: MLlibIsotonicRegressionModel)
  extends Model[IsotonicRegressionModel] with IsotonicRegressionBase {

  /** @group setParam */
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setFeatureIndex(value: Int): this.type = set(featureIndex, value)

  /** Boundaries in increasing order for which predictions are known. */
  def boundaries: Vector = Vectors.dense(oldModel.boundaries)

  /**
   * Predictions associated with the boundaries at the same index, monotone because of isotonic
   * regression.
   */
  def predictions: Vector = Vectors.dense(oldModel.predictions)

  override def copy(extra: ParamMap): IsotonicRegressionModel = {
    copyValues(new IsotonicRegressionModel(uid, oldModel), extra)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val predict = dataset.schema($(featuresCol)).dataType match {
      case DoubleType =>
        udf { feature: Double => oldModel.predict(feature) }
      case _: VectorUDT =>
        val idx = $(featureIndex)
        udf { features: Vector => oldModel.predict(features(idx)) }
    }
    dataset.withColumn($(predictionCol), predict(col($(featuresCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false)
  }
}
