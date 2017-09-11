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

package org.apache.spark.ml.tuning

import java.io.IOException
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.existentials

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasCollectSubModels, HasParallelism, HasPersistSubModelsPath}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils

/**
 * Params for [[TrainValidationSplit]] and [[TrainValidationSplitModel]].
 */
private[ml] trait TrainValidationSplitParams extends ValidatorParams {
  /**
   * Param for ratio between train and validation data. Must be between 0 and 1.
   * Default: 0.75
   *
   * @group param
   */
  val trainRatio: DoubleParam = new DoubleParam(this, "trainRatio",
    "ratio between training set and validation set (>= 0 && <= 1)", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getTrainRatio: Double = $(trainRatio)

  setDefault(trainRatio -> 0.75)
}

/**
 * Validation for hyper-parameter tuning.
 * Randomly splits the input dataset into train and validation sets,
 * and uses evaluation metric on the validation set to select the best model.
 * Similar to [[CrossValidator]], but only splits the set once.
 */
@Since("1.5.0")
class TrainValidationSplit @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Estimator[TrainValidationSplitModel]
  with TrainValidationSplitParams with HasParallelism with HasCollectSubModels
  with HasPersistSubModelsPath with MLWritable with Logging {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("tvs"))

  /** @group setParam */
  @Since("1.5.0")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  @Since("1.5.0")
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  @Since("1.5.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.5.0")
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Set the mamixum level of parallelism to evaluate models in parallel.
   * Default is 1 for serial evaluation
   *
   * @group expertSetParam
   */
  @Since("2.3.0")
  def setParallelism(value: Int): this.type = set(parallelism, value)

  /** @group expertSetParam */
  @Since("2.3.0")
  def setCollectSubModels(value: Boolean): this.type = set(collectSubModels, value)

  /** @group expertSetParam */
  @Since("2.3.0")
  def setPersistSubModelsPath(value: String): this.type = set(persistSubModelsPath, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): TrainValidationSplitModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)

    // Create execution context based on $(parallelism)
    val executionContext = getExecutionContext

    val instr = Instrumentation.create(this, dataset)
    instr.logParams(trainRatio, seed, parallelism)
    logTuningParams(instr)

    val Array(trainingDataset, validationDataset) =
      dataset.randomSplit(Array($(trainRatio), 1 - $(trainRatio)), $(seed))
    trainingDataset.cache()
    validationDataset.cache()

    val collectSubModelsParam = $(collectSubModels)
    val persistSubModelsPathParam = $(persistSubModelsPath)

    var subModels: Array[Model[_]] = if (collectSubModelsParam) {
      Array.fill[Model[_]](epm.length)(null)
    } else null

    // Fit models in a Future for training in parallel
    logDebug(s"Train split with multiple sets of parameters.")
    val modelFutures = epm.zipWithIndex.map { case (paramMap, paramIndex) =>
      Future[Model[_]] {
        val model = est.fit(trainingDataset, paramMap).asInstanceOf[Model[_]]

        if (collectSubModelsParam) {
          subModels(paramIndex) = model
        }
        if (persistSubModelsPathParam.nonEmpty) {
          val modelPath = new Path(persistSubModelsPathParam, paramIndex.toString).toString
          model.asInstanceOf[MLWritable].save(modelPath)
        }
        model
      } (executionContext)
    }

    // Unpersist training data only when all models have trained
    Future.sequence[Model[_], Iterable](modelFutures)(implicitly, executionContext)
      .onComplete { _ => trainingDataset.unpersist() } (executionContext)

    // Evaluate models in a Future that will calulate a metric and allow model to be cleaned up
    val metricFutures = modelFutures.zip(epm).map { case (modelFuture, paramMap) =>
      modelFuture.map { model =>
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(model.transform(validationDataset, paramMap))
        logDebug(s"Got metric $metric for model trained with $paramMap.")
        metric
      } (executionContext)
    }

    // Wait for all metrics to be calculated
    val metrics = metricFutures.map(ThreadUtils.awaitResult(_, Duration.Inf))

    // Unpersist validation set once all metrics have been produced
    validationDataset.unpersist()

    logInfo(s"Train validation split metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best train validation split metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    instr.logSuccess(bestModel)
    copyValues(new TrainValidationSplitModel(uid, bestModel, metrics, subModels).setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  @Since("1.5.0")
  override def copy(extra: ParamMap): TrainValidationSplit = {
    val copied = defaultCopy(extra).asInstanceOf[TrainValidationSplit]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  @Since("2.0.0")
  override def write: MLWriter = new TrainValidationSplit.TrainValidationSplitWriter(this)
}

@Since("2.0.0")
object TrainValidationSplit extends MLReadable[TrainValidationSplit] {

  @Since("2.0.0")
  override def read: MLReader[TrainValidationSplit] = new TrainValidationSplitReader

  @Since("2.0.0")
  override def load(path: String): TrainValidationSplit = super.load(path)

  private[TrainValidationSplit] class TrainValidationSplitWriter(instance: TrainValidationSplit)
    extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class TrainValidationSplitReader extends MLReader[TrainValidationSplit] {

    /** Checked against metadata when loading model */
    private val className = classOf[TrainValidationSplit].getName

    override def load(path: String): TrainValidationSplit = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val tvs = new TrainValidationSplit(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(tvs, metadata, skipParams = List("estimatorParamMaps"))
      tvs
    }
  }
}

/**
 * Model from train validation split.
 *
 * @param uid Id.
 * @param bestModel Estimator determined best model.
 * @param validationMetrics Evaluated validation metrics.
 */
@Since("1.5.0")
class TrainValidationSplitModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("1.5.0") val bestModel: Model[_],
    @Since("1.5.0") val validationMetrics: Array[Double],
    @Since("2.3.0") val subModels: Array[Model[_]])
  extends Model[TrainValidationSplitModel] with TrainValidationSplitParams with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], validationMetrics: JList[Double]) = {
    this(uid, bestModel, validationMetrics.asScala.toArray, null)
  }

  private[ml] def this(uid: String, bestModel: Model[_], validationMetrics: Array[Double]) = {
    this(uid, bestModel, validationMetrics, null)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): TrainValidationSplitModel = {
    val copied = new TrainValidationSplitModel (
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      validationMetrics.clone(),
      TrainValidationSplitModel.copySubModels(subModels))
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter = new TrainValidationSplitModel.TrainValidationSplitModelWriter(this)

  @Since("2.3.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  def save(path: String, persistSubModels: Boolean): Unit = {
    write.asInstanceOf[TrainValidationSplitModel.TrainValidationSplitModelWriter]
      .persistSubModels(persistSubModels).save(path)
  }
}

@Since("2.0.0")
object TrainValidationSplitModel extends MLReadable[TrainValidationSplitModel] {

  private[TrainValidationSplitModel] def copySubModels(subModels: Array[Model[_]]) = {
    var copiedSubModels: Array[Model[_]] = null
    if (subModels != null) {
      val numParamMaps = subModels.length
      copiedSubModels = Array.fill[Model[_]](numParamMaps)(null)
      for (i <- 0 until numParamMaps) {
          copiedSubModels(i) = subModels(i).copy(ParamMap.empty).asInstanceOf[Model[_]]
      }
    }
    copiedSubModels
  }

  @Since("2.0.0")
  override def read: MLReader[TrainValidationSplitModel] = new TrainValidationSplitModelReader

  @Since("2.0.0")
  override def load(path: String): TrainValidationSplitModel = super.load(path)

  private[TrainValidationSplitModel]
  class TrainValidationSplitModelWriter(instance: TrainValidationSplitModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    protected var shouldPersistSubModels: Boolean = false

    /**
     * Set option for persist sub models.
     */
    @Since("2.3.0")
    def persistSubModels(persist: Boolean): this.type = {
      shouldPersistSubModels = persist
      this
    }

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = ("validationMetrics" -> instance.validationMetrics.toSeq) ~
        ("shouldPersistSubModels" -> shouldPersistSubModels)
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
      if (shouldPersistSubModels) {
        require(instance.subModels != null, "Cannot get sub models to persist.")
        val subModelsPath = new Path(path, "subModels")
        for (paramIndex <- 0 until instance.getEstimatorParamMaps.length) {
          val modelPath = new Path(subModelsPath, paramIndex.toString).toString
          instance.subModels(paramIndex).asInstanceOf[MLWritable].save(modelPath)
        }
      }
    }
  }

  private class TrainValidationSplitModelReader extends MLReader[TrainValidationSplitModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[TrainValidationSplitModel].getName

    override def load(path: String): TrainValidationSplitModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val validationMetrics = (metadata.metadata \ "validationMetrics").extract[Seq[Double]].toArray
      val shouldPersistSubModels = (metadata.metadata \ "shouldPersistSubModels").extract[Boolean]

      val subModels: Array[Model[_]] = if (shouldPersistSubModels) {
        val subModelsPath = new Path(path, "subModels")
        val _subModels = Array.fill[Model[_]](estimatorParamMaps.length)(null)
        for (paramIndex <- 0 until estimatorParamMaps.length) {
          val modelPath = new Path(subModelsPath, paramIndex.toString).toString
          _subModels(paramIndex) =
            DefaultParamsReader.loadParamsInstance(modelPath, sc)
        }
        _subModels
      } else null

      val model = new TrainValidationSplitModel(metadata.uid, bestModel, validationMetrics,
        subModels)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(model, metadata, skipParams = List("estimatorParamMaps"))
      model
    }
  }
}
