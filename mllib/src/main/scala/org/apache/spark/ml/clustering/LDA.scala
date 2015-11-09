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

package org.apache.spark.ml.clustering

import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.util.{SchemaUtils, Identifiable}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.shared.{HasCheckpointInterval, HasFeaturesCol, HasSeed, HasMaxIter}
import org.apache.spark.ml.param._
import org.apache.spark.mllib.clustering.{DistributedLDAModel => OldDistributedLDAModel,
    EMLDAOptimizer => OldEMLDAOptimizer, LDA => OldLDA, LDAModel => OldLDAModel,
    LDAOptimizer => OldLDAOptimizer, LocalLDAModel => OldLocalLDAModel,
    OnlineLDAOptimizer => OldOnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{VectorUDT, Vectors, Matrix, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.functions.{col, monotonicallyIncreasingId, udf}
import org.apache.spark.sql.types.StructType


private[clustering] trait LDAParams extends Params with HasFeaturesCol with HasMaxIter
  with HasSeed with HasCheckpointInterval {

  /**
   * Param for the number of topics (clusters) to infer. Must be > 1. Default: 10.
   * @group param
   */
  @Since("1.6.0")
  final val k = new IntParam(this, "k", "number of topics (clusters) to infer",
    ParamValidators.gt(1))

  /** @group getParam */
  @Since("1.6.0")
  def getK: Int = $(k)

  /**
   * Concentration parameter (commonly named "alpha") for the prior placed on documents'
   * distributions over topics ("theta").
   *
   * This is the parameter to a Dirichlet distribution, where larger values mean more smoothing
   * (more regularization).
   *
   * If set to a singleton vector [-1], then docConcentration is set automatically. If set to
   * singleton vector [alpha] where alpha != -1, then alpha is replicated to a vector of
   * length k in fitting. Otherwise, the [[docConcentration]] vector must be length k.
   * (default = [-1] = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Currently only supports symmetric distributions, so all values in the vector should be
   *       the same.
   *     - Values should be > 1.0
   *     - default = uniformly (50 / k) + 1, where 50/k is common in LDA libraries and +1 follows
   *       from Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Values should be >= 0
   *     - default = uniformly (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val docConcentration = new DoubleArrayParam(this, "docConcentration",
    "Concentration parameter (commonly named \"alpha\") for the prior placed on documents'" +
      " distributions over topics (\"theta\").", validDocConcentration)

  /** Check that the docConcentration is valid, independently of other Params */
  private def validDocConcentration(alpha: Array[Double]): Boolean = {
    if (alpha.length == 1) {
      alpha(0) == -1 || alpha(0) >= 1.0
    } else if (alpha.length > 1) {
      alpha.forall(_ >= 1.0)
    } else {
      false
    }
  }

  /** @group getParam */
  @Since("1.6.0")
  def getDocConcentration: Array[Double] = $(docConcentration)

  /**
   * Concentration parameter (commonly named "beta" or "eta") for the prior placed on topics'
   * distributions over terms.
   *
   * This is the parameter to a symmetric Dirichlet distribution.
   *
   * Note: The topics' distributions over terms are called "beta" in the original LDA paper
   * by Blei et al., but are called "phi" in many later papers such as Asuncion et al., 2009.
   *
   * If set to -1, then topicConcentration is set automatically.
   *  (default = -1 = automatic)
   *
   * Optimizer-specific parameter settings:
   *  - EM
   *     - Value should be > 1.0
   *     - default = 0.1 + 1, where 0.1 gives a small amount of smoothing and +1 follows
   *       Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   *  - Online
   *     - Value should be >= 0
   *     - default = (1.0 / k), following the implementation from
   *       [[https://github.com/Blei-Lab/onlineldavb]].
   * @group param
   */
  @Since("1.6.0")
  final val topicConcentration = new DoubleParam(this, "topicConcentration",
    "Concentration parameter (commonly named \"beta\" or \"eta\") for the prior placed on topic'" +
      " distributions over terms.", (beta: Double) => beta == -1 || beta >= 0.0)

  /** @group getParam */
  @Since("1.6.0")
  def getTopicConcentration: Double = $(topicConcentration)

  /**
   * Optimizer or inference algorithm used to estimate the LDA model, specified as a
   * [[LDAOptimizer]] type.
   * Currently supported:
   *  - Online Variational Bayes: [[OnlineLDAOptimizer]] (default)
   *  - Expectation-Maximization (EM): [[EMLDAOptimizer]]
   * @group param
   */
  @Since("1.6.0")
  final val optimizer = new Param[LDAOptimizer](this, "optimizer", "Optimizer or inference" +
    " algorithm used to estimate the LDA model")

  /** @group getParam */
  @Since("1.6.0")
  def getOptimizer: LDAOptimizer = $(optimizer)

  /**
   * Output column with estimates of the topic mixture distribution for each document (often called
   * "theta" in the literature).  Returns a vector of zeros for an empty document.
   *
   * This uses a variational approximation following Hoffman et al. (2010), where the approximate
   * distribution is called "gamma."  Technically, this method returns this approximation "gamma"
   * for each document.
   * @group param
   */
  @Since("1.6.0")
  final val topicDistributionCol = new Param[String](this, "topicDistribution", "Output column" +
    " with estimates of the topic mixture distribution for each document (often called \"theta\"" +
    " in the literature).  Returns a vector of zeros for an empty document.")

  setDefault(topicDistributionCol -> "topicDistribution")

  /** @group getParam */
  @Since("1.6.0")
  def getTopicDistributionCol: String = $(topicDistributionCol)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(topicDistributionCol), new VectorUDT)
  }

  override def validateParams(): Unit = {
    if (getDocConcentration.length != 1) {
      require(getDocConcentration.length == getK, s"LDA docConcentration was of length" +
        s" ${getDocConcentration.length}, but k = $getK.  docConcentration must be either" +
        s" length 1 (scalar) or an array of length k.")
    }
  }
}


/**
 * :: Experimental ::
 * Model fitted by [[LDA]].
 *
 * @param vocabSize  Vocabulary size (number of terms or terms in the vocabulary)
 * @param oldLocalModel  Underlying spark.mllib model.
 *                       If this model was produced by [[OnlineLDAOptimizer]], then this is the
 *                       only model representation.
 *                       If this model was produced by [[EMLDAOptimizer]], then this local
 *                       representation may be built lazily.
 * @param sqlContext  Used to construct local DataFrames for returning query results
 */
@Since("1.6.0")
@Experimental
class LDAModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("1.6.0") val vocabSize: Int,
    @Since("1.6.0") protected var oldLocalModel: Option[OldLocalLDAModel],
    @Since("1.6.0") @transient protected val sqlContext: SQLContext)
  extends Model[LDAModel] with LDAParams with Logging {

  /** Returns underlying spark.mllib model */
  @Since("1.6.0")
  protected def getModel: OldLDAModel = oldLocalModel match {
    case Some(m) => m
    case None =>
      // Should never happen.
      throw new RuntimeException("LDAModel required local model format," +
        " but the underlying model is missing.")
  }

  /**
   * The features for LDA should be a [[Vector]] representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDAModel = {
    val copied = new LDAModel(uid, vocabSize, oldLocalModel, sqlContext)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def transform(dataset: DataFrame): DataFrame = {
    if ($(topicDistributionCol).nonEmpty) {
      val t = udf(oldLocalModel.get.getTopicDistributionMethod(sqlContext.sparkContext))
      dataset.withColumn($(topicDistributionCol), t(col($(featuresCol))))
    } else {
      logWarning("LDAModel.transform was called as a noop. Set an output column such as" +
        " topicDistributionCol to produce results.")
      dataset
    }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  /**
   * Value for [[docConcentration]] estimated from data.
   * If [[OnlineLDAOptimizer]] was used and [[OnlineLDAOptimizer.optimizeDocConcentration]] was set
   * to false, then this returns the fixed (given) value for the [[docConcentration]] parameter.
   */
  @Since("1.6.0")
  def estimatedDocConcentration: Vector = getModel.docConcentration

  /**
   * Inferred topics, where each topic is represented by a distribution over terms.
   * This is a matrix of size vocabSize x k, where each column is a topic.
   * No guarantees are given about the ordering of the topics.
   *
   * WARNING: If this model is actually a [[DistributedLDAModel]] instance from [[EMLDAOptimizer]],
   *          then this method could involve collecting a large amount of data to the driver
   *          (on the order of vocabSize x k).
   */
  @Since("1.6.0")
  def topicsMatrix: Matrix = getModel.topicsMatrix

  /** Indicates whether this instance is of type [[DistributedLDAModel]] */
  @Since("1.6.0")
  def isDistributed: Boolean = false

  /**
   * Calculates a lower bound on the log likelihood of the entire corpus.
   *
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * WARNING: If this model was learned via a [[DistributedLDAModel]], this involves collecting
   *          a large [[topicsMatrix]] to the driver.  This implementation may be changed in the
   *          future.
   *
   * @param dataset  test corpus to use for calculating log likelihood
   * @return variational lower bound on the log likelihood of the entire corpus
   */
  @Since("1.6.0")
  def logLikelihood(dataset: DataFrame): Double = oldLocalModel match {
    case Some(m) =>
      val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
      m.logLikelihood(oldDataset)
    case None =>
      // Should never happen.
      throw new RuntimeException("LocalLDAModel.logLikelihood was called," +
        " but the underlying model is missing.")
  }

  /**
   * Calculate an upper bound bound on perplexity.  (Lower is better.)
   * See Equation (16) in the Online LDA paper (Hoffman et al., 2010).
   *
   * @param dataset test corpus to use for calculating perplexity
   * @return Variational upper bound on log perplexity per token.
   */
  @Since("1.6.0")
  def logPerplexity(dataset: DataFrame): Double = oldLocalModel match {
    case Some(m) =>
      val oldDataset = LDA.getOldDataset(dataset, $(featuresCol))
      m.logPerplexity(oldDataset)
    case None =>
      // Should never happen.
      throw new RuntimeException("LocalLDAModel.logPerplexity was called," +
        " but the underlying model is missing.")
  }

  /**
   * Return the topics described by their top-weighted terms.
   *
   * @param maxTermsPerTopic  Maximum number of terms to collect for each topic.
   *                          Default value of 10.
   * @return  Local DataFrame with one topic per Row, with columns:
   *           - "topic": IntegerType: topic index
   *           - "termIndices": ArrayType(IntegerType): term indices, sorted in order of decreasing
   *                            term importance
   *           - "termWeights": ArrayType(DoubleType): corresponding sorted term weights
   */
  @Since("1.6.0")
  def describeTopics(maxTermsPerTopic: Int): DataFrame = {
    val topics = getModel.describeTopics(maxTermsPerTopic).zipWithIndex.map {
      case ((termIndices, termWeights), topic) =>
        (topic, termIndices.toSeq, termWeights.toSeq)
    }
    sqlContext.createDataFrame(topics).toDF("topic", "termIndices", "termWeights")
  }

  @Since("1.6.0")
  def describeTopics(): DataFrame = describeTopics(10)
}


/**
 * :: Experimental ::
 *
 * Distributed model fitted by [[LDA]] using the [[EMLDAOptimizer]].
 *
 * This model stores the inferred topics, the full training dataset, and the topic distribution
 * for each training document.
 */
@Since("1.6.0")
@Experimental
class DistributedLDAModel private[ml] (
    uid: String,
    vocabSize: Int,
    private val oldDistributedModel: OldDistributedLDAModel,
    sqlContext: SQLContext)
  extends LDAModel(uid, vocabSize, None, sqlContext) {

  /**
   * Convert this distributed model to a local representation.  This discards info about the
   * training dataset.
   */
  @Since("1.6.0")
  def toLocal: LDAModel = {
    if (oldLocalModel.isEmpty) {
      oldLocalModel = Some(oldDistributedModel.toLocal)
    }
    new LDAModel(uid, vocabSize, oldLocalModel, sqlContext)
  }

  @Since("1.6.0")
  override protected def getModel: OldLDAModel = oldDistributedModel

  @Since("1.6.0")
  override def copy(extra: ParamMap): DistributedLDAModel = {
    val copied = new DistributedLDAModel(uid, vocabSize, oldDistributedModel, sqlContext)
    if (oldLocalModel.nonEmpty) copied.oldLocalModel = oldLocalModel
    copyValues(copied, extra).setParent(parent)
    copied
  }

  @Since("1.6.0")
  override def topicsMatrix: Matrix = {
    if (oldLocalModel.isEmpty) {
      oldLocalModel = Some(oldDistributedModel.toLocal)
    }
    super.topicsMatrix
  }

  @Since("1.6.0")
  override def isDistributed: Boolean = true

  @Since("1.6.0")
  override def logLikelihood(dataset: DataFrame): Double = {
    if (oldLocalModel.isEmpty) {
      oldLocalModel = Some(oldDistributedModel.toLocal)
    }
    super.logLikelihood(dataset)
  }

  @Since("1.6.0")
  override def logPerplexity(dataset: DataFrame): Double = {
    if (oldLocalModel.isEmpty) {
      oldLocalModel = Some(oldDistributedModel.toLocal)
    }
    super.logPerplexity(dataset)
  }

  /**
   * Log likelihood of the observed tokens in the training set,
   * given the current parameter estimates:
   *  log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)
   *
   * Notes:
   *  - This excludes the prior; for that, use [[logPrior]].
   *  - Even with [[logPrior]], this is NOT the same as the data log likelihood given the
   *    hyperparameters.
   *  - This is computed from the topic distributions computed during training. If you call
   *    [[logLikelihood()]] on the same training dataset, the topic distributions will be computed
   *    again, possibly giving different results.
   */
  @Since("1.6.0")
  lazy val trainingLogLikelihood: Double = oldDistributedModel.logLikelihood

  /**
   * Log probability of the current parameter estimate:
   * log P(topics, topic distributions for docs | Dirichlet hyperparameters)
   */
  @Since("1.6.0")
  lazy val logPrior: Double = oldDistributedModel.logPrior
}


/**
 * :: Experimental ::
 *
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "term" = "word": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over terms representing some concept
 *  - "document": one piece of text, corresponding to one row in the input data
 *
 * References:
 *  - Original LDA paper (journal version):
 *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *
 * Input data (featuresCol):
 *  LDA is given a collection of documents as input data, via the featuresCol parameter.
 *  Each document is specified as a [[Vector]] of length vocabSize, where each entry is the
 *  count for the corresponding term (word) in the document.  Feature transformers such as
 *  [[org.apache.spark.ml.feature.Tokenizer]] and [[org.apache.spark.ml.feature.CountVectorizer]]
 *  can be useful for converting text to word count vectors.
 *
 * @see [[http://en.wikipedia.org/wiki/Latent_Dirichlet_allocation Latent Dirichlet allocation
 *       (Wikipedia)]]
 */
@Since("1.6.0")
@Experimental
class LDA @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends Estimator[LDAModel] with LDAParams {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("lda"))

  setDefault(maxIter -> 20, k -> 10, docConcentration -> Array(-1.0), topicConcentration -> -1.0,
    optimizer -> new OnlineLDAOptimizer, checkpointInterval -> 10)

  /**
   * The features for LDA should be a [[Vector]] representing the word counts in a document.
   * The vector should be of length vocabSize, with counts for each term (word).
   * @group setParam
   */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("1.6.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /** @group setParam */
  @Since("1.6.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** @group setParam */
  @Since("1.6.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Array[Double]): this.type = set(docConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setDocConcentration(value: Double): this.type = set(docConcentration, Array(value))

  /** @group setParam */
  @Since("1.6.0")
  def setTopicConcentration(value: Double): this.type = set(topicConcentration, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOptimizer(value: LDAOptimizer): this.type = set(optimizer, value)

  /**
   * Set [[optimizer]] by name (case-insensitive):
   *  - "online" = [[OnlineLDAOptimizer]]
   *  - "em" = [[EMLDAOptimizer]]
   * @group setParam
   */
  @Since("1.6.0")
  def setOptimizer(value: String): this.type = value.toLowerCase match {
    case "online" => setOptimizer(new OnlineLDAOptimizer)
    case "em" => setOptimizer(new EMLDAOptimizer)
    case _ => throw new IllegalArgumentException(
      s"LDA was given unknown optimizer '$value'.  Supported values: em, online")
  }

  /** @group setParam */
  @Since("1.6.0")
  def setTopicDistributionCol(value: String): this.type = set(topicDistributionCol, value)

  @Since("1.6.0")
  override def copy(extra: ParamMap): LDA = defaultCopy(extra)

  @Since("1.6.0")
  override def fit(dataset: DataFrame): LDAModel = {
    transformSchema(dataset.schema, logging = true)
    val oldLDA = new OldLDA()
      .setK($(k))
      .setDocConcentration(Vectors.dense($(docConcentration)))
      .setTopicConcentration($(topicConcentration))
      .setMaxIterations($(maxIter))
      .setSeed($(seed))
      .setCheckpointInterval($(checkpointInterval))
      .setOptimizer($(optimizer).getOldOptimizer)
    // TODO: persist here, or in old LDA?
    val oldData = LDA.getOldDataset(dataset, $(featuresCol))
    val oldModel = oldLDA.run(oldData)
    val newModel = oldModel match {
      case m: OldLocalLDAModel =>
        new LDAModel(uid, m.vocabSize, Some(m), dataset.sqlContext)
      case m: OldDistributedLDAModel =>
        new DistributedLDAModel(uid, m.vocabSize, m, dataset.sqlContext)
    }
    copyValues(newModel).setParent(this)
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


private[clustering] object LDA {

  /** Get dataset for spark.mllib LDA */
  def getOldDataset(dataset: DataFrame, featuresCol: String): RDD[(Long, Vector)] = {
    dataset
      .withColumn("docId", monotonicallyIncreasingId())
      .select("docId", featuresCol)
      .map { case Row(docId: Long, features: Vector) =>
        (docId, features)
      }
  }
}


/**
 * :: Experimental ::
 *
 * Abstraction for specifying the [[LDA.optimizer]] Param.
 */
@Since("1.6.0")
@Experimental
sealed abstract class LDAOptimizer extends Params {
  private[clustering] def getOldOptimizer: OldLDAOptimizer
}


/**
 * :: Experimental ::
 *
 * Expectation-Maximization (EM) [[LDA.optimizer]].
 * This class may be used for specifying optimizer-specific Params.
 */
@Since("1.6.0")
@Experimental
class EMLDAOptimizer @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends LDAOptimizer {

  @Since("1.6.0")
  override def copy(extra: ParamMap): EMLDAOptimizer = defaultCopy(extra)

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("EMLDAOpt"))

  private[clustering] override def getOldOptimizer: OldEMLDAOptimizer = {
    new OldEMLDAOptimizer
  }
}


/**
 * :: Experimental ::
 *
 * Online Variational Bayes [[LDA.optimizer]].
 * This class may be used for specifying optimizer-specific Params.
 *
 * For details, see the original Online LDA paper:
 *   Hoffman, Blei and Bach.  "Online Learning for Latent Dirichlet Allocation."
 *   Neural Information Processing Systems, 2010.
 *   [[http://www.cs.columbia.edu/~blei/papers/HoffmanBleiBach2010b.pdf]]
 */
@Since("1.6.0")
@Experimental
class OnlineLDAOptimizer @Since("1.6.0") (
    @Since("1.6.0") override val uid: String) extends LDAOptimizer {

  @Since("1.6.0")
  override def copy(extra: ParamMap): OnlineLDAOptimizer = defaultCopy(extra)

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("OnlineLDAOpt"))

  private[clustering] override def getOldOptimizer: OldOnlineLDAOptimizer = {
    new OldOnlineLDAOptimizer()
      .setTau0($(tau0))
      .setKappa($(kappa))
      .setMiniBatchFraction($(subsamplingRate))
      .setOptimizeDocConcentration($(optimizeDocConcentration))
  }

  /**
   * A (positive) learning parameter that downweights early iterations. Larger values make early
   * iterations count less.
   * Default: 1024, following the Online LDA paper (Hoffman et al., 2010).
   * @group param
   */
  @Since("1.6.0")
  final val tau0 = new DoubleParam(this, "tau0", "A (positive) learning parameter that" +
    " downweights early iterations. Larger values make early iterations count less.",
    ParamValidators.gt(0))

  setDefault(tau0 -> 1024)

  /** @group getParam */
  @Since("1.6.0")
  def getTau0: Double = $(tau0)

  /** @group setParam */
  @Since("1.6.0")
  def setTau0(value: Double): this.type = set(tau0, value)

  /**
   * Learning rate, set as an exponential decay rate.
   * This should be between (0.5, 1.0] to guarantee asymptotic convergence.
   * Default: 0.51, based on the Online LDA paper (Hoffman et al., 2010).
   * @group param
   */
  @Since("1.6.0")
  final val kappa = new DoubleParam(this, "kappa", "Learning rate, set as an exponential decay" +
    " rate. This should be between (0.5, 1.0] to guarantee asymptotic convergence.",
    ParamValidators.gt(0))

  setDefault(kappa -> 0.51)

  /** @group setParam */
  @Since("1.6.0")
  def setKappa(value: Double): this.type = set(kappa, value)

  /** @group getParam */
  @Since("1.6.0")
  def getKappa: Double = $(kappa)

  /**
   * Fraction of the corpus to be sampled and used in each iteration of mini-batch gradient descent,
   * in range (0, 1].
   *
   * Note that this should be adjusted in synch with [[LDA.maxIter]]
   * so the entire corpus is used.  Specifically, set both so that
   * maxIterations * miniBatchFraction >= 1.
   *
   * Note: This is the same as the `miniBatchFraction` parameter in
   *       [[org.apache.spark.mllib.clustering.OnlineLDAOptimizer]].
   *
   * Default: 0.05, i.e., 5% of total documents.
   * @group param
   */
  @Since("1.6.0")
  final val subsamplingRate = new DoubleParam(this, "subsamplingRate", "Fraction of the corpus" +
    " to be sampled and used in each iteration of mini-batch gradient descent, in range (0, 1].",
    ParamValidators.inRange(0.0, 1.0, lowerInclusive = false, upperInclusive = true))

  setDefault(subsamplingRate -> 0.05)

  /** @group getParam */
  @Since("1.6.0")
  def getSubsamplingRate: Double = $(subsamplingRate)

  /** @group setParam */
  @Since("1.6.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /**
   * Indicates whether the docConcentration (Dirichlet parameter for
   * document-topic distribution) will be optimized during training.
   * Setting this to true will make the model more expressive and fit the training data better.
   * Default: false
   * @group param
   */
  @Since("1.6.0")
  final val optimizeDocConcentration = new BooleanParam(this, "optimizeDocConcentration",
    "Indicates whether the docConcentration (Dirichlet parameter for document-topic" +
      " distribution) will be optimized during training.")

  setDefault(optimizeDocConcentration -> true)

  /** @group getParam */
  @Since("1.6.0")
  def getOptimizeDocConcentration: Boolean = $(optimizeDocConcentration)

  /** @group setParam */
  @Since("1.6.0")
  def setOptimizeDocConcentration(value: Boolean): this.type = set(optimizeDocConcentration, value)
}
