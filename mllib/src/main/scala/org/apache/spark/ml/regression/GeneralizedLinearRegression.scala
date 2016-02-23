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

import breeze.stats.distributions.{Gaussian => GD}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.optim._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Params for Generalized Linear Regression.
 */
private[regression] trait GeneralizedLinearRegressionBase extends PredictorParams
  with HasFitIntercept with HasMaxIter with HasTol with HasRegParam with HasWeightCol
  with HasSolver with Logging {

  /**
   * Param for the name of family which is a description of the error distribution
   * to be used in the model.
   * Supported options: "gaussian", "binomial", "poisson" and "gamma".
   * @group param
   */
  @Since("2.0.0")
  final val family: Param[String] = new Param(this, "family",
    "the name of family which is a description of the error distribution to be used in the model",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedFamilyNames.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getFamily: String = $(family)

  /**
   * Param for the name of the model link function.
   * Supported options: "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt".
   * @group param
   */
  @Since("2.0.0")
  final val link: Param[String] = new Param(this, "link", "the name of the model link function",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedLinkNames.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getLink: String = $(link)

  import GeneralizedLinearRegression._
  protected lazy val familyObj = Family.fromName($(family))
  protected lazy val linkObj = if (isDefined(link)) Link.fromName($(link)) else familyObj.defaultLink
  protected lazy val familyAndLink = new FamilyAndLink(familyObj, linkObj)

  @Since("2.0.0")
  override def validateParams(): Unit = {
    if (isDefined(link)) {
      import GeneralizedLinearRegression._
      require(supportedFamilyAndLinkParis.contains(familyObj -> linkObj),
        s"Generalized Linear Regression with ${$(family)} family does not support ${$(link)} " +
          s"link function.")
    }
  }
}

/**
 * :: Experimental ::
 *
 * Fit a Generalized Linear Model ([[https://en.wikipedia.org/wiki/Generalized_linear_model]])
 * specified by giving a symbolic description of the linear predictor and
 * a description of the error distribution.
 */
@Experimental
@Since("2.0.0")
class GeneralizedLinearRegression @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Regressor[Vector, GeneralizedLinearRegression, GeneralizedLinearRegressionModel]
  with GeneralizedLinearRegressionBase with Logging {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("glm"))

  /**
   * Sets the value of param [[family]].
   * @group setParam
   */
  @Since("2.0.0")
  def setFamily(value: String): this.type = set(family, value)

  /**
   * Sets the value of param [[link]].
   * @group setParam
   */
  @Since("2.0.0")
  def setLink(value: String): this.type = set(link, value)

  /**
   * Set if we should fit the intercept.
   * Default is true.
   * @group setParam
   */
  @Since("2.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("2.0.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("2.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Sets [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is empty, so all instances have weight one.
   * @group setParam
   */
  @Since("2.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "")

  /**
   * Set the solver algorithm used for optimization.
   * Currently only support "irls" which is also the default solver.
   * @group setParam
   */
  @Since("2.0.0")
  def setSolver(value: String): this.type = set(solver, value)
  setDefault(solver -> "irls")

  override protected def train(dataset: DataFrame): GeneralizedLinearRegressionModel = {
    val numFeatures = dataset.select(col($(featuresCol))).limit(1).map {
      case Row(features: Vector) => features.size
    }.first()
    if (numFeatures > WeightedLeastSquares.MaxNumFeatures) {
      val msg = "Currently, GeneralizedLinearRegression only supports number of features" +
        s" <= 4096. Found $numFeatures in the input dataset."
      throw new SparkException(msg)
    }

    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] = dataset.select(col($(labelCol)), w, col($(featuresCol)))
      .map { case Row(label: Double, weight: Double, features: Vector) =>
        Instance(label, weight, features)
      }

    if ($(family) == "gaussian" && $(link) == "identity") {
      val optimizer = new WeightedLeastSquares($(fitIntercept), $(regParam),
        standardizeFeatures = true, standardizeLabel = true)
      val wlsModel = optimizer.fit(instances)
      val model = copyValues(
        new GeneralizedLinearRegressionModel(uid, wlsModel.coefficients, wlsModel.intercept)
          .setParent(this))
      return model
    }

    val newInstances = instances.map { instance =>
      val mu = familyObj.initialize(instance.label, instance.weight)
      val eta = familyAndLink.predict(mu)
      Instance(eta, instance.weight, instance.features)
    }

    val initialModel = new WeightedLeastSquares($(fitIntercept), $(regParam),
      standardizeFeatures = true, standardizeLabel = true)
      .fit(newInstances)

    val optimizer = new IterativelyReweightedLeastSquares(initialModel, familyAndLink.reweightFunc,
      $(fitIntercept), $(regParam), $(maxIter), $(tol))

    val irlsModel = optimizer.fit(instances)

    val model = copyValues(
      new GeneralizedLinearRegressionModel(uid, irlsModel.coefficients, irlsModel.intercept)
        .setParent(this))
    model
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegression = defaultCopy(extra)
}

@Since("2.0.0")
private[ml] object GeneralizedLinearRegression {

  /** Set of family and link pairs that GeneralizedLinearRegression supports */
  lazy val supportedFamilyAndLinkParis = Set(
    Gaussian -> Identity, Gaussian -> Log, Gaussian -> Inverse,
    Binomial -> Logit, Binomial -> Probit, Binomial -> CLogLog,
    Poisson -> Log, Poisson -> Identity, Poisson -> Sqrt,
    Gamma -> Inverse, Gamma -> Identity, Gamma -> Log
  )

  /** Set of families that GeneralizedLinearRegression supports */
  lazy val supportedFamilyNames = supportedFamilyAndLinkParis.map(_._1.name)

  /** Set of links that GeneralizedLinearRegression supports */
  lazy val supportedLinkNames = supportedFamilyAndLinkParis.map(_._2.name)

  val epsilon: Double = 1E-16

  /**
   * One wrapper of family and link instance to be used in the model.
   * @param family the family instance
   * @param link the link instance
   */
  private[ml] class FamilyAndLink(val family: Family, var link: Link) extends Serializable {

    /** Weights for IRLS steps. */
    def weights(mu: Double): Double = {
      val x = family.clean(mu)
      1.0 / (math.pow(this.link.deriv(x), 2.0) * family.variance(x))
    }

    /** The adjusted response variable. */
    def adjusted(y: Double, mu: Double, eta: Double): Double = {
      val x = family.clean(mu)
      eta + (y - x) * link.deriv(x)
    }

    /** Linear predictor based on given mu. */
    def predict(mu: Double): Double = link.link(family.clean(mu))

    /** Fitted value based on linear predictor eta. */
    def fitted(eta: Double): Double = family.clean(link.unlink(eta))

    val reweightFunc: (Instance, WeightedLeastSquaresModel) => (Double, Double) = {
      (instance: Instance, model: WeightedLeastSquaresModel) => {
        val eta = model.predict(instance.features)
        val mu = fitted(eta)
        val z = adjusted(instance.label, mu, eta)
        val w = weights(mu) * instance.weight
        (z, w)
      }
    }
  }

  /**
   * A description of the error distribution to be used in the model.
   * @param name the name of the family
   */
  private[ml] abstract class Family(val name: String) extends Serializable {

    /** The default link instance of this family. */
    val defaultLink: Link

    /** Initialize the starting value for mu. */
    def initialize(y: Double, weight: Double): Double

    /** The variance of the endogenous variable's mean, given the value mu. */
    def variance(mu: Double): Double

    /** Trim the fitted value so that it will be in valid range. */
    def clean(mu: Double): Double = mu
  }

  private[ml] object Family {

    /**
     * Gets the [[Family]] object from its name.
     * @param name family name: "gaussian", "binomial", "poisson" or "gamma".
     */
    def fromName(name: String): Family = {
      name match {
        case Gaussian.name => Gaussian
        case Binomial.name => Binomial
        case Poisson.name => Poisson
        case Gamma.name => Gamma
      }
    }
  }

  /**
   * Gaussian exponential family distribution.
   * The default link for the Gaussian family is the identity link.
   */
  private[ml] object Gaussian extends Family("gaussian") {

    val defaultLink: Link = Identity

    override def initialize(y: Double, weight: Double): Double = y

    def variance(mu: Double): Double = 1.0

    override def clean(mu: Double): Double = {
      if (mu.isNegInfinity) {
        Double.MinValue
      } else if (mu.isPosInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
   * Binomial exponential family distribution.
   * The default link for the Binomial family is the logit link.
   */
  private[ml] object Binomial extends Family("binomial") {

    val defaultLink: Link = Logit

    override def initialize(y: Double, weight: Double): Double = {
      val mu = (weight * y + 0.5) / (weight + 1.0)
      require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
        s"should be in range (0, 1), but got $mu")
      mu
    }

    override def variance(mu: Double): Double = mu * (1.0 - mu)

    override def clean(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu > 1.0 - epsilon) {
        1.0 - epsilon
      } else {
        mu
      }
    }
  }

  /**
   * Poisson exponential family distribution.
   * The default link for the Poisson family is the log link.
   */
  private[ml] object Poisson extends Family("poisson") {

    val defaultLink: Link = Log

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Poisson family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu

    override def clean(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
   * Gamma exponential family distribution.
   * The default link for the Gamma family is the inverse link.
   */
  private[ml] object Gamma extends Family("gamma") {

    val defaultLink: Link = Inverse

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Gamma family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = math.pow(mu, 2.0)

    override def clean(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
   * A description of the link function to be used in the model.
   * The link function provides the relationship between the linear predictor
   * and the mean of the distribution function.
   * @param name the name of link function
   */
  private[ml] abstract class Link(val name: String) extends Serializable {

    /** The link function. */
    def link(mu: Double): Double

    /** Derivative of the link function. */
    def deriv(mu: Double): Double

    /** The inverse link function. */
    def unlink(eta: Double): Double
  }

  private[ml] object Link {

    /**
     * Gets the [[Link]] object from its name.
     * @param name link name: "identity", "logit", "log", "inverse", "probit", "cloglog" or "sqrt".
     */
    def fromName(name: String): Link = {
      name match {
        case Identity.name => Identity
        case Logit.name => Logit
        case Log.name => Log
        case Inverse.name => Inverse
        case Probit.name => Probit
        case CLogLog.name => CLogLog
        case Sqrt.name => Sqrt
      }
    }
  }

  private[ml] object Identity extends Link("identity") {

    override def link(mu: Double): Double = mu

    override def deriv(mu: Double): Double = 1.0

    override def unlink(eta: Double): Double = eta
  }

  private[ml] object Logit extends Link("logit") {

    override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

    override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
  }

  private[ml] object Log extends Link("log") {

    override def link(mu: Double): Double = math.log(mu)

    override def deriv(mu: Double): Double = 1.0 / mu

    override def unlink(eta: Double): Double = math.exp(eta)
  }

  private[ml] object Inverse extends Link("inverse") {

    override def link(mu: Double): Double = 1.0 / mu

    override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

    override def unlink(eta: Double): Double = 1.0 / eta
  }

  private[ml] object Probit extends Link("probit") {

    override def link(mu: Double): Double = GD(0.0, 1.0).icdf(mu)

    override def deriv(mu: Double): Double = 1.0 / GD(0.0, 1.0).pdf(GD(0.0, 1.0).icdf(mu))

    override def unlink(eta: Double): Double = GD(0.0, 1.0).cdf(eta)
  }

  private[ml] object CLogLog extends Link("cloglog") {

    override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

    override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
  }

  private[ml] object Sqrt extends Link("sqrt") {

    override def link(mu: Double): Double = math.sqrt(mu)

    override def deriv(mu: Double): Double = 1.0 / (2.0 * math.sqrt(mu))

    override def unlink(eta: Double): Double = math.pow(eta, 2.0)
  }
}

/**
 * :: Experimental ::
 * Model produced by [[GeneralizedLinearRegression]].
 */
@Experimental
@Since("2.0.0")
class GeneralizedLinearRegressionModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val coefficients: Vector,
    @Since("2.0.0") val intercept: Double)
  extends RegressionModel[Vector, GeneralizedLinearRegressionModel]
  with GeneralizedLinearRegressionBase {

  override protected def predict(features: Vector): Double = {
    val eta = BLAS.dot(features, coefficients) + intercept
    familyAndLink.fitted(eta)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegressionModel = {
    copyValues(new GeneralizedLinearRegressionModel(uid, coefficients, intercept), extra)
      .setParent(parent)
  }
}
