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

import scala.util.Random

import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.regression.FactorizationMachines._
import org.apache.spark.ml.regression.FMRegressorSuite._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col}

class FMRegressorSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 10
  @transient var crossDataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val (crossDatasetTmp, _) = generateFactorInteractionInput(spark, 2, 10, 1000, seed)
    crossDataset = crossDatasetTmp
  }

  test("params") {
    ParamsSuite.checkParams(new FMRegressor)
    val model = new FMRegressorModel("fmr_test", 0.0, Vectors.dense(0.0),
      new DenseMatrix(1, 8, new Array[Double](8)))
    ParamsSuite.checkParams(model)
  }

  test("factorization machines squaredError") {
    val numFeatures = 10
    val factorSize = 4
    val (data, coefficients) = generateFactorInteractionInput(
      spark, factorSize, numFeatures, 1000, seed)
    val (b, w, v) = splitCoefficients(new DenseVector(coefficients),
      numFeatures, factorSize, true, true)

    val fm = new FMRegressor()
      .setSolver("adamW")
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setFactorSize(factorSize)
      .setInitStd(0.01)
      .setMaxIter(1000)
      .setMiniBatchFraction(1.0)
      .setStepSize(1.0)
      .setRegParam(0.0)
      .setTol(1E-6)
    val fmModel = fm.fit(data)
    val res = fmModel.transform(data)

    // check mse value
    val mse = res.select((col("prediction") - col("label")).as("error"))
      .select((col("error") * col("error")).as("error_square"))
      .agg(avg("error_square"))
      .collect()(0).getAs[Double](0)
    assert(mse ~== 0.0 absTol 1E-6)

    // check coefficients
    assert(b ~== fmModel.bias absTol 1E-4)
    assert(w ~== fmModel.linear absTol 1E-4)
    (0 until numFeatures).foreach { i =>
      ((i + 1) until numFeatures).foreach { j =>
        // assert <v_i, v_j> is same
        var innerProd1 = 0.0
        var innerProd2 = 0.0
        (0 until factorSize).foreach { k =>
          innerProd1 += v(i, k) * v(j, k)
          innerProd2 += fmModel.factors(i, k) * fmModel.factors(j, k)
        }
        assert(innerProd1 ~== innerProd2 absTol 1E-4)
      }
    }
  }

  test("read/write") {
    def checkModelData(
      model: FMRegressorModel,
      model2: FMRegressorModel
    ): Unit = {
      assert(model.bias === model2.bias)
      assert(model.linear.toArray === model2.linear.toArray)
      assert(model.factors.toArray === model2.factors.toArray)
      assert(model.numFeatures === model2.numFeatures)
    }
    val fm = new FMRegressor()
    val data = crossDataset
      .withColumnRenamed("features", allParamSettings("featuresCol").toString)
      .withColumnRenamed("label", allParamSettings("labelCol").toString)
    testEstimatorAndModelReadWrite(fm, data, allParamSettings,
      allParamSettings, checkModelData)
  }
}

object FMRegressorSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "featuresCol" -> "myFeatures",
    "labelCol" -> "myLabel",
    "predictionCol" -> "prediction",
    "factorSize" -> 2,
    "fitBias" -> false,
    "fitLinear" -> false,
    "regParam" -> 0.01,
    "miniBatchFraction" -> 0.1,
    "initStd" -> 0.01,
    "maxIter" -> 2,
    "stepSize" -> 0.1,
    "tol" -> 1e-4,
    "solver" -> "gd"
  )

  def generateFactorInteractionInput(
    spark: SparkSession,
    factorSize: Int,
    numFeatures: Int,
    numSamples: Int,
    seed: Int
  ): (DataFrame, Array[Double]) = {
    import spark.implicits._
    val sc = spark.sparkContext

    val rnd = new Random(seed)
    val coefficientsSize = factorSize * numFeatures + numFeatures + 1
    val coefficients = Array.fill(coefficientsSize)(rnd.nextDouble() - 0.5)
    val (bias, linear, factors) = splitCoefficients(
      Vectors.dense(coefficients), numFeatures, factorSize, true, true)

    val X: DataFrame = sc.parallelize(0 until numSamples).map { i =>
      val x = new DenseVector(Array.fill(numFeatures)(rnd.nextDouble() - 0.5))
      (i, x)
    }.toDF("id", "features")

    val fmModel = new FMRegressorModel(
      "fmr_test", bias, linear, factors)
    fmModel.set(fmModel.factorSize, factorSize)
    fmModel.set(fmModel.fitBias, true)
    fmModel.set(fmModel.fitLinear, true)
    val data = fmModel.transform(X)
      .withColumn("label", col("prediction"))
      .select("features", "label")
    (data, coefficients)
  }
}
