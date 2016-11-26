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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Dataset

class MinHashSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val data = {
      for (i <- 0 to 95) yield Vectors.sparse(100, (i until i + 5).map((_, 1.0)))
    }
    dataset = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")
  }

  test("params") {
    ParamsSuite.checkParams(new MinHash)
    val model = new MinHashModel("mh", numEntries = 2, randCoefficients = Array(1))
    ParamsSuite.checkParams(model)
  }

  test("MinHash: default params") {
    val rp = new MinHash
    assert(rp.getOutputDim === 1.0)
  }

  test("read/write") {
    def checkModelData(model: MinHashModel, model2: MinHashModel): Unit = {
      assert(model.numEntries === model2.numEntries)
      assertResult(model.randCoefficients)(model2.randCoefficients)
    }
    val mh = new MinHash()
    val settings = Map("inputCol" -> "keys", "outputCol" -> "values")
    testEstimatorAndModelReadWrite(mh, dataset, settings, checkModelData)
  }

  test("hashFunction") {
    val model = new MinHashModel("mh", numEntries = 20, randCoefficients = Array(0, 1, 3))
    val res = model.hashFunction(Vectors.sparse(10, Seq((2, 1.0), (3, 1.0), (5, 1.0), (7, 1.0))))
    assert(res.equals(Vectors.dense(0.0, 3.0, 4.0)))
  }

  test("keyDistance and hashDistance") {
    val model = new MinHashModel("mh", numEntries = 20, randCoefficients = Array(1))
    val v1 = Vectors.sparse(10, Seq((2, 1.0), (3, 1.0), (5, 1.0), (7, 1.0)))
    val v2 = Vectors.sparse(10, Seq((1, 1.0), (3, 1.0), (5, 1.0), (7, 1.0), (9, 1.0)))
    val keyDist = model.keyDistance(v1, v2)
    val hashDist = model.hashDistance(Vectors.dense(-5, 5), Vectors.dense(1, 2))
    assert(keyDist === 0.5)
    assert(hashDist === 3)
  }

  test("MinHash: test of LSH property") {
    val mh = new MinHash()
      .setOutputDim(1)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(12344)

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(dataset, mh, 0.75, 0.5)
    assert(falsePositive < 0.3)
    assert(falseNegative < 0.3)
  }

  test("approxNearestNeighbors for min hash") {
    val mh = new MinHash()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(12345)

    val key: Vector = Vectors.sparse(100,
      (0 until 100).filter(_.toString.contains("1")).map((_, 1.0)))

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(mh, dataset, key, 20,
      singleProbing = true)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxSimilarityJoin for minhash on different dataset") {
    val data1 = {
      for (i <- 0 until 20) yield Vectors.sparse(100, (5 * i until 5 * i + 5).map((_, 1.0)))
    }
    val df1 = spark.createDataFrame(data1.map(Tuple1.apply)).toDF("keys")

    val data2 = {
      for (i <- 0 until 30) yield Vectors.sparse(100, (3 * i until 3 * i + 3).map((_, 1.0)))
    }
    val df2 = spark.createDataFrame(data2.map(Tuple1.apply)).toDF("keys")

    val mh = new MinHash()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")
      .setSeed(12345)

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(mh, df1, df2, 0.5)
    assert(precision == 1.0)
    assert(recall >= 0.7)
  }
}
