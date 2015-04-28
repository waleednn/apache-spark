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

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{Row, SQLContext}

class ChiSqSelectorSuite extends FunSuite with MLlibTestSparkContext {
  test("Test Chi-Square selector") {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val data = Seq(
      LabeledPoint(0.0, Vectors.sparse(3, Array((0, 8.0), (1, 7.0)))),
      LabeledPoint(1.0, Vectors.sparse(3, Array((1, 9.0), (2, 6.0)))),
      LabeledPoint(1.0, Vectors.dense(Array(0.0, 9.0, 8.0))),
      LabeledPoint(2.0, Vectors.dense(Array(8.0, 9.0, 5.0)))
    )

    val preFilteredData = Seq(
      Vectors.dense(Array(0.0)),
      Vectors.dense(Array(6.0)),
      Vectors.dense(Array(8.0)),
      Vectors.dense(Array(5.0))
    )

    val df = sc.parallelize(data.map(x => (x.label, x.features)).zip(preFilteredData)
      .map(x => Tuple3(x._1._1, x._1._2, x._2)))
      .toDF("label", "data", "preFilteredData")

    val model = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setInputCol("data")
      .setLabelCol("label")
      .setOutputCol("expected")

    model.fit(df).transform(df).select("expected", "preFilteredData").collect().foreach {
      case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 ~== vec2 absTol 1e-1)
    }
  }
}
