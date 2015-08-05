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
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

class VectorSlicerSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("params") {
    val slicer = new VectorSlicer
    ParamsSuite.checkParams(slicer)
    assert(slicer.getSelectedIndices.length === 0)
    assert(slicer.getSelectedNames.length === 0)
    withClue("VectorSlicer should not have any features selected by default") {
      intercept[IllegalArgumentException] {
        slicer.validateParams()
      }
    }
  }

  test("feature validity checks") {
    import VectorSlicer._
    assert(validIndices(Array(0, 1, 8, 2)))
    assert(validIndices(Array.empty[Int]))
    assert(!validIndices(Array(-1)))
    assert(!validIndices(Array(1, 2, 1)))

    assert(validNames(Array("a", "b")))
    assert(validNames(Array.empty[String]))
    assert(!validNames(Array("", "b")))
    assert(!validNames(Array("a", "b", "a")))
  }

  test("Test vector slicer") {
    val sqlContext = new SQLContext(sc)

    val data = Array(
      Vectors.sparse(5, Seq((0, -2.0), (1, 2.3))),
      Vectors.dense(-2.0, 2.3, 0.0, 0.0, 1.0),
      Vectors.dense(0.0, 0.0, 0.0, 0.0, 0.0),
      Vectors.dense(0.6, -1.1, -3.0, 4.5, 3.3),
      Vectors.sparse(5, Seq())
    )

    // Expected after selecting indices 1, 4
    val expected = Array(
      Vectors.sparse(2, Seq((0, 2.3))),
      Vectors.dense(2.3, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(-1.1, 3.3),
      Vectors.sparse(2, Seq())
    )

    val attrs = Array(
      NumericAttribute.defaultAttr.withIndex(0).withName("f0"),
      NumericAttribute.defaultAttr.withIndex(1).withName("f1"),
      NumericAttribute.defaultAttr.withIndex(2).withName("f2"),
      NumericAttribute.defaultAttr.withIndex(3).withName("f3"),
      NumericAttribute.defaultAttr.withIndex(4).withName("f4")
    )

    val resultAttrs = Array(
      NumericAttribute.defaultAttr.withIndex(0).withName("f1"),
      NumericAttribute.defaultAttr.withIndex(1).withName("f4")
    )

    val attrGroup = new AttributeGroup("features", attrs.asInstanceOf[Array[Attribute]])
    val resultAttrGroup = new AttributeGroup("expected", resultAttrs.asInstanceOf[Array[Attribute]])

    val rdd = sc.parallelize(data.zip(expected)).map { case (a, b) => Row(a, b) }
    val df = sqlContext.createDataFrame(rdd,
      StructType(Array(attrGroup.toStructField(), resultAttrGroup.toStructField())))

    val vectorSlicer = new VectorSlicer().setInputCol("features").setOutputCol("result")

    def validateResults(df: DataFrame): Unit = {
      df.select("result", "expected").collect().foreach { case Row(vec1: Vector, vec2: Vector) =>
        assert(vec1 === vec2)
      }
      val resultMetadata = AttributeGroup.fromStructField(df.schema("result"))
      val expectedMetadata = AttributeGroup.fromStructField(df.schema("expected"))
      assert(resultMetadata.numAttributes === expectedMetadata.numAttributes)
      resultMetadata.attributes.get.zip(expectedMetadata.attributes.get).foreach { case (a, b) =>
        assert(a === b)
      }
    }

    vectorSlicer.setSelectedIndices(Array(1, 4)).setSelectedNames(Array.empty)
    validateResults(vectorSlicer.transform(df))

    vectorSlicer.setSelectedIndices(Array(1)).setSelectedNames(Array("f4"))
    validateResults(vectorSlicer.transform(df))

    vectorSlicer.setSelectedIndices(Array.empty).setSelectedNames(Array("f1", "f4"))
    validateResults(vectorSlicer.transform(df))
  }
}
