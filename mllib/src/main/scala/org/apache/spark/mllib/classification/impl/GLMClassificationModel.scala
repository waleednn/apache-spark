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

package org.apache.spark.mllib.classification.impl

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.util.Loader
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.JacksonUtils

/**
 * Helper class for import/export of GLM classification models.
 */
private[classification] object GLMClassificationModel {

  object SaveLoadV1_0 {

    def thisFormatVersion: String = "1.0"

    /** Model data for import/export */
    case class Data(weights: Vector, intercept: Double, threshold: Option[Double])

    /**
     * Helper method for saving GLM classification model metadata and data.
     * @param modelClass  String name for model class, to be saved with metadata
     * @param numClasses  Number of classes label can take, to be saved with metadata
     */
    def save(
        sc: SparkContext,
        path: String,
        modelClass: String,
        numFeatures: Int,
        numClasses: Int,
        weights: Vector,
        intercept: Double,
        threshold: Option[Double]): Unit = {
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()

      // Create JSON metadata.
      val metadataNode = JacksonUtils.createObjectNode
      metadataNode.put("class", modelClass)
      metadataNode.put("version", thisFormatVersion)
      metadataNode.put("numFeatures", numFeatures)
      metadataNode.put("numClasses", numClasses)
      val metadata = JacksonUtils.writeValueAsString(metadataNode)
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val data = Data(weights, intercept, threshold)
      spark.createDataFrame(Seq(data)).repartition(1).write.parquet(Loader.dataPath(path))
    }

    /**
     * Helper method for loading GLM classification model data.
     *
     * NOTE: Callers of this method should check numClasses, numFeatures on their own.
     *
     * @param modelClass  String name for model class (used for error messages)
     */
    def loadData(sc: SparkContext, path: String, modelClass: String): Data = {
      val dataPath = Loader.dataPath(path)
      val spark = SparkSession.builder().sparkContext(sc).getOrCreate()
      val dataRDD = spark.read.parquet(dataPath)
      val dataArray = dataRDD.select("weights", "intercept", "threshold").take(1)
      assert(dataArray.length == 1, s"Unable to load $modelClass data from: $dataPath")
      val data = dataArray(0)
      assert(data.size == 3, s"Unable to load $modelClass data from: $dataPath")
      val (weights, intercept) = data match {
        case Row(weights: Vector, intercept: Double, _) =>
          (weights, intercept)
      }
      val threshold = if (data.isNullAt(2)) {
        None
      } else {
        Some(data.getDouble(2))
      }
      Data(weights, intercept, threshold)
    }
  }
}
