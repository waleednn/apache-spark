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

package org.apache.spark.ml

import scala.annotation.varargs

import org.apache.spark.ml.param.{ParamMap, ParamPair, Params}
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.api.java.JavaSchemaRDD

/**
 * Abstract class for transformers that transform one dataset into another.
 */
abstract class Transformer extends PipelineStage with Params {

  /**
   * Transforms the dataset with optional parameters
   * @param dataset input dataset
   * @param paramPairs optional list of param pairs, overwrite embedded params
   * @return transformed dataset
   */
  @varargs
  def transform(dataset: SchemaRDD, paramPairs: ParamPair[_]*): SchemaRDD = {
    val map = new ParamMap()
    paramPairs.foreach(map.put(_))
    transform(dataset, map)
  }

  /**
   * Transforms the dataset with provided parameter map.
   * @param dataset input dataset
   * @param paramMap parameters
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD

  // Java-friendly versions of transform.

  @varargs
  def transform(dataset: JavaSchemaRDD, paramPairs: ParamPair[_]*): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramPairs: _*).toJavaSchemaRDD
  }

  def transform(dataset: JavaSchemaRDD, paramMap: ParamMap): JavaSchemaRDD = {
    transform(dataset.schemaRDD, paramMap).toJavaSchemaRDD
  }
}
