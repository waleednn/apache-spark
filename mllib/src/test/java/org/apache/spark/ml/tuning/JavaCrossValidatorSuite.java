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

package org.apache.spark.ml.tuning;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.ml.classification.LogisticRegressionSuite.generateLogisticInputAsList;


public class JavaCrossValidatorSuite extends SharedSparkSession implements Serializable {

  private transient Dataset<Row> dataset;

  @Override
  public void setUp() throws IOException {
    super.setUp();
    List<LabeledPoint> points = generateLogisticInputAsList(1.0, 1.0, 100, 42);
    dataset = spark.createDataFrame(jsc.parallelize(points, 2), LabeledPoint.class);
  }

  @Test
  public void crossValidationWithLogisticRegression() {
    LogisticRegression lr = new LogisticRegression();
    ParamMap[] lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam(), new double[]{0.001, 1000.0})
      .addGrid(lr.maxIter(), new int[]{0, 10})
      .build();
    BinaryClassificationEvaluator eval = new BinaryClassificationEvaluator();
    CrossValidator cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3);
    CrossValidatorModel cvModel = cv.fit(dataset);
    LogisticRegression parent = (LogisticRegression) cvModel.bestModel().parent();
    Assert.assertEquals(0.001, parent.getRegParam(), 0.0);
    Assert.assertEquals(10, parent.getMaxIter());
  }
}
