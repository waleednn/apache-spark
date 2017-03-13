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

package org.apache.spark.ml.regression;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.LogisticRegressionSuite;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.tree.impl.TreeTests;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.Utils;

public class JavaDecisionTreeRegressorSuite extends SharedSparkSession {

  @Test
  public void runDT() throws IOException {
    int nPoints = 20;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> data = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    Map<Integer, Integer> categoricalFeatures = new HashMap<>();
    Dataset<Row> dataFrame = TreeTests.setMetadata(data, categoricalFeatures, 0);

    // This tests setters. Training with various options is tested in Scala.
    DecisionTreeRegressor dt = new DecisionTreeRegressor()
      .setMaxDepth(2)
      .setMaxBins(10)
      .setMinInstancesPerNode(5)
      .setMinInfoGain(0.0)
      .setMaxMemoryInMB(256)
      .setCacheNodeIds(false)
      .setCheckpointInterval(10)
      .setMaxDepth(2); // duplicate setMaxDepth to check builder pattern
    for (String impurity : DecisionTreeRegressor.supportedImpurities()) {
      dt.setImpurity(impurity);
    }
    DecisionTreeRegressionModel model = dt.fit(dataFrame);

    model.transform(dataFrame);
    model.numNodes();
    model.depth();
    model.toDebugString();

    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model.write().overwrite().save(path);
      DecisionTreeRegressionModel sameModel = DecisionTreeRegressionModel.load(path);
      TreeTests.checkEqual(model, sameModel);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
