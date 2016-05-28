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

package org.apache.spark.ml.classification;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.spark.SharedSparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.tree.impl.TreeTests;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.Utils;

public class JavaRandomForestClassifierSuite extends SharedSparkSession {

  @Test
  public void runDT() throws IOException {
    int nPoints = 20;
    double A = 2.0;
    double B = -1.5;

    JavaRDD<LabeledPoint> data = jsc.parallelize(
      LogisticRegressionSuite.generateLogisticInputAsList(A, B, nPoints, 42), 2).cache();
    Map<Integer, Integer> categoricalFeatures = new HashMap<>();
    Dataset<Row> dataFrame = TreeTests.setMetadata(data, categoricalFeatures, 2);

    // This tests setters. Training with various options is tested in Scala.
    RandomForestClassifier rf = new RandomForestClassifier()
      .setMaxDepth(2)
      .setMaxBins(10)
      .setMinInstancesPerNode(5)
      .setMinInfoGain(0.0)
      .setMaxMemoryInMB(256)
      .setCacheNodeIds(false)
      .setCheckpointInterval(10)
      .setSubsamplingRate(1.0)
      .setSeed(1234)
      .setNumTrees(3)
      .setMaxDepth(2); // duplicate setMaxDepth to check builder pattern
    for (String impurity : RandomForestClassifier.supportedImpurities()) {
      rf.setImpurity(impurity);
    }
    for (String featureSubsetStrategy : RandomForestClassifier.supportedFeatureSubsetStrategies()) {
      rf.setFeatureSubsetStrategy(featureSubsetStrategy);
    }
    String[] realStrategies = {".1", ".10", "0.10", "0.1", "0.9", "1.0"};
    for (String strategy : realStrategies) {
      rf.setFeatureSubsetStrategy(strategy);
    }
    String[] integerStrategies = {"1", "10", "100", "1000", "10000"};
    for (String strategy : integerStrategies) {
      rf.setFeatureSubsetStrategy(strategy);
    }
    String[] invalidStrategies = {"-.1", "-.10", "-0.10", ".0", "0.0", "1.1", "0"};
    for (String strategy : invalidStrategies) {
      try {
        rf.setFeatureSubsetStrategy(strategy);
        Assert.fail("Expected exception to be thrown for invalid strategies");
      } catch (Exception e) {
        Assert.assertTrue(e instanceof IllegalArgumentException);
      }
    }

    RandomForestClassificationModel model = rf.fit(dataFrame);

    model.transform(dataFrame);
    model.totalNumNodes();
    model.toDebugString();
    model.trees();
    model.treeWeights();
    Vector importances = model.featureImportances();

    File tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark");
    String path = tempDir.toURI().toString();
    try {
      model.save(path);
      RandomForestClassificationModel sameModel =
          RandomForestClassificationModel.load(path);
      TreeTests.checkEqual(model, sameModel);
    } finally {
      Utils.deleteRecursively(tempDir);
    }
  }
}
