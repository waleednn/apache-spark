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

package org.apache.spark.ml.tree

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.impl.TreeTests
import org.apache.spark.ml.tree.{ContinuousSplit, DecisionTreeModel, LeafNode, Node}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.tree.impurity.GiniCalculator
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.util.collection.OpenHashMap

/**
 * Test suite for [[CodeGenerationDecisionTreeModel]].
 */
class CodeGenerationDecisionTreeModelSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Codegened trees of just a leaf should always return that score
  test("leaf node conversion") {
    val scores = List(-1.0, -0.1, 0.0, 0.1, 1.0, 100.0)
    // impurity ignored by codegen model, so use a junk value
    val imp = new GiniCalculator(Array(1.0, 5.0, 1.0))
    val nodes = scores.map(new LeafNode(_, imp.calculate, imp))
    val predictors = nodes.map(CodeGenerationDecisionTreeModel.getScorer(_))
    val input = Vectors.dense(0)
    val results = predictors.map(_(input))
    (scores zip results) foreach {
      case (e, v) =>
        assert(e ~== v absTol 1E-5)
    }
  }

  test("basic tree conversion") {
    /* Tree structure borrowed from RandomForestSuite */
    /* Build tree for testing, with this structure:
          grandParent
      left2       parent
                left  right
     */
    val leftImp = new GiniCalculator(Array(3.0, 2.0, 1.0))
    val left = new LeafNode(0.0, leftImp.calculate(), leftImp)

    val rightImp = new GiniCalculator(Array(1.0, 2.0, 5.0))
    val right = new LeafNode(2.0, rightImp.calculate(), rightImp)

    val parent = TreeTests.buildParentNode(left, right, new ContinuousSplit(0, 0.5))
    val parentImp = parent.impurityStats

    val left2Imp = new GiniCalculator(Array(1.0, 6.0, 1.0))
    val left2 = new LeafNode(0.1, left2Imp.calculate(), left2Imp)

    val grandParent = TreeTests.buildParentNode(left2, parent, new ContinuousSplit(1, 1.0))


    val vectorExpectations = List(
      (Vectors.dense(0.0, 0.9), 0.1), // left2
      (Vectors.dense(0.4, 1.2), 0.0), // left
      (Vectors.dense(0.5, 1.2), 2.0) // right
    )

    val predictor = CodeGenerationDecisionTreeModel.getScorer(grandParent)
    vectorExpectations.foreach{ case (v, e) =>
      val r = predictor(v)
      assert(e ~== r absTol 1E-5)
    }
  }
}
