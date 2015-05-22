#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row, SQLContext

"""
A simple example demonstrating model selection using CrossValidator.
This example also demonstrates how Pipelines are Estimators.
Run with:

  bin/spark-submit examples/src/main/python/ml/cross_validator.py
"""

if __name__ == "__main__":
    sc = SparkContext(appName="CrossValidatorExample")
    sqlContext = SQLContext(sc)

    # Prepare training documents, which are labeled.
    LabeledDocument = Row("id", "text", "label")
    training = sc.parallelize([(0, "a b c d e spark", 1.0),
                               (1, "b d", 0.0),
                               (2, "spark f g h", 1.0),
                               (3, "hadoop mapreduce", 0.0)]) \
        .map(lambda x: LabeledDocument(*x)).toDF()

    # Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and lr.
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")
    lr = LogisticRegression(maxIter=10, regParam=0.001)
    pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

    # We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    # This will allow us to jointly choose parameters for all Pipeline stages.
    # A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    # We use a ParamGridBuilder to construct a grid of parameters to search over.
    # With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    # this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    paramGrid = ParamGridBuilder() \
        .addGrid(hashingTF.numFeatures, [10, 100, 1000]) \
        .addGrid(lr.regParam, [0.1, 0.01]) \
        .build()

    crossval = CrossValidator(estimator=pipeline,
                              estimatorParamMaps=paramGrid,
                              evaluator=BinaryClassificationEvaluator(),
                              numFolds=2)

    # Run cross-validation, and choose the best set of parameters.
    cvModel = crossval.fit(training)

    # Prepare test documents, which are unlabeled.
    Document = Row("id", "text")
    test = sc.parallelize([(4L, "spark i j k"),
                           (5L, "l m n"),
                           (6L, "mapreduce spark"),
                           (7L, "apache hadoop")]) \
        .map(lambda x: Document(*x)).toDF()

    # Make predictions on test documents. cvModel uses the best model found (lrModel).
    prediction = cvModel.transform(test)
    selected = prediction.select("id", "text", "probability", "prediction")
    for row in selected.collect():
        print(row)

    sc.stop()
