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

# $example on$
from numpy import array
from math import sqrt
# $example off$

from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import BisectingKMeans, BisectingKMeansModel
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="BisectingKMeansExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile("data/mllib/bisecting_kmeans_data.txt")
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

    # Build the model (cluster the data)
    clusters = BisectingKMeans.train(parsedData, 2, maxIterations=5)

    # Evaluate clustering
    cost = clusters.computeCost(parsedData)
    print("Bisecting K-means Cost = " + str(cost))

    # Save and load model
    path = "target/org/apache/spark/PythonBisectingKMeansExample/BisectingKMeansModel"
    clusters.save(sc, path)
    sameModel = BisectingKMeansModel.load(sc, path)
    # $example off$

    sc.stop()
