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
from pyspark.sql import SQLContext
import numpy as np
from pyspark.mllib.linalg import Vectors
# $example on$
from pyspark.mllib.stat import Statistics
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="StratifiedSamplingExample")  # SparkContext
    sqlContext = SQLContext(sc)

    # $example on$
    # an RDD of any key value pairs
    data = sc.parallelize([(1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')])

    # specify the exact fraction desired from each key as a dictionary
    fractions = {1: 0.1, 2: 0.6, 3: 0.3}

    approxSample = data.sampleByKey(False, fractions)

    # $example off$

    for each in approxSample.collect():
        print(each)

    sc.stop()
