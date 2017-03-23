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

from pyspark import keyword_only
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *

__all__ = ["FPGrowth", "FPGrowthModel"]


class HasSupport(Params):
    """
    Mixin for param support: [0.0, 1.0].
    """

    minSupport = Param(
        Params._dummy(),
        "minSupport",
        "Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that appears more "
        "than (minSupport * size-of-the-dataset) times will be output",
        typeConverter=TypeConverters.toFloat)

    def setMinSupport(self, value):
        """
        Sets the value of :py:attr:`minSupport`.
        """
        if not (0 <= value <= 1):
            raise ValueError("Support must be in range [0, 1]")
        return self._set(minSupport=value)

    def getMinSupport(self):
        """
        Gets the value of minSupport or its default value.
        """
        return self.getOrDefault(self.minSupport)


class HasConfidence(Params):
    """
    Mixin for param confidence: [0.0, 1.0].
    """

    minConfidence = Param(
        Params._dummy(),
        "minConfidence",
        "Minimal confidence for generating Association Rule. [0.0, 1.0]",
        typeConverter=TypeConverters.toFloat)

    def setMinConfidence(self, value):
        """
        Sets the value of :py:attr:`minConfidence`.
        """
        if not (0 <= value <= 1):
            raise ValueError("Confidence must be in range [0, 1]")
        return self._set(minConfidence=value)

    def getMinConfidence(self):
        """
        Gets the value of minConfidence or its default value.
        """
        return self.getOrDefault(self.minConfidence)


class HasItemsCol(Params):
    """
    Mixin for param itemsCol: items column name.
    """

    itemsCol = Param(Params._dummy(), "itemsCol",
                     "items column name.", typeConverter=TypeConverters.toString)

    def __init__(self):
        super(HasItemsCol, self).__init__()
        self._setDefault(itemsCol='items')

    def setItemsCol(self, value):
        """
        Sets the value of :py:attr:`itemsCol`.
        """
        return self._set(itemsCol=value)

    def getItemsCol(self):
        """
        Gets the value of itemsCol or its default value.
        """
        return self.getOrDefault(self.itemsCol)


class FPGrowthModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """Model fitted by FPGrowth.

    .. versionadded:: 2.2.0
    """
    @property
    @since("2.2.0")
    def freqItemsets(self):
        """DataFrame with two columns:
        * `items` - Itemset of the same type as the input column.
        * `freq`  - Frequency of the itemset (`LongType`).
        """
        return self._call_java("freqItemsets")

    @property
    @since("2.2.0")
    def associationRules(self):
        """Data with three columns:
        * `antecedent`  - Array of the same type as the input column.
        * `consequent`  - Single element array of the same type as the input column.
        * `confidence`  - Confidence for the rule (`DoubleType`)."""
        return self._call_java("associationRules")


class FPGrowth(JavaEstimator, HasItemsCol, HasPredictionCol,
               HasSupport, HasConfidence, JavaMLWritable, JavaMLReadable):
    """A parallel FP-growth algorithm to mine frequent itemsets

    * Li et al., PFP: Parallel FP-Growth for Query Recommendation [LI2008]_
    * Han et al., Mining frequent patterns without candidate generation [HAN2000]_

    .. [LI2008] http://dx.doi.org/10.1145/1454008.1454027
    .. [HAN2000] http://dx.doi.org/10.1145/335191.335372

     .. note:: Internally `transform` `collects` and `broadcasts` association rules.

    >>> from pyspark.sql.functions import split
    >>> data = (spark.read
    ...     .text("data/mllib/sample_fpgrowth.txt")
    ...     .select(split("value", "\s+").alias("items")))
    >>> data.show(truncate=False)
    +------------------------+
    |items                   |
    +------------------------+
    |[r, z, h, k, p]         |
    |[z, y, x, w, v, u, t, s]|
    |[s, x, o, n, r]         |
    |[x, z, y, m, t, s, q, e]|
    |[z]                     |
    |[x, z, y, r, q, t, p]   |
    +------------------------+
    >>> fp = FPGrowth(minSupport=0.2, minConfidence=0.7)
    >>> fpm = fp.fit(data)
    >>> fpm.freqItemsets.show(5)
    +---------+----+
    |    items|freq|
    +---------+----+
    |      [s]|   3|
    |   [s, x]|   3|
    |[s, x, z]|   2|
    |   [s, z]|   2|
    |      [r]|   3|
    +---------+----+
    only showing top 5 rows
    >>> fpm.associationRules.show(5)
    +----------+----------+----------+
    |antecedent|consequent|confidence|
    +----------+----------+----------+
    |    [t, s]|       [y]|       1.0|
    |    [t, s]|       [x]|       1.0|
    |    [t, s]|       [z]|       1.0|
    |       [p]|       [r]|       1.0|
    |       [p]|       [z]|       1.0|
    +----------+----------+----------+
    only showing top 5 rows
    >>> new_data = spark.createDataFrame([(["t", "s"], )], ["items"])
    >>> sorted(fpm.transform(new_data).first().prediction)
    ['x', 'y', 'z']

    .. versionadded:: 2.2.0
    """
    @keyword_only
    def __init__(self, minSupport=0.3, minConfidence=0.8, itemsCol="items",
                 predictionCol="prediction", numPartitions=None):
        """
        __init__(self, minSupport=0.3, minConfidence=0.8, itemsCol="items", \
                 predictionCol="prediction", numPartitions=None)
        """
        super(FPGrowth, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.fpm.FPGrowth", self.uid)
        self._setDefault(minSupport=0.3, minConfidence=0.8,
                         itemsCol="items", predictionCol="prediction")
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.2.0")
    def setParams(self, minSupport=0.3, minConfidence=0.8, itemsCol="items",
                  predictionCol="prediction", numPartitions=None):
        """
        setParams(self, minSupport=0.3, minConfidence=0.8, itemsCol="items", \
                  predictionCol="prediction", numPartitions=None)
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return FPGrowthModel(java_model)
