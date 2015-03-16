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

# DO NOT MODIFY. The code is generated by _gen_shared_params.py.

from pyspark.ml.param import Param, Params


class HasMaxIter(Params):
    """
    Params with maxIter.
    """

    # a placeholder to make it appear in the generated doc
    maxIter = Param(Params._dummy(), "maxIter", "max number of iterations", 100)

    def __init__(self):
        super(HasMaxIter, self).__init__()
        #: param for max number of iterations
        self.maxIter = Param(self, "maxIter", "max number of iterations", 100)

    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        self.paramMap[self.maxIter] = value
        return self

    def getMaxIter(self):
        """
        Gets the value of maxIter or its default value.
        """
        if self.maxIter in self.paramMap:
            return self.paramMap[self.maxIter]
        else:
            return self.maxIter.defaultValue


class HasRegParam(Params):
    """
    Params with regParam.
    """

    # a placeholder to make it appear in the generated doc
    regParam = Param(Params._dummy(), "regParam", "regularization constant", 0.1)

    def __init__(self):
        super(HasRegParam, self).__init__()
        #: param for regularization constant
        self.regParam = Param(self, "regParam", "regularization constant", 0.1)

    def setRegParam(self, value):
        """
        Sets the value of :py:attr:`regParam`.
        """
        self.paramMap[self.regParam] = value
        return self

    def getRegParam(self):
        """
        Gets the value of regParam or its default value.
        """
        if self.regParam in self.paramMap:
            return self.paramMap[self.regParam]
        else:
            return self.regParam.defaultValue


class HasFeaturesCol(Params):
    """
    Params with featuresCol.
    """

    # a placeholder to make it appear in the generated doc
    featuresCol = Param(Params._dummy(), "featuresCol", "features column name", 'features')

    def __init__(self):
        super(HasFeaturesCol, self).__init__()
        #: param for features column name
        self.featuresCol = Param(self, "featuresCol", "features column name", 'features')

    def setFeaturesCol(self, value):
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        self.paramMap[self.featuresCol] = value
        return self

    def getFeaturesCol(self):
        """
        Gets the value of featuresCol or its default value.
        """
        if self.featuresCol in self.paramMap:
            return self.paramMap[self.featuresCol]
        else:
            return self.featuresCol.defaultValue


class HasLabelCol(Params):
    """
    Params with labelCol.
    """

    # a placeholder to make it appear in the generated doc
    labelCol = Param(Params._dummy(), "labelCol", "label column name", 'label')

    def __init__(self):
        super(HasLabelCol, self).__init__()
        #: param for label column name
        self.labelCol = Param(self, "labelCol", "label column name", 'label')

    def setLabelCol(self, value):
        """
        Sets the value of :py:attr:`labelCol`.
        """
        self.paramMap[self.labelCol] = value
        return self

    def getLabelCol(self):
        """
        Gets the value of labelCol or its default value.
        """
        if self.labelCol in self.paramMap:
            return self.paramMap[self.labelCol]
        else:
            return self.labelCol.defaultValue


class HasPredictionCol(Params):
    """
    Params with predictionCol.
    """

    # a placeholder to make it appear in the generated doc
    predictionCol = Param(Params._dummy(), "predictionCol", "prediction column name", 'prediction')

    def __init__(self):
        super(HasPredictionCol, self).__init__()
        #: param for prediction column name
        self.predictionCol = Param(self, "predictionCol", "prediction column name", 'prediction')

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        self.paramMap[self.predictionCol] = value
        return self

    def getPredictionCol(self):
        """
        Gets the value of predictionCol or its default value.
        """
        if self.predictionCol in self.paramMap:
            return self.paramMap[self.predictionCol]
        else:
            return self.predictionCol.defaultValue


class HasInputCol(Params):
    """
    Params with inputCol.
    """

    # a placeholder to make it appear in the generated doc
    inputCol = Param(Params._dummy(), "inputCol", "input column name", 'input')

    def __init__(self):
        super(HasInputCol, self).__init__()
        #: param for input column name
        self.inputCol = Param(self, "inputCol", "input column name", 'input')

    def setInputCol(self, value):
        """
        Sets the value of :py:attr:`inputCol`.
        """
        self.paramMap[self.inputCol] = value
        return self

    def getInputCol(self):
        """
        Gets the value of inputCol or its default value.
        """
        if self.inputCol in self.paramMap:
            return self.paramMap[self.inputCol]
        else:
            return self.inputCol.defaultValue


class HasOutputCol(Params):
    """
    Params with outputCol.
    """

    # a placeholder to make it appear in the generated doc
    outputCol = Param(Params._dummy(), "outputCol", "output column name", 'output')

    def __init__(self):
        super(HasOutputCol, self).__init__()
        #: param for output column name
        self.outputCol = Param(self, "outputCol", "output column name", 'output')

    def setOutputCol(self, value):
        """
        Sets the value of :py:attr:`outputCol`.
        """
        self.paramMap[self.outputCol] = value
        return self

    def getOutputCol(self):
        """
        Gets the value of outputCol or its default value.
        """
        if self.outputCol in self.paramMap:
            return self.paramMap[self.outputCol]
        else:
            return self.outputCol.defaultValue


class HasNumFeatures(Params):
    """
    Params with numFeatures.
    """

    # a placeholder to make it appear in the generated doc
    numFeatures = Param(Params._dummy(), "numFeatures", "number of features", 1 << 18)

    def __init__(self):
        super(HasNumFeatures, self).__init__()
        #: param for number of features
        self.numFeatures = Param(self, "numFeatures", "number of features", 1 << 18)

    def setNumFeatures(self, value):
        """
        Sets the value of :py:attr:`numFeatures`.
        """
        self.paramMap[self.numFeatures] = value
        return self

    def getNumFeatures(self):
        """
        Gets the value of numFeatures or its default value.
        """
        if self.numFeatures in self.paramMap:
            return self.paramMap[self.numFeatures]
        else:
            return self.numFeatures.defaultValue
