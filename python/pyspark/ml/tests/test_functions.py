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

import numpy as np
import pandas as pd
import unittest

from pyspark.ml.functions import batch_infer_udf
from pyspark.sql.functions import struct
from pyspark.sql.types import *
from pyspark.testing.mlutils import SparkSessionTestCase


class BatchInferUDFTests(SparkSessionTestCase):
    def setUp(self):
        super(BatchInferUDFTests, self).setUp()
        self.data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)

        # 4 scalar columns
        self.pdf = pd.DataFrame(self.data, columns=['a', 'b', 'c', 'd'])
        self.df = self.spark.createDataFrame(self.pdf)

        # 1 tensor column of 4 doubles
        self.pdf_tensor = pd.DataFrame()
        self.pdf_tensor['t1'] = self.pdf.values.tolist()
        self.df_tensor1 = self.spark.createDataFrame(self.pdf_tensor)

        # 2 tensor columns of 4 doubles and 3 doubles
        self.pdf_tensor['t2'] = self.pdf.drop(columns='d').values.tolist()
        self.df_tensor2 = self.spark.createDataFrame(self.pdf_tensor)

    def test_identity(self):
        def predict_batch_fn():
            def predict(inputs):
                return inputs
            return predict

        # single column input => single column output
        identity = batch_infer_udf(predict_batch_fn, return_type=DoubleType())
        preds = self.df.withColumn("preds", identity(struct('a'))).toPandas()
        self.assertTrue(preds['a'].equals(preds['preds']))

        # multiple column input => multiple column output
        identity = batch_infer_udf(predict_batch_fn,
                                   return_type=StructType([
                                       StructField('a1', DoubleType(), True),
                                       StructField('b1', DoubleType(), True)
                                   ]))
        preds = self.df.withColumn("preds", identity(struct('a', 'b'))) \
                    .select("a", "b", "preds.*") \
                    .toPandas()
        self.assertTrue(preds['a'].equals(preds['a1']))
        self.assertTrue(preds['b'].equals(preds['b1']))

    def test_batching(self):
        batch_size = 10

        def predict_batch_fn():
            def predict(inputs):
                batch_size = len(inputs)
                # just return the batch size as the "prediction"
                outputs = [batch_size for i in inputs]
                return outputs
            return predict

        identity = batch_infer_udf(predict_batch_fn,
                                   return_type=IntegerType(),
                                   batch_size=batch_size)
        preds = self.df.withColumn("preds", identity(struct('a'))).toPandas()

        batch_sizes = preds['preds'].to_numpy()
        self.assertTrue(all(batch_sizes <= batch_size))

    def test_caching(self):
        def predict_batch_fn():
            # emulate loading a model, this should only be invoked once (per worker process)
            fake_output = np.random.random()

            def predict(inputs):
                return [fake_output for i in inputs]
            return predict

        identity = batch_infer_udf(predict_batch_fn, return_type=DoubleType())

        # results should be the same
        df1 = self.df.withColumn("preds", identity(struct('a'))).toPandas()
        df2 = self.df.withColumn("preds", identity(struct('a'))).toPandas()
        self.assertTrue(df1.equals(df2))

    def test_transform_scalar(self):
        columns = self.df.columns

        # scalar columns with no input_names or input_tensor_shapes => single numpy array
        def array_sum_fn():
            def predict(inputs):
                return np.sum(inputs, axis=1)
            return predict

        sum_cols = batch_infer_udf(array_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5)
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # scalar columns with input_names => dictionary of numpy arrays
        def dict_sum_fn():
            def predict(inputs):
                result = inputs['a'].add(inputs['b']).add(inputs['c']).add(inputs['d'])
                return result
            return predict

        sum_cols = batch_infer_udf(dict_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5,
                                   input_names=columns)
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # scalar columns with non-matching input_names => dictionary of numpy arrays with new names
        def dict_sum_fn():
            def predict(inputs):
                result = inputs['a1'].add(inputs['b1']).add(
                    inputs['c1']).add(inputs['d1'])
                return result
            return predict

        sum_cols = batch_infer_udf(dict_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5,
                                   input_names=['a1', 'b1', 'c1', 'd1'])
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # scalar columns with one tensor_input_shape => single numpy array
        sum_cols = batch_infer_udf(array_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5,
                                   input_tensor_shapes=[[-1, 4]])
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # scalar columns with multiple tensor_input_shapes => ERROR
        sum_cols = batch_infer_udf(array_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5,
                                   input_tensor_shapes=[[-1, 2], [-1, 2]])
        with self.assertRaisesRegex(Exception, 
                                    "Multiple input_tensor_shapes require associated input_names"):
            self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()

    def test_transform_tensor(self):
        columns = self.df_tensor1.columns

        # tensor column with no input_names or input_tensor_shapes => ERROR
        def array_sum_fn():
            def predict(inputs):
                # just return sum of all columns
                return np.sum(inputs, axis=1)
            return predict

        sum_cols = batch_infer_udf(array_sum_fn,
                             return_type=DoubleType(),
                             batch_size=5)
        # TODO: raise better error
        with self.assertRaises(Exception):
            preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns))).toPandas()

        # tensor column with input_name=> ERROR
        def dict_sum_fn():
            def predict(inputs):
                result = np.sum(inputs['dense_input'])
                return result
            return predict

        sum_cols = batch_infer_udf(dict_sum_fn,
                             return_type=DoubleType(),
                             batch_size=5,
                             input_names=['dense_input'])
        # TODO: raise better error
        with self.assertRaises(Exception):
            preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns))).toPandas()

        # tensor column with tensor_input_shape => single numpy array
        sum_cols = batch_infer_udf(array_sum_fn,
                                   return_type=DoubleType(),
                                   batch_size=5,
                                   input_tensor_shapes=[[-1, 4]])
        preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # tensor column with multiple tensor_input_shapes => ERROR
        sum_cols = batch_infer_udf(array_sum_fn,
                             return_type=DoubleType(),
                             batch_size=5,
                             input_tensor_shapes=[[-1,4], [-1,4]])
        with self.assertRaisesRegex(Exception, 
                                    "Multiple input_tensor_shapes require associated input_names"):
            preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns))).toPandas()


if __name__ == "__main__":
    from python.pyspark.ml.tests.test_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
