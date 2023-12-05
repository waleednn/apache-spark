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


import unittest
import datetime

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class ResampleOnMixin:
    def test_resample_on(self):
        np.random.seed(77)
        dates = [
            datetime.datetime(2022, 5, 1, 4, 5, 6),
            datetime.datetime(2022, 5, 3),
            datetime.datetime(2022, 5, 3, 23, 59, 59),
            datetime.datetime(2022, 5, 4),
            pd.NaT,
            datetime.datetime(2022, 5, 4, 0, 0, 1),
            datetime.datetime(2022, 5, 11),
        ]
        pdf = pd.DataFrame(
            np.random.rand(len(dates), 3), index=pd.DatetimeIndex(dates), columns=list("ABC")
        )
        pdf["X"] = pd.DatetimeIndex(dates)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.resample("2D", on="X").sum().sort_index(),
            psdf.resample("2D", on=psdf.X).sum().sort_index(),
            almost=True,
        )


class ResampleOnTests(ResampleOnMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.resample.test_on import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
