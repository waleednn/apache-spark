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

from pyspark.sql.tests.test_datasources import DataSourcesTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class DataSourcesParityTests(DataSourcesTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_csv_sampling_ratio(self):
        super().test_csv_sampling_ratio()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_json_sampling_ratio(self):
        super().test_json_sampling_ratio()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_datasources import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
