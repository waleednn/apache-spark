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
import os

from pyspark.sql import SparkSession
from pyspark.sql.tests.test_catalog import CatalogTestsMixin
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class CatalogParityTests(CatalogTestsMixin, ReusedSQLTestCase):
    @classmethod
    def setUpClass(cls):
        super(CatalogParityTests, cls).setUpClass()
        cls._spark = cls.spark  # Assign existing Spark session to run the server
        # Sets the remote address. Now, we create a remote Spark Session.
        # Note that this is only allowed in testing.
        os.environ["SPARK_REMOTE"] = "sc://localhost"
        cls.spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        # TODO(SPARK-41529): Implement stop in RemoteSparkSession.
        #  Stop the regular Spark session (server) too.
        cls.spark = cls._spark
        super(CatalogParityTests, cls).tearDownClass()
        del os.environ["SPARK_REMOTE"]


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_catalog import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
