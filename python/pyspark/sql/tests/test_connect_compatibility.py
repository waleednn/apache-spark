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
import inspect

from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.sql.classic.dataframe import DataFrame as ClassicDataFrame
from pyspark.sql.classic.column import Column as ClassicColumn
from pyspark.sql.session import SparkSession as ClassicSparkSession
from pyspark.sql.catalog import Catalog as ClassicCatalog

if should_test_connect:
    from pyspark.sql.connect.dataframe import DataFrame as ConnectDataFrame
    from pyspark.sql.connect.column import Column as ConnectColumn
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
    from pyspark.sql.connect.catalog import Catalog as ConnectCatalog


class ConnectCompatibilityTestsMixin:
    def _get_public_methods(self, cls):
        """Get public methods of a class."""
        return {
            name: method
            for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
            if not name.startswith("_")
        }

    def _get_public_properties(self, cls):
        """Get public properties of a class."""
        return {
            name: member
            for name, member in inspect.getmembers(cls)
            if isinstance(member, property) and not name.startswith("_")
        }

    def _compare_method_signatures(self, classic_cls, connect_cls, cls_name):
        """Compare method signatures between classic and connect classes."""
        classic_methods = self._get_public_methods(classic_cls)
        connect_methods = self._get_public_methods(connect_cls)

        common_methods = set(classic_methods.keys()) & set(connect_methods.keys())

        for method in common_methods:
            classic_signature = inspect.signature(classic_methods[method])
            connect_signature = inspect.signature(connect_methods[method])

            if not method == "createDataFrame":
                self.assertEqual(
                    classic_signature,
                    connect_signature,
                    f"Signature mismatch in {cls_name} method '{method}'\n"
                    f"Classic: {classic_signature}\n"
                    f"Connect: {connect_signature}",
                )

    def _compare_property_lists(
        self, classic_cls, connect_cls, cls_name, expected_missing_properties
    ):
        """Compare properties between classic and connect classes."""
        classic_properties = self._get_public_properties(classic_cls)
        connect_properties = self._get_public_properties(connect_cls)

        # Identify missing properties
        classic_only_properties = set(classic_properties.keys()) - set(connect_properties.keys())

        # Compare the actual missing properties with the expected ones
        self.assertEqual(
            classic_only_properties,
            expected_missing_properties,
            f"{cls_name}: Unexpected missing properties in Connect: {classic_only_properties}",
        )

    def _check_missing_methods(self, classic_cls, connect_cls, cls_name, expected_missing_methods):
        """Check for expected missing methods between classic and connect classes."""
        classic_methods = self._get_public_methods(classic_cls)
        connect_methods = self._get_public_methods(connect_cls)

        # Identify missing methods
        classic_only_methods = set(classic_methods.keys()) - set(connect_methods.keys())

        # Compare the actual missing methods with the expected ones
        self.assertEqual(
            classic_only_methods,
            expected_missing_methods,
            f"{cls_name}: Unexpected missing methods in Connect: {classic_only_methods}",
        )

    def check_compatibility(
        self,
        classic_cls,
        connect_cls,
        cls_name,
        expected_missing_properties,
        expected_missing_methods,
    ):
        """
        Main method for checking compatibility between classic and connect.

        This method performs the following checks:
        - API signature comparison between classic and connect classes.
        - Property comparison, identifying any missing properties in the connect class.
        - Method comparison, identifying any missing methods in the connect class.

        Parameters
        ----------
        classic_cls : type
            The classic class to compare.
        connect_cls : type
            The connect class to compare.
        cls_name : str
            The name of the class.
        expected_missing_properties : set
            A set of properties expected to be missing in the connect class.
        expected_missing_methods : set
            A set of methods expected to be missing in the connect class.
        """
        self._compare_method_signatures(classic_cls, connect_cls, cls_name)
        self._compare_property_lists(
            classic_cls, connect_cls, cls_name, expected_missing_properties
        )
        self._check_missing_methods(classic_cls, connect_cls, cls_name, expected_missing_methods)

    def test_dataframe_compatibility(self):
        """Test DataFrame compatibility between classic and connect."""
        expected_missing_properties = {"sql_ctx", "isStreaming"}
        expected_missing_methods = {"inputFiles", "isLocal", "semanticHash", "isEmpty"}
        self.check_compatibility(
            ClassicDataFrame,
            ConnectDataFrame,
            "DataFrame",
            expected_missing_properties,
            expected_missing_methods,
        )

    def test_column_compatibility(self):
        """Test Column compatibility between classic and connect."""
        expected_missing_properties = set()
        expected_missing_methods = set()
        self.check_compatibility(
            ClassicColumn,
            ConnectColumn,
            "Column",
            expected_missing_properties,
            expected_missing_methods,
        )

    def test_spark_session_compatibility(self):
        """Test SparkSession compatibility between classic and connect."""
        expected_missing_properties = {"sparkContext", "version"}
        expected_missing_methods = {"newSession"}
        self.check_compatibility(
            ClassicSparkSession,
            ConnectSparkSession,
            "SparkSession",
            expected_missing_properties,
            expected_missing_methods,
        )

    def test_catalog_compatibility(self):
        """Test Catalog compatibility between classic and connect."""
        expected_missing_properties = set()
        expected_missing_methods = set()
        self.check_compatibility(
            ClassicCatalog,
            ConnectCatalog,
            "Catalog",
            expected_missing_properties,
            expected_missing_methods,
        )


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ConnectCompatibilityTests(ConnectCompatibilityTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_connect_compatibility import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
