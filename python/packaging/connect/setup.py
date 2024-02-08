#!/usr/bin/env python3

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

import sys
from setuptools import setup
import os
from shutil import copyfile

try:
    exec(open("pyspark/version.py").read())
except IOError:
    print(
        "Failed to load PySpark version file for packaging. You must be in Spark's python dir.",
        file=sys.stderr,
    )
    sys.exit(-1)
VERSION = __version__  # noqa

try:
    # We copy the shell script to be under pyspark/python/pyspark so that the launcher scripts
    # find it where expected. The rest of the files aren't copied because they are accessed
    # using Python imports instead which will be resolved correctly.
    try:
        os.makedirs("pyspark/python/pyspark")
    except OSError:
        # Don't worry if the directory already exists.
        pass
    copyfile("packaging/connect/setup.py", "setup.py")
    copyfile("packaging/connect/setup.cfg", "setup.cfg")

    # If you are changing the versions here, please also change ./python/pyspark/sql/pandas/utils.py
    # For Arrow, you should also check ./pom.xml and ensure there are no breaking changes in the
    # binary format protocol with the Java version, see ARROW_HOME/format/* for specifications.
    # Also don't forget to update python/docs/source/getting_started/install.rst.
    _minimum_pandas_version = "1.4.4"
    _minimum_numpy_version = "1.21"
    _minimum_pyarrow_version = "4.0.0"
    _minimum_grpc_version = "1.59.3"
    _minimum_googleapis_common_protos_version = "1.56.4"

    with open("README.md") as f:
        long_description = f.read()

    connect_packages = [
        "pyspark",
        "pyspark.cloudpickle",
        "pyspark.mllib",
        "pyspark.mllib.linalg",
        "pyspark.mllib.stat",
        "pyspark.ml",
        "pyspark.ml.connect",
        "pyspark.ml.linalg",
        "pyspark.ml.param",
        "pyspark.ml.torch",
        "pyspark.ml.deepspeed",
        "pyspark.sql",
        "pyspark.sql.avro",
        "pyspark.sql.connect",
        "pyspark.sql.connect.avro",
        "pyspark.sql.connect.client",
        "pyspark.sql.connect.functions",
        "pyspark.sql.connect.proto",
        "pyspark.sql.connect.streaming",
        "pyspark.sql.connect.streaming.worker",
        "pyspark.sql.functions",
        "pyspark.sql.pandas",
        "pyspark.sql.protobuf",
        "pyspark.sql.streaming",
        "pyspark.sql.worker",
        "pyspark.streaming",
        "pyspark.pandas",
        "pyspark.pandas.data_type_ops",
        "pyspark.pandas.indexes",
        "pyspark.pandas.missing",
        "pyspark.pandas.plot",
        "pyspark.pandas.spark",
        "pyspark.pandas.typedef",
        "pyspark.pandas.usage_logging",
        "pyspark.testing",
        "pyspark.resource",
        "pyspark.errors",
        "pyspark.errors.exceptions",
    ]

    setup(
        name="pyspark-connect",
        version=VERSION,
        description="Python Spark Connect client for Apache Spark",
        long_description=long_description,
        long_description_content_type="text/markdown",
        author="Spark Developers",
        author_email="dev@spark.apache.org",
        url="https://github.com/apache/spark/tree/master/python",
        packages=connect_packages,
        license="http://www.apache.org/licenses/LICENSE-2.0",
        # Don't forget to update python/docs/source/getting_started/install.rst
        # if you're updating the versions or dependencies.
        install_requires=[
            "pandas>=%s" % _minimum_pandas_version,
            "pyarrow>=%s" % _minimum_pyarrow_version,
            "grpcio>=%s" % _minimum_grpc_version,
            "grpcio-status>=%s" % _minimum_grpc_version,
            "googleapis-common-protos>=%s" % _minimum_googleapis_common_protos_version,
            "numpy>=%s" % _minimum_numpy_version,
        ],
        python_requires=">=3.8",
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "License :: OSI Approved :: Apache Software License",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
            "Programming Language :: Python :: Implementation :: CPython",
            "Programming Language :: Python :: Implementation :: PyPy",
            "Typing :: Typed",
        ],
    )
finally:
    os.remove("setup.py")
    os.remove("setup.cfg")
