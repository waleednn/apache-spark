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

from pyspark.loose_version import LooseVersion
from pyspark.errors import PySparkImportError, PySparkRuntimeError


def require_minimum_pandas_version() -> None:
    """Raise ImportError if minimum version of Pandas is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pandas_version = "1.4.4"

    try:
        import pandas

        have_pandas = True
    except ImportError as error:
        have_pandas = False
        raised_error = error
    if not have_pandas:
        raise PySparkImportError(
            error_class="PACKAGE_NOT_INSTALLED",
            message_parameters={
                "package_name:": "Pandas",
                "minimum_version": str(minimum_pandas_version),
            },
        ) from raised_error
    if LooseVersion(pandas.__version__) < LooseVersion(minimum_pandas_version):
        raise PySparkImportError(
            error_class="UNSUPPORTED_PACKAGE_VERSION",
            message_parameters={
                "package_name:": "Pandas",
                "minimum_version": str(minimum_pandas_version),
                "current_version": str(pandas.__version__),
            },
        )


def require_minimum_pyarrow_version() -> None:
    """Raise ImportError if minimum version of pyarrow is not installed"""
    # TODO(HyukjinKwon): Relocate and deduplicate the version specification.
    minimum_pyarrow_version = "4.0.0"

    import os

    try:
        import pyarrow

        have_arrow = True
    except ImportError as error:
        have_arrow = False
        raised_error = error
    if not have_arrow:
        raise PySparkImportError(
            error_class="PACKAGE_NOT_INSTALLED",
            message_parameters={
                "package_name:": "PyArrow",
                "minimum_version": str(minimum_pyarrow_version),
            },
        ) from raised_error
    if LooseVersion(pyarrow.__version__) < LooseVersion(minimum_pyarrow_version):
        raise PySparkImportError(
            error_class="UNSUPPORTED_PACKAGE_VERSION",
            message_parameters={
                "package_name:": "PyArrow",
                "minimum_version": str(minimum_pyarrow_version),
                "current_version": str(pyarrow.__version__),
            },
        )
    if os.environ.get("ARROW_PRE_0_15_IPC_FORMAT", "0") == "1":
        raise PySparkRuntimeError(
            error_class="ARROW_LEGACY_IPC_FORMAT",
            message_parameters={},
        )
