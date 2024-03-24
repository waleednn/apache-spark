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

import json
import importlib.resources

# Note: Though we call them "error classes" here, the proper name is "error conditions",
#   hence why the name of the JSON file different.
#   For more information, please see: https://issues.apache.org/jira/browse/SPARK-46810
#   This discrepancy will be resolved as part of: https://issues.apache.org/jira/browse/SPARK-47429
# Note: When we drop support for Python 3.8, we should migrate from importlib.resources.read_text()
#   to importlib.resources.files().joinpath().read_text().
#   See: https://docs.python.org/3/library/importlib.resources.html#importlib.resources.open_text
ERROR_CLASSES_JSON = importlib.resources.read_text("pyspark.errors", "error-conditions.json")
ERROR_CLASSES_MAP = json.loads(ERROR_CLASSES_JSON)
