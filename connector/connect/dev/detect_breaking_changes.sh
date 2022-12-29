#!/bin/bash
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
if [[ $# -gt 1 ]]; then
  echo "Illegal number of parameters."
  echo "Usage: ./connector/connect/dev/generate_protos.sh [path]"
  exit -1
fi


SPARK_HOME="$(cd "`dirname $0`"/../../..; pwd)"
cd "$SPARK_HOME"

pushd connector/connect/common/src/main

buf breaking --against "https://github.com/apache/spark/archive/master.zip#strip_components=1,subdir=connector/connect/common/src/main"
if [[ $? -ne -0 ]]; then
  echo "Buf detected breaking changes for your Pull Request. Please verify."
  echo "Please make sure your branch is current against spark/master."
  exit 1
fi

