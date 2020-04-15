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


class TaskResourceRequest(object):
    """
    .. note:: Evolving

    A task resource request. This is used in conjuntion with the
    :class:`pyspark.resource.ResourceProfile` to programmatically specify the resources
    needed for an RDD that will be applied at the stage level. The amount is specified
    as a Double to allow for saying you want more then 1 task per resource. Valid values
    are less than or equal to 0.5 or whole numbers.
    Use :class:`pyspark.resource.TaskResourceRequests` class as a convenience API.

    :param resourceName: Name of the resource
    :param amount: Amount requesting as a Double to support fractional resource requests.
        Valid values are less than or equal to 0.5 or whole numbers.

    .. versionadded:: 3.1.0
    """
    def __init__(self, resourceName, amount):
        """
        Create a new :class:`pyspark.resource.TaskResourceRequest` that wraps the underlying
        JVM object.
        """
        from pyspark.context import SparkContext
        _jvm = SparkContext._jvm
        if _jvm is not None:
            self._java_task_request = SparkContext._jvm.org.apache.spark.resource.TaskResourceRequest(
                resourceName, float(amount))
        else:
            self._java_task_request = None
            self._name = resourceName
            self._amount = amount

    @property
    def resourceName(self):
        if self._java_task_request is not None:
            return self._java_task_request.resourceName()
        else:
            return self._name

    @property
    def amount(self):
        if self._java_task_request is not None:
            return self._java_task_request.amount()
        else:
            return self._amount
