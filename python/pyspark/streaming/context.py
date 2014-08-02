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

import time

from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer
from pyspark.storagelevel import *
from pyspark.rdd import RDD
from pyspark.context import SparkContext
from pyspark.streaming.dstream import DStream

from py4j.java_collections import ListConverter


class StreamingContext(object):
    """
    Main entry point for Spark Streaming functionality. A StreamingContext represents the
    connection to a Spark cluster, and can be used to create L{DStream}s and
    broadcast variables on that cluster.
    """

    def __init__(self, master=None, appName=None, sparkHome=None, pyFiles=None,
        environment=None, batchSize=1024, serializer=PickleSerializer(), conf=None,
        gateway=None, duration=None):
        """
        Create a new StreamingContext. At least the master and app name and duration
        should be set, either through the named parameters here or through C{conf}.

        @param master: Cluster URL to connect to
               (e.g. mesos://host:port, spark://host:port, local[4]).
        @param appName: A name for your job, to display on the cluster web UI.
        @param sparkHome: Location where Spark is installed on cluster nodes.
        @param pyFiles: Collection of .zip or .py files to send to the cluster
               and add to PYTHONPATH.  These can be paths on the local file
               system or HDFS, HTTP, HTTPS, or FTP URLs.
        @param environment: A dictionary of environment variables to set on
               worker nodes.
        @param batchSize: The number of Python objects represented as a single
               Java object.  Set 1 to disable batching or -1 to use an
               unlimited batch size.
        @param serializer: The serializer for RDDs.
        @param conf: A L{SparkConf} object setting Spark properties.
        @param gateway: Use an existing gateway and JVM, otherwise a new JVM
               will be instatiated.
        @param duration: A L{Duration} Duration for SparkStreaming

        """

        # launch call back server
        if not gateway:
            gateway = launch_gateway()
#        gateway.restart_callback_server()

        # Create the Python Sparkcontext
        self._sc = SparkContext(master=master, appName=appName, sparkHome=sparkHome,
                        pyFiles=pyFiles, environment=environment, batchSize=batchSize,
                        serializer=serializer, conf=conf, gateway=gateway)
        self._jvm = self._sc._jvm
        self._jssc = self._initialize_context(self._sc._jsc, duration._jduration)

    # Initialize StremaingContext in function to allow subclass specific initialization
    def _initialize_context(self, jspark_context, jduration):
        return self._jvm.JavaStreamingContext(jspark_context, jduration)

    def start(self):
        """
        Start the execution of the streams.
        """
        self._jssc.start()

    def awaitTermination(self, timeout=None):
        """
        Wait for the execution to stop.
        """
        if timeout:
            self._jssc.awaitTermination(timeout)
        else:
            self._jssc.awaitTermination()

    # start from simple one. storageLevel is not passed for now.
    def socketTextStream(self, hostname, port):
        """
        Create an input from TCP source hostname:port. Data is received using
        a TCP socket and receive byte is interpreted as UTF8 encoded '\n' delimited
        lines.
        """
        return DStream(self._jssc.socketTextStream(hostname, port), self, UTF8Deserializer())

    def textFileStream(self, directory):
        """
        Create an input stream that monitors a Hadoop-compatible file system
        for new files and reads them as text files. Files must be wrriten to the
        monitored directory by "moving" them from another location within the same
        file system. FIle names starting with . are ignored.
        """
        return DStream(self._jssc.textFileStream(directory), self, UTF8Deserializer())
