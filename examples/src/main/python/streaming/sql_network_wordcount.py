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

"""
 Use DataFrames and SQL to count words in UTF8 encoded, '\n' delimited text received from the
 network every second.

 Usage: sql_network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/sql_network_wordcount.py localhost 9999`
"""

import os
import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, Row


def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: sql_network_wordcount.py <hostname> <port> "
        exit(-1)
    host, port = sys.argv[1:]
    sc = SparkContext(appName="PythonSqlNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    # Create a socket stream on target ip:port and count the
    # words in input stream of \n delimited text (eg. generated by 'nc')
    lines = ssc.socketTextStream(host, int(port))
    words = lines.flatMap(lambda line: line.split(" "))

    # Convert RDDs of the words DStream to DataFrame and run SQL query
    def process(time, rdd):
        print "========= %s =========" % str(time)

        try:
            # Get the singleton instance of SQLContext
            sqlContext = getSqlContextInstance(rdd.context)

            # Convert RDD[String] to RDD[Row] to DataFrame
            rowRdd = rdd.map(lambda w: Row(word=w))
            wordsDataFrame = sqlContext.createDataFrame(rowRdd)

            # Register as table
            wordsDataFrame.registerTempTable("words")

            # Do word count on table using SQL and print it
            wordCountsDataFrame = \
                sqlContext.sql("select word, count(*) as total from words group by word")
            wordCountsDataFrame.show()
        except:
            pass

    words.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
