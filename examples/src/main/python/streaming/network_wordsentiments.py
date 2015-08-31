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
 Shows the most positive words in UTF8 encoded, '\n' delimited text directly received the network
 every 5 seconds. The streaming data is joined with a static RDD of the word-sentiment file
 provided by http://alexdavies.net/twitter-sentiment-analysis/

 Usage: network_wordcount.py <hostname> <port>
   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive data.

 To run this on your local machine, you need to first run a Netcat server
    `$ nc -lk 9999`
 and then run the example
    `$ bin/spark-submit examples/src/main/python/streaming/network_wordsentiments.py localhost 9999`
"""

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def print_happiest_words(rdd):
    top_list = rdd.take(5)
    print("Happiest topics in the last 5 seconds (%d total):" % rdd.count())
    for tuple in top_list:
        print("%s (%d happiness)" % (tuple[1], tuple[0]))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordsentiments.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingNetworkWordSentiments")
    ssc = StreamingContext(sc, 5)

    file_path = "examples/src/main/resources/twitter_sentiment_list.txt"
    word_sentiments = ssc.sparkContext.textFile(file_path) \
        .map(lambda line: tuple(line.split(",")[:-1]))

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))

    word_counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)

    happiest_words = word_counts.transform(lambda rdd: word_sentiments.join(rdd)) \
        .map(lambda (word, tuple): (word, float(tuple[0]) * tuple[1])) \
        .map(lambda (word, happiness): (happiness, word)) \
        .transform(lambda rdd: rdd.sortByKey())

    happiest_words.foreachRDD(print_happiest_words)

    ssc.start()
    ssc.awaitTermination()
