/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.mllib

// $example on$
import org.apache.spark.{SparkConf, SparkContext}
// $example off$
import org.apache.spark.sql.SQLContext


object StratifiedSamplingExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("StratifiedSamplingExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // an RDD[(K, V)] of any key value pairs
    val data = sc.parallelize(
      Seq((1, 'a'), (1, 'b'), (2, 'c'), (2, 'd'), (2, 'e'), (3, 'f')))

    // specify the exact fraction desired from each key
    val fractions = Map(1 -> 0.1, 2 -> 0.6, 3 -> 0.3)

    // Get an exact sample from each stratum
    val approxSample = data.sampleByKey(withReplacement = false, fractions)
    val exactSample = data.sampleByKeyExact(withReplacement = false, fractions)

    // $example off$

    approxSample.foreach(println)
    exactSample.foreach(println)

    sc.stop()
  }
}
// scalastyle:on println
