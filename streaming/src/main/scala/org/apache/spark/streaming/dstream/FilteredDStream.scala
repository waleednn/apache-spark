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

package org.apache.spark.streaming.dstream

import org.apache.spark.streaming.{Duration, Time}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

private[streaming]
class FilteredDStream[T: ClassTag](
    parent: DStream[T],
    filterFunc: T => Boolean
  ) extends DStream[T](parent.ssc) {

  setName("FilteredRDD")

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[T]] = {
    setCallSite
    val rdd: Option[RDD[T]] = parent.getOrCompute(validTime).map(_.filter(filterFunc))
    ssc.sparkContext.setLocalProperty(RDD_NAME, name)
    return rdd
  }
}


