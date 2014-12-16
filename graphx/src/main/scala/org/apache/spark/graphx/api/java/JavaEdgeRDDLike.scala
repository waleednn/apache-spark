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
package org.apache.spark.graphx.api.java

import java.lang.{Long => JLong}

import org.apache.spark.api.java.JavaRDDLike
import org.apache.spark.graphx._

trait JavaEdgeRDDLike [ED, VD, This <: JavaEdgeRDDLike[ED, VD, This, R],
R <: JavaRDDLike[Edge[ED], R]]
  extends Serializable {

//  def wrapRDD(edgeRDD: RDD[Edge[ED]]): This

//  def setName() = toRDDEdges.setName("JavaEdgeRDD")
//
//  def collect(): JList[Edge[ED]] = toRDDEdges.collect().toList.asInstanceOf
//
//  def count(): Long = toRDDEdges.count()
}
