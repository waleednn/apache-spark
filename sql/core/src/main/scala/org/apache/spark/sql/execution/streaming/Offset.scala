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

package org.apache.spark.sql.execution.streaming

/**
 * An offset is a monotonically increasing metric used to track progress in the computation of a
 * stream. Since offsets are retrieved from a [[Source]] by a single thread, we know the global
 * ordering of two [[Offset]] instances.  We do assume that if two offsets are `equal` then no
 * new data has arrived.
 */
abstract class Offset {

  /**
   * A JSON-serialized representation of an Offset that is
   * used for saving offsets to the offset log.
   *
   * @return JSON string encoding
   */
  def json: String
}

/**
 * Used when loading a JSON serialized offset from external storage.
 * We are currently not responsible for converting JSON serialized
 * data into an internal (i.e., object) representation. Sources should
 * define a factory method in their source Offset companion objects
 * that accepts a [[SerializedOffset]] for doing the conversion.
 */
case class SerializedOffset(override val json: String) extends Offset
