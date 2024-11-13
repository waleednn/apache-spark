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

package org.apache.spark.sql.connect

import org.apache.spark.sql
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types.DataType


/**
 * Functions for registering user-defined functions. Use `SparkSession.udf` to access this:
 *
 * {{{
 *   spark.udf
 * }}}
 *
 * @since 3.5.0
 */
class UDFRegistration(session: SparkSession) extends sql.UDFRegistration {
  override protected def register(
      name: String,
      udf: UserDefinedFunction,
      source: String,
      validateParameterCount: Boolean): UserDefinedFunction = {
    val named = udf.withName(name)
    session.registerUdf(UdfToProtoUtils.toProto(named))
    named
  }

  override def registerJava(name: String, className: String, returnDataType: DataType): Unit = {
    throw new UnsupportedOperationException(
      "registerJava is currently not supported in Spark Connect.")
  }

  /** @inheritdoc */
  override def register(
      name: String,
      udaf: UserDefinedAggregateFunction): UserDefinedAggregateFunction =
    throw ConnectClientUnsupportedErrors.registerUdaf()
}
