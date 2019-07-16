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

package org.apache.spark.sql.execution.datasources.v2.jdbc

import java.io.IOException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class JDBCDataWriter extends DataWriter[InternalRow] with Logging{

  @throws[IOException]
  def write(record: InternalRow): Unit = {
    logInfo("***dsv2-flows*** write " )

    val schema = StructType(Seq(
      StructField("name", StringType, true),
      StructField("rollnum", StringType, true),
      StructField("occupation", StringType, true)))

    val ret = record.toSeq(schema)
    logInfo("***dsv2-flows*** write " + ret.mkString(":") )
  }

  @throws[IOException]
  def commit: WriterCommitMessage = {
    logInfo("***dsv2-flows*** commit called " )
    JDBCWriterCommitMessage
  }

  @throws[IOException]
  def abort(): Unit = {
    logInfo("***dsv2-flows*** abort called " )
  }
}

object JDBCWriterCommitMessage extends WriterCommitMessage {
  val commitMessage: String = "committed"
}
