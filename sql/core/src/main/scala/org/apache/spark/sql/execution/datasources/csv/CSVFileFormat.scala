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

package org.apache.spark.sql.execution.datasources.csv

import java.nio.charset.{Charset, StandardCharsets}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce._

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.util.SerializableConfiguration

/**
 * Provides access to CSV data from pure SQL statements.
 */
class CSVFileFormat extends TextBasedFileFormat with DataSourceRegister {

  override def shortName(): String = "csv"

  override def toString: String = "CSV"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[CSVFileFormat]

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    if (files.isEmpty) {
      None
    } else {
      val csvOptions = new CSVOptions(options)
      val paths = files.map(_.getPath.toString)
      val lines: Dataset[String] = readText(sparkSession, csvOptions, paths)
      val caseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
      Some(CSVInferSchema.infer(lines, caseSensitive, csvOptions))
    }
  }

  override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)
    val conf = job.getConfiguration
    val csvOptions = new CSVOptions(options)
    csvOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new CSVOutputWriterFactory(csvOptions)
  }

  override def buildReader(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val csvOptions = new CSVOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val lines = {
        val conf = broadcastedHadoopConf.value.value
        val linesReader = new HadoopFileLinesReader(file, conf)
        Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
        linesReader.map { line =>
          new String(line.getBytes, 0, line.getLength, csvOptions.charset)
        }
      }

      val linesWithoutHeader = if (csvOptions.headerFlag && file.start == 0) {
        UnivocityParser.dropHeaderLine(lines, csvOptions)
      } else {
        lines
      }

      val linesFiltered = UnivocityParser.filterCommentAndEmpty(linesWithoutHeader, csvOptions)
      val parser = new UnivocityParser(dataSchema, requiredSchema, csvOptions)
      linesFiltered.flatMap(parser.parse)
    }
  }

  private def readText(
      sparkSession: SparkSession,
      options: CSVOptions,
      inputPaths: Seq[String]): Dataset[String] = {
    if (Charset.forName(options.charset) == StandardCharsets.UTF_8) {
      sparkSession.baseRelationToDataFrame(
        DataSource.apply(
          sparkSession,
          paths = inputPaths,
          className = classOf[TextFileFormat].getName
        ).resolveRelation(checkFilesExist = false))
        .select("value").as[String](Encoders.STRING)
    } else {
      val charset = options.charset
      val rdd = sparkSession.sparkContext
        .hadoopFile[LongWritable, Text, TextInputFormat](inputPaths.mkString(","))
        .mapPartitions(_.map(pair => new String(pair._2.getBytes, 0, pair._2.getLength, charset)))
      sparkSession.createDataset(rdd)(Encoders.STRING)
    }
  }

  private def verifySchema(schema: StructType): Unit = {
    def verifyType(dataType: DataType): Unit = dataType match {
        case ByteType | ShortType | IntegerType | LongType | FloatType |
             DoubleType | BooleanType | _: DecimalType | TimestampType |
             DateType | StringType =>

        case udt: UserDefinedType[_] => verifyType(udt.sqlType)

        case _ =>
          throw new UnsupportedOperationException(
            s"CSV data source does not support ${dataType.simpleString} data type.")
    }

    schema.foreach(field => verifyType(field.dataType))
  }
}

private[csv] class CSVOutputWriterFactory(params: CSVOptions) extends OutputWriterFactory {
  override def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): OutputWriter = {
    new CsvOutputWriter(path, dataSchema, context, params)
  }

  override def getFileExtension(context: TaskAttemptContext): String = {
    ".csv" + CodecStreams.getCompressionExtension(context)
  }
}

private[csv] class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    params: CSVOptions) extends OutputWriter with Logging {
  private val writer = CodecStreams.createOutputStream(context, new Path(path))
  private val gen = new UnivocityGenerator(dataSchema, writer, params)
  private var printHeader = params.headerFlag

  override def write(row: Row): Unit = throw new UnsupportedOperationException("call writeInternal")

  override protected[sql] def writeInternal(row: InternalRow): Unit = {
    gen.write(row, printHeader)
    printHeader = false
  }

  override def close(): Unit = gen.close()
}
