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

package org.apache.spark.sql.hive.execution

import java.io.File
import java.util.Properties

import scala.language.existentials

import antlr.SemanticException
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.{HiveIgnoreKeyTextOutputFormat, RCFileOutputFormat}
import org.apache.hadoop.hive.ql.io.orc.{OrcOutputFormat, OrcSerde}
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.util.{SerializableJobConf, Utils}

case class InsertIntoDir(
    path: String,
    isLocal: Boolean,
    fileFormat: String,
    rowFormat: CatalogStorageFormat,
    child: SparkPlan) extends SaveAsHiveFile {

  @transient private val sessionState = sqlContext.sessionState.asInstanceOf[HiveSessionState]
  def output: Seq[Attribute] = Seq.empty

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val hadoopConf = sessionState.newHadoopConf()

    val properties = new Properties()

    val Array(cols, types) = child.output.foldLeft(Array("", "")) { case (r, a) =>
      r(0) = r(0) + a.name + ","
      r(1) = r(1) + a.dataType.typeName + ":"
      r
    }

    properties.put("columns", cols.dropRight(1))
    properties.put("columns.types", types.dropRight(1))

    val fileFormatMap = Map[String, (Class[_ <: OutputFormat[_, _]], Class[_ <: SerDe])](
      "orc" -> (classOf[OrcOutputFormat], classOf[OrcSerde]),
      "parquet" -> (classOf[MapredParquetOutputFormat], classOf[ParquetHiveSerDe]),
      "rcfile" -> (classOf[RCFileOutputFormat], classOf[LazySimpleSerDe]),
      "textfile" -> (classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]], classOf[LazySimpleSerDe]),
      "sequencefile" -> (classOf[SequenceFileOutputFormat[Any, Any]], classOf[LazySimpleSerDe])
    )

    val (ouputFormatClass, serdeClass) = fileFormatMap.getOrElse(fileFormat.toLowerCase,
      throw new SemanticException(s"Unrecognized file format in STORED AS clause: $fileFormat," +
        s" expected one of ${fileFormatMap.keys.mkString(",")}"))

    properties.put(serdeConstants.SERIALIZATION_LIB, serdeClass.getName)
    import scala.collection.JavaConverters._
    properties.putAll(rowFormat.serdeProperties.asJava)

    // if user specified a serde in the ROW FORMAT, use that.
    rowFormat.serde.map(properties.put(serdeConstants.SERIALIZATION_LIB, _))

    val tableDesc = new TableDesc(
      classOf[TextInputFormat],
      ouputFormatClass,
      properties
    )

    val isCompressed =
      sessionState.conf.getConfString("hive.exec.compress.output", "false").toBoolean

    val targetPath = new Path(path)

    val fileSinkConf = new FileSinkDesc(targetPath.toString, tableDesc, isCompressed)

    val jobConf = new JobConf(hadoopConf)
    jobConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    val jobConfSer = new SerializableJobConf(jobConf)

    val writerContainer = new SparkHiveWriterContainer(
        jobConf,
        fileSinkConf,
        child.output)

    if( !isLocal ) {
      FileSystem.get(jobConf).delete(targetPath, true)
    }

    @transient val outputClass = writerContainer.newSerializer(tableDesc).getSerializedClass
    saveAsHiveFile(child.execute(), outputClass, fileSinkConf, jobConfSer, writerContainer,
      isCompressed)

    val outputPath = FileOutputFormat.getOutputPath(jobConf)
    if( isLocal ) {
      Utils.deleteRecursively(new File(path))
      outputPath.getFileSystem(hadoopConf).copyToLocalFile(true, outputPath, targetPath)
      log.info(s"Copied results from ${outputPath} to local dir ${path}")
    } else {
      log.info(s"Results available at path ${outputPath}")
    }

    Seq.empty[InternalRow]
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult.toArray

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult.asInstanceOf[Seq[InternalRow]], 1)
  }
}
