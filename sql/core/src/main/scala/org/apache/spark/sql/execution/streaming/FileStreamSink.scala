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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormat, FileFormatWriter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.SerializableConfiguration

object FileStreamSink extends Logging {
  // The name of the subdirectory that is used to store metadata about which files are valid.
  val metadataDir = "_spark_metadata"

  /**
   * Returns true if there is a single path that has a metadata log indicating which files should
   * be read.
   */
  def hasMetadata(path: Seq[String], hadoopConf: Configuration, sqlConf: SQLConf): Boolean = {
    path match {
      case Seq(singlePath) =>
        val hdfsPath = new Path(singlePath)
        val fs = hdfsPath.getFileSystem(hadoopConf)
        if (fs.isDirectory(hdfsPath)) {
          val metadataPath = new Path(hdfsPath, metadataDir)
          checkEscapedMetadataPath(fs, metadataPath, sqlConf)
          fs.exists(metadataPath)
        } else {
          false
        }
      case _ => false
    }
  }

  def checkEscapedMetadataPath(fs: FileSystem, metadataPath: Path, sqlConf: SQLConf): Unit = {
    if (sqlConf.getConf(SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED)
        && StreamExecution.containsSpecialCharsInPath(metadataPath)) {
      val legacyMetadataPath = new Path(metadataPath.toUri.toString)
      val legacyMetadataPathExists =
        try {
          fs.exists(legacyMetadataPath)
        } catch {
          case NonFatal(e) =>
            // We may not have access to this directory. Don't fail the query if that happens.
            logWarning(e.getMessage, e)
            false
        }
      if (legacyMetadataPathExists) {
        throw new SparkException(s"Found $legacyMetadataPath. In Spark 2.4 and earlier, the " +
          s"file sink metadata is written to $legacyMetadataPath when using $metadataPath as " +
          s"the output path. We detected that $legacyMetadataPath exists but not sure " +
          s"whether we should ignore this directory and start your query using $metadataPath " +
          s"directly. If you would like to recover your query from $legacyMetadataPath, please " +
          s"*move* all files in $legacyMetadataPath to $metadataPath and rerun your codes. If " +
          s"$legacyMetadataPath is not related to the query or you have already moved the files " +
          s"to $metadataPath, you can either set SQL conf " +
          s"'${SQLConf.STREAMING_CHECKPOINT_ESCAPED_PATH_CHECK_ENABLED.key}' to false to turn " +
          s"off this check, or just remove $legacyMetadataPath.")
      }
    }
  }

  /**
   * Returns true if the path is the metadata dir or its ancestor is the metadata dir.
   * E.g.:
   *  - ancestorIsMetadataDirectory(/.../_spark_metadata) => true
   *  - ancestorIsMetadataDirectory(/.../_spark_metadata/0) => true
   *  - ancestorIsMetadataDirectory(/a/b/c) => false
   */
  def ancestorIsMetadataDirectory(path: Path, hadoopConf: Configuration): Boolean = {
    val fs = path.getFileSystem(hadoopConf)
    var currentPath = path.makeQualified(fs.getUri, fs.getWorkingDirectory)
    while (currentPath != null) {
      if (currentPath.getName == FileStreamSink.metadataDir) {
        return true
      } else {
        currentPath = currentPath.getParent
      }
    }
    return false
  }
}

/**
 * A sink that writes out results to parquet files.  Each batch is written out to a unique
 * directory. After all of the files in a batch have been successfully written, the list of
 * file paths is appended to the log atomically. In the case of partial failures, some duplicate
 * data may be present in the target directory, but only one copy of each file will be present
 * in the log.
 */
class FileStreamSink(
    sparkSession: SparkSession,
    path: String,
    fileFormat: FileFormat,
    partitionColumnNames: Seq[String],
    options: Map[String, String]) extends Sink with Logging {

  private val hadoopConf = sparkSession.sessionState.newHadoopConf()
  private val basePath = new Path(path)
  private val logPath = {
    val metadataDir = new Path(basePath, FileStreamSink.metadataDir)
    val fs = metadataDir.getFileSystem(hadoopConf)
    FileStreamSink.checkEscapedMetadataPath(fs, metadataDir, sparkSession.sessionState.conf)
    metadataDir
  }
  private val fileLog =
    new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, logPath.toString)

  private def basicWriteJobStatsTracker: BasicWriteJobStatsTracker = {
    val serializableHadoopConf = new SerializableConfiguration(hadoopConf)
    new BasicWriteJobStatsTracker(serializableHadoopConf, BasicWriteJobStatsTracker.metrics)
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = path)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ =>  // Do nothing
      }

      // Get the actual partition columns as attributes after matching them by name with
      // the given columns names.
      val partitionColumns: Seq[Attribute] = partitionColumnNames.map { col =>
        val nameEquality = data.sparkSession.sessionState.conf.resolver
        data.logicalPlan.output.find(f => nameEquality(f.name, col)).getOrElse {
          throw new RuntimeException(s"Partition column $col not found in schema ${data.schema}")
        }
      }
      val qe = data.queryExecution

      FileFormatWriter.write(
        sparkSession = sparkSession,
        plan = qe.executedPlan,
        fileFormat = fileFormat,
        committer = committer,
        outputSpec = FileFormatWriter.OutputSpec(path, Map.empty, qe.analyzed.output),
        hadoopConf = hadoopConf,
        partitionColumns = partitionColumns,
        bucketSpec = None,
        statsTrackers = Seq(basicWriteJobStatsTracker),
        options = options)
    }
  }

  override def toString: String = s"FileSink[$path]"
}
