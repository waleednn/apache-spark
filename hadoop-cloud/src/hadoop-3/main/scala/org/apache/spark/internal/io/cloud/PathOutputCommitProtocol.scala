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

package org.apache.spark.internal.io.cloud

import java.io.IOException
import java.util.{Date, UUID}

import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.{Path, StreamCapabilities}
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, PathOutputCommitter, PathOutputCommitterFactory}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.io.{FileCommitProtocol, FileNameSpec, SparkHadoopWriterUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.cloud.PathOutputCommitProtocol.THEAD_COUNT_DEFAULT
import org.apache.spark.mapred.SparkHadoopMapRedUtil

/**
 * Spark Commit protocol for Path Output Committers.
 * This committer will work with the `FileOutputCommitter` and subclasses.
 * All implementations *must* be serializable.
 *
 * Rather than ask the `FileOutputFormat` for a committer, it uses the
 * `org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory` factory
 * API to create the committer.
 *
 * In `setupCommitter` the factory is identified and instantiated;
 * this factory then creates the actual committer implementation.
 *
 * Dynamic Partition support will be determined once the committer is
 * instantiated in the setupJob/setupTask methods. If this
 * class was instantiated with `dynamicPartitionOverwrite` set to true,
 * then the instantiated committer should either be an instance of
 * `FileOutputCommitter` or it must implement the `StreamCapabilities`
 * interface and declare that it has the capability
 * `mapreduce.job.committer.dynamic.partitioning`.
 * That feature is available on Hadoop releases with the Intermediate
 * Manifest Committer for GCS and ABFS; it is not declared
 * as supported by the S3A committers where file rename is O(data).
 * If a committer does not declare explicit support for dynamic partition
 * support then the extra set of renames which take place during job commit,
 * after the PathOutputCommitter itself promotes work to the destination
 * directory, may take a large amount of time.
 * @constructor Instantiate.
 * @param jobId                     job
 * @param path                      destination
 * @param dynamicPartitionOverwrite does the caller want support for dynamic
 *                                  partition overwrite?
 */
class PathOutputCommitProtocol(
    private val jobId: String,
    private val path: String,
    private val dynamicPartitionOverwrite: Boolean = false)
  extends FileCommitProtocol with Serializable with Logging {

  import FileCommitProtocol._
  import PathOutputCommitProtocol._

  /** The committer created. */
  @transient private var committer: PathOutputCommitter = _

  require(path != null, "Null destination specified")

  private[cloud] val destination: String = path

  /** The destination path. This is serializable in Hadoop 3. */
  private[cloud] val destPath: Path = new Path(destination)

  private var threadCount = THEAD_COUNT_DEFAULT

  /**
   * Tracks files staged by this task for absolute output paths. These outputs are not managed by
   * the Hadoop OutputCommitter, so we must move these to their final locations on job commit.
   *
   * The mapping is from the temp output path to the final desired output path of the file.
   */
  @transient private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
   * Tracks partitions with default path that have new files written into them by this task,
   * e.g. a=1/b=2. Files under these partitions will be saved into staging directory and moved to
   * destination directory at the end, if `dynamicPartitionOverwrite` is true.
   */
  @transient private var partitionPaths: mutable.Set[String] = null

  /**
   * The staging directory of this write job. Spark uses it to deal with files with absolute output
   * path, or writing data into partitioned directory with dynamicPartitionOverwrite=true.
   */
  @transient private lazy val stagingDir: Path = getStagingDir(path, jobId)

  logTrace(s"Instantiated committer with job ID=$jobId;" +
    s" destination=$destPath;" +
    s" dynamicPartitionOverwrite=$dynamicPartitionOverwrite")


  /**
   * Set up the committer.
   * This creates it by talking directly to the Hadoop factories, instead
   * of the V1 `mapred.FileOutputFormat` methods.
   * @param context task attempt
   * @return the committer to use. This will always be a subclass of
   *         `PathOutputCommitter`.
   */
  protected def setupCommitter(context: TaskAttemptContext): PathOutputCommitter = {
    logTrace(s"Setting up committer for path $destination")
    committer = PathOutputCommitterFactory.createCommitter(destPath, context)

    // Special feature to force out the FileOutputCommitter, so as to guarantee
    // that the binding is working properly.
    val rejectFileOutput = context.getConfiguration
      .getBoolean(REJECT_FILE_OUTPUT, REJECT_FILE_OUTPUT_DEFVAL)
    if (rejectFileOutput && committer.isInstanceOf[FileOutputCommitter]) {
      // the output format returned a file output format committer, which
      // is exactly what we do not want. So switch back to the factory.
      val factory = PathOutputCommitterFactory.getCommitterFactory(
        destPath,
        context.getConfiguration)
      logTrace(s"Using committer factory $factory")
      committer = factory.createOutputCommitter(destPath, context)
    }

    logTrace(s"Using committer ${committer.getClass}")
    logTrace(s"Committer details: $committer")
    if (committer.isInstanceOf[FileOutputCommitter]) {
      require(!rejectFileOutput,
        s"Committer created is the FileOutputCommitter $committer")

      if (committer.isCommitJobRepeatable(context)) {
        // If FileOutputCommitter says its job commit is repeatable, it means
        // it is using the v2 algorithm, which is not safe for task commit
        // failures. Warn
        logTrace(s"Committer $committer may not be tolerant of task commit failures")
      }
    } else {
      // if required other committers need to be checked for dynamic partition
      // compatibility through a StreamCapabilities probe.
      if (dynamicPartitionOverwrite) {
        if (supportsDynamicPartitions) {
          logDebug(
            s"Committer $committer has declared compatibility with dynamic partition overwrite")
        } else {
          logWarning(s"Committer $committer has incomplete support for" +
            " dynamic partition overwrite.")
        }
      }
    }
    committer
  }


  /**
   * Does the instantiated committer support dynamic partitions?
   * @return true if the committer declares itself compatible.
   */
  private def supportsDynamicPartitions = {
    committer.isInstanceOf[FileOutputCommitter] ||
      (committer.isInstanceOf[StreamCapabilities] &&
        committer.asInstanceOf[StreamCapabilities]
          .hasCapability(CAPABILITY_DYNAMIC_PARTITIONING))
  }

  /**
   * Record the directory used so that dynamic partition overwrite
   * knows to delete it.
   * Includes the check that the directory is defined.
   *
   * @param dir directory
   */
  protected def addPartitionedDir(dir: Option[String]): Unit = {
    assert(dir.isDefined,
      "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
    partitionPaths += dir.get
  }

  /**
   * Get an immutable copy of the partition set of a task attempt.
   * Will be None unless/until [[setupTask()]], including the Job instance.
   *
   * @return None if not initiated; an immutable set otherwise.
   */
  private[cloud] def getPartitions: Option[Set[String]] = {
    if (partitionPaths != null) {
      Some(partitionPaths.toSet)
    } else {
      None
    }
  }

  override def newTaskTempFile(
      taskContext: TaskAttemptContext, dir: Option[String],
      ext: String): String = {
    newTaskTempFile(taskContext, dir, FileNameSpec("", ext))
  }

  /**
   * Create a temporary file for a task.
   *
   * @param taskContext task context
   * @param dir         optional subdirectory
   * @param spec        file naming specification
   * @return a path as a string
   */
  override def newTaskTempFile(
      taskContext: TaskAttemptContext,
      dir: Option[String],
      spec: FileNameSpec): String = {

    // if there is dynamic partition overwrite, its directory must
    // be validated and included in the set of partitions.
    if (dynamicPartitionOverwrite) {
      addPartitionedDir(dir)
    }
    val workDir = committer.getWorkPath
    val parent = dir.map {
      d => new Path(workDir, d)
    }.getOrElse(workDir)
    val file = new Path(parent, getFilename(taskContext, spec))
    logTrace(s"Creating task file $file for dir $dir and spec $spec")
    file.toString
  }

  override def newTaskTempFileAbsPath(
      taskContext: TaskAttemptContext,
      absoluteDir: String,
      ext: String): String = {
    newTaskTempFileAbsPath(taskContext, absoluteDir, FileNameSpec("", ext))
  }

  /**
   * Create a temporary file with an absolute path.
   * Note that this is dangerous as the outcome of any job commit failure
   * is undefined, and potentially slow on cloud storage.
   *
   * @param taskContext task context
   * @param absoluteDir final directory
   * @param spec output filename
   * @return a path string
   */
  override def newTaskTempFileAbsPath(
    taskContext: TaskAttemptContext,
    absoluteDir: String,
    spec: FileNameSpec): String = {

    // qualify the path in the same fs as the staging dir.
    // this makes sure they are in the same filesystem
    val fs = stagingDir.getFileSystem(taskContext.getConfiguration)
    val target = fs.makeQualified(new Path(absoluteDir))
    if (dynamicPartitionOverwrite) {
      // safety check to make sure that the destination path
      // is not a parent of the destination -as if so it will
      // be deleted and the job will fail quite dramatically.

      require(!isAncestorOf(target, stagingDir),
        s"cannot not use $target as a destination of work" +
        s" in dynamic partitioned overwrite query writing to $stagingDir")
    }
    val filename = getFilename(taskContext, spec)
    val absOutputPath = new Path(absoluteDir, filename).toString
    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    val tmpOutputPath = new Path(stagingDir,
      UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    val temp = super.newTaskTempFileAbsPath(taskContext, absoluteDir, spec)
    logTrace(s"Creating temporary file $temp for absolute dir $target")
    tmpOutputPath
  }

  protected def getFilename(taskContext: TaskAttemptContext,
      spec: FileNameSpec): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"${spec.prefix}part-$split%05d-$jobId${spec.suffix}"
  }


  override def setupJob(jobContext: JobContext): Unit = {
    // Setup IDs
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    jobContext.getConfiguration.set("mapreduce.job.id", jobId.toString)
    jobContext.getConfiguration
      .set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration
      .set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(
      jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext,
      taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    val (allAbsPathFiles, allPartitionPaths) =
      taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])])
        .unzip
    val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

    val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
    logDebug(s"Committing files staged for absolute locations $filesToMove")
    val absParentPaths = filesToMove.values.map(new Path(_).getParent).toSet
    if (dynamicPartitionOverwrite) {
      logDebug(
        s"Clean up absolute partition directories for overwriting: $absParentPaths")
      absParentPaths.foreach(fs.delete(_, true))
    }
    logDebug(s"Create absolute parent directories: $absParentPaths")
    absParentPaths.foreach(fs.mkdirs)
    for ((src, dst) <- filesToMove) {
      if (!fs.rename(new Path(src), new Path(dst))) {
        throw new IOException(
          s"Failed to rename $src to $dst when committing files staged for " +
            s"absolute locations")
      }
    }

    if (dynamicPartitionOverwrite) {
      val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
      logDebug(
        s"Clean up default partition directories for overwriting: $partitionPaths")
      for (part <- partitionPaths) {
        val finalPartPath = new Path(path, part)
        if (!fs.delete(finalPartPath, true) &&
          !fs.exists(finalPartPath.getParent)) {
          // According to the official hadoop FileSystem API spec, delete op should assume
          // the destination is no longer present regardless of return value, thus we do not
          // need to double check if finalPartPath exists before rename.
          // Also in our case, based on the spec, delete returns false only when finalPartPath
          // does not exist. When this happens, we need to take action if parent of finalPartPath
          // also does not exist(e.g. the scenario described on SPARK-23815), because
          // FileSystem API spec on rename op says the rename dest(finalPartPath) must have
          // a parent that exists, otherwise we may get unexpected result on the rename.
          fs.mkdirs(finalPartPath.getParent)
        }
        val stagingPartPath = new Path(stagingDir, part)
        if (!fs.rename(stagingPartPath, finalPartPath)) {
          throw new IOException(
            s"Failed to rename $stagingPartPath to $finalPartPath when " +
              s"committing files staged for overwriting dynamic partitions")
        }
      }
    }

    fs.delete(stagingDir, true)
  }

  /**
   * Abort the job; log and ignore any IO exception thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param jobContext job context
   */
  override def abortJob(jobContext: JobContext): Unit = {
    try {
      committer.abortJob(jobContext, JobStatus.State.FAILED)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
    try {
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
      fs.delete(stagingDir, true)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${jobContext.getJobID}", e)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
  }

  override def commitTask(
      taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    logTrace(s"Commit task ${attemptId}")
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId,
      attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
  }

  /**
   * Abort the task; log and ignore any failure thrown.
   * This is invariably invoked in an exception handler; raising
   * an exception here will lose the root cause of the failure.
   *
   * @param taskContext context
   */
  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    try {
      committer.abortTask(taskContext)
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}",
          e)
    }
    // best effort cleanup of other staged files
    try {
      for ((src, _) <- addedAbsPathFiles) {
        val tmp = new Path(src)
        tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
      }
    } catch {
      case e: IOException =>
        logWarning(s"Exception while aborting ${taskContext.getTaskAttemptID}",
          e)
    }
  }

  /**
   * Payload of the task commit message
   * @param addedAbsPathFiles map of staging to absolute files
   * @param partitionPaths set of partition directories written to in dynamic overwrite
   * @param iostatistics any IOStatistics collected.
   */
  private[cloud] class TaskCommitInfo(
      addedAbsPathFiles: Map[String, String],
      partitionPaths: Set[String],
      iostatistics: IOStatisticsSnapshot) extends Serializable

}



object PathOutputCommitProtocol {

  /**
   * Hadoop configuration option.
   * Fail fast if the committer is using the path output protocol.
   * This option can be used to catch configuration issues early.
   *
   * It's mostly relevant when testing/diagnostics, as it can be used to
   * enforce that schema-specific options are triggering a switch
   * to a new committer.
   */
  val REJECT_FILE_OUTPUT = "pathoutputcommit.reject.fileoutput"
  val THREAD_COUNT = "pathoutputcommit.reject.fileoutput"
  val THEAD_COUNT_DEFAULT = 8

  /**
   * Default behavior: accept the file output.
   */
  val REJECT_FILE_OUTPUT_DEFVAL = false

  /**
   * Stream Capabilities probe for spark dynamic partitioning compatibility.
   */
  private[cloud] val CAPABILITY_DYNAMIC_PARTITIONING =
    "mapreduce.job.committer.dynamic.partitioning"

  /**
   * Scheme prefix for per-filesystem scheme committers.
   */
  private[cloud] val OUTPUTCOMMITTER_FACTORY_SCHEME = "mapreduce.outputcommitter.factory.scheme"

  /**
   * Is one path equal to or ancestor of another?
   * @param parent parent path; may be root.
   * @param child path which is to be tested
   * @return true if the paths are the same or parent is above child
   */
  private[cloud] def isAncestorOf(parent: Path, child: Path): Boolean = {
    if (parent == child) {
      true
    } else if (child.isRoot) {
      false
    } else {
      isAncestorOf(parent, child.getParent)
    }
  }
}
