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

package org.apache.spark.shuffle.io.plugin

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext

import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.shuffle.api.metadata.ShuffleOutputTracker
import org.apache.spark.util.{ThreadUtils, Utils}

class MockAsyncBackupShuffleDriverComponents(
    localDelegate: ShuffleDriverComponents,
    backupDir: File) extends ShuffleDriverComponents {

  private val backupManager = new MockAsyncShuffleBlockBackupManager(
    backupDir,
    ExecutionContext.fromExecutorService(ThreadUtils.newDaemonSingleThreadExecutor(
      "driver-test-shuffle-backups")))
  private val tracker = new MockAsyncBackupShuffleOutputTracker(
    backupManager, localDelegate.shuffleOutputTracker())

  override def getAddedExecutorSparkConf(): util.Map[String, String] = {
    val delegateConfs = localDelegate.getAddedExecutorSparkConf().asScala
    (delegateConfs ++
      Map[String, String](MockAsyncBackupShuffleDataIO.BACKUP_DIR -> backupDir.getAbsolutePath))
      .asJava
  }

  override def cleanupApplication(): Unit = {
    localDelegate.cleanupApplication()
    if (backupDir != null) {
      Utils.deleteRecursively(backupDir)
    }
  }

  override def shuffleOutputTracker(): ShuffleOutputTracker = tracker
}
