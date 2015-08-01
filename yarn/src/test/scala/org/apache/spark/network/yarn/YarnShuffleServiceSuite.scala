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
package org.apache.spark.network.yarn

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.shuffle.ShuffleTestAccessor
import org.apache.spark.network.shuffle.protocol.ExecutorShuffleInfo

class YarnShuffleServiceSuite extends SparkFunSuite with Matchers {
  private[yarn] var yarnConfig: YarnConfiguration = new YarnConfiguration

  {
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, "spark_shuffle");
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format("spark_shuffle"),
      "org.apache.spark.network.yarn.YarnShuffleService");

    yarnConfig.get("yarn.nodemanager.local-dirs").split(",").foreach { dir =>
      val d = new File(dir)
      if (d.exists()) {
        FileUtils.deleteDirectory(d)
      }
      FileUtils.forceMkdir(d)
    }
  }

  test("executor state kept across NM restart") {
    val service = new YarnShuffleService
    service.init(yarnConfig)
    val app1Id = ApplicationId.newInstance(0, 1)
    val app1Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app1Id, null)
    service.initializeApplication(app1Data)
    val app2Id = ApplicationId.newInstance(0, 2)
    val app2Data: ApplicationInitializationContext =
      new ApplicationInitializationContext("user", app2Id, null)
    service.initializeApplication(app2Data)

    val execStateFile = service.registeredExecutorFile
    execStateFile should not be (null)
    execStateFile.exists() should be (false)
    val shuffleInfo1 = new ExecutorShuffleInfo(Array("/foo", "/bar"), 3, "sort")
    val shuffleInfo2 = new ExecutorShuffleInfo(Array("/bippy"), 5, "hash")


    val blockHandler = service.blockHandler
    val blockResolver = ShuffleTestAccessor.getBlockResolver(blockHandler)
    ShuffleTestAccessor.registeredExecutorFile(blockResolver) should be (execStateFile)

    blockResolver.registerExecutor(app1Id.toString, "exec-1", shuffleInfo1)
    blockResolver.registerExecutor(app2Id.toString, "exec-2", shuffleInfo2)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", blockResolver) should
      be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", blockResolver) should
      be (Some(shuffleInfo2))

    execStateFile.exists() should be (true)

    // now we pretend the shuffle service goes down, and comes back up
    service.stop()

    val s2 = new YarnShuffleService
    s2.init(yarnConfig)
    s2.registeredExecutorFile should be (execStateFile)

    val handler2 = s2.blockHandler
    val resolver2 = ShuffleTestAccessor.getBlockResolver(handler2)

    // until we initialize the application, don't know about any executors
    // that is so that if the application gets removed while the NM was down, it still eventually
    // gets purged from our list of apps.
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (None)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    // now we reinitialize only one of the apps (as if app2 was stopped during the NM restart)
    s2.initializeApplication(app1Data)
    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver2) should be (Some(shuffleInfo1))


    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver2) should be (None)

    // Act like the NM restarts one more time

    s2.stop()

    val s3 = new YarnShuffleService
    s3.init(yarnConfig)
    s3.registeredExecutorFile should be (execStateFile)

    // the second app won't even be in our file of saved executor info
    // (this is mostly an implementation detail, by itself its not really an important check ...)
    s3.recoveredExecutorRegistrations.get(app1Id.toString) should not be (null)
    s3.recoveredExecutorRegistrations.get(app2Id.toString) should be (null)

    val handler3 = s3.blockHandler
    val resolver3 = ShuffleTestAccessor.getBlockResolver(handler3)

    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver3) should be (None)
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (None)

    // now if we initialize both those apps, we'll have restored executor info for app 1,
    // but for app 2, there won't anything to restore
    s3.initializeApplication(app1Data)
    s3.initializeApplication(app2Data)

    ShuffleTestAccessor.getExecutorInfo(app1Id, "exec-1", resolver3) should be (Some(shuffleInfo1))
    ShuffleTestAccessor.getExecutorInfo(app2Id, "exec-2", resolver3) should be (None)

  }

}
