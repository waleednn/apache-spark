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

package org.apache.spark.resource

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.config._


/**
 * Manager of resource profiles.
 * Note we never remove a resource profile at this point. Its expected this number if small
 * so this shouldn't be much overhead.
 */
@Evolving
private[spark] class ResourceProfileManager(sparkConf: SparkConf) {
  private val resourceProfileIdToResourceProfile =
    new ConcurrentHashMap[Int, ImmutableResourceProfile]()

  private val master = sparkConf.get("spark.master")

  private val defaultProfile = ImmutableResourceProfile.getOrCreateDefaultProfile(sparkConf)
  addResourceProfile(defaultProfile)

  def defaultResourceProfile: ImmutableResourceProfile = defaultProfile

  private val taskCpusDefaultProfile = defaultProfile.getTaskCpus.get

  // If we use anything except the default profile, its only supported on YARN right now
  // TODO - add tests
  private[spark] def isSupported(rp: ImmutableResourceProfile): Unit = {
    val dynamicEnabled = sparkConf.get(DYN_ALLOCATION_ENABLED)
    val isNotYarn = !master.equals("yarn")
    val isYarn = !isNotYarn
    val isNotDefaultProfile = rp.id != ImmutableResourceProfile.DEFAULT_RESOURCE_PROFILE_ID
    if ((isNotDefaultProfile && isNotYarn) || (isNotDefaultProfile && isYarn && !dynamicEnabled)) {
      throw new SparkException("ResourceProfiles are only supported on YARN with dynamic " +
        "allocation enabled.")
    }
  }

  def addResourceProfile(rp: ImmutableResourceProfile): Unit = {
    isSupported(rp)
    // force the computation of maxTasks and limitingResource now so we don't have cost later
    rp.limitingResource(sparkConf)
    resourceProfileIdToResourceProfile.putIfAbsent(rp.id, rp)
  }

  /*
   * Gets the ResourceProfile associated with the id, if a profile doesn't exist
   * it returns the default ResourceProfile created from the application level configs.
   */
  def resourceProfileFromId(rpId: Int): ImmutableResourceProfile = {
    val rp = resourceProfileIdToResourceProfile.get(rpId)
    if (rp == null) {
      throw new SparkException(s"ResourceProfileId $rpId not found!")
    }
    rp
  }

  def taskCpusForProfileId(rpId: Int): Int = {
    resourceProfileFromId(rpId).getTaskCpus.getOrElse(taskCpusDefaultProfile)
  }
}
