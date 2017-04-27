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

package org.apache.spark.storage

import java.io.{File, IOException}

import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.{SparkConfWithEnv, Utils}

/**
 * Tests for the spark.local.dir and SPARK_LOCAL_DIRS configuration options.
 */
class LocalDirsSuite extends SparkFunSuite with BeforeAndAfter {

  before {
    Utils.clearLocalRootDirs()
  }

  after {
    Utils.clearLocalRootDirs()
  }

  test("Utils.getLocalDir() returns a valid directory, even if some local dirs are missing") {
    // Regression test for SPARK-2974
    assert(!new File("/NONEXISTENT_PATH").exists())
    val conf = new SparkConf(false)
      .set("spark.local.dir", s"/NONEXISTENT_PATH,${System.getProperty("java.io.tmpdir")}")
    assert(new File(Utils.getLocalDir(conf)).exists())
  }

  test("SPARK_LOCAL_DIRS override also affects driver") {
    // Regression test for SPARK-2975
    assert(!new File("/NONEXISTENT_PATH").exists())
    // spark.local.dir only contains invalid directories, but that's not a problem since
    // SPARK_LOCAL_DIRS will override it on both the driver and workers:
    val conf = new SparkConfWithEnv(Map("SPARK_LOCAL_DIRS" -> System.getProperty("java.io.tmpdir")))
      .set("spark.local.dir", "/NONEXISTENT_PATH")
    assert(new File(Utils.getLocalDir(conf)).exists())
  }

  test("Utils.getLocalDir() throws an exception if any temporary directory cannot be retrieved") {
    val path1 = "/NONEXISTENT_PATH_ONE"
    val path2 = "/NONEXISTENT_PATH_TWO"
    assert(!new File(path1).exists())
    assert(!new File(path2).exists())
    val conf = new SparkConf(false).set("spark.local.dir", s"$path1,$path2")
    val message = intercept[IOException] {
      Utils.getLocalDir(conf)
    }.getMessage
    // If any temporary directory could not be retrieved under the given paths above, it should
    // throw an exception with the message that includes the paths.
    assert(message.contains(s"$path1,$path2"))
  }
}
