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

package org.apache.spark.util

import java.util.concurrent.TimeoutException

import scala.concurrent.Await

import akka.actor._

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.SSLSampleConfigs._


/**
  * Test the AkkaUtils with various security settings.
  */
class AkkaUtilsSuite extends FunSuite with LocalSparkContext with ResetSystemProperties {

  test("remote fetch security bad password") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")

    val securityManager = new SecurityManager(conf)
    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)
    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "true")
    badconf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(badconf)

    assert(securityManagerBad.isAuthenticationEnabled() === true)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = conf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)
    }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security off") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "false")
    conf.set("spark.authenticate.secret", "bad")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "false")
    badconf.set("spark.authenticate.secret", "good")
    val securityManagerBad = new SecurityManager(badconf)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = badconf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security off
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security pass") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val goodconf = new SparkConf
    goodconf.set("spark.authenticate", "true")
    goodconf.set("spark.authenticate.secret", "good")
    val securityManagerGood = new SecurityManager(goodconf)

    assert(securityManagerGood.isAuthenticationEnabled() === true)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = goodconf, securityManager = securityManagerGood)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0, MapStatus(
      BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security on and passwords match
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
           Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch security off client") {
    val conf = new SparkConf
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")

    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val badconf = new SparkConf
    badconf.set("spark.authenticate", "false")
    badconf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(badconf)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = badconf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)
    }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

  test("remote fetch ssl on") {
    val conf = sparkSSLConfig()
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val slaveConf = sparkSSLConfig()
    val securityManagerBad = new SecurityManager(slaveConf)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = slaveConf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    assert(securityManagerBad.isAuthenticationEnabled() === false)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    // this should succeed since security off
    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }


  test("remote fetch ssl on and security enabled") {
    val conf = sparkSSLConfig()
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val slaveConf = sparkSSLConfig()
    slaveConf.set("spark.authenticate", "true")
    slaveConf.set("spark.authenticate.secret", "good")
    val securityManagerBad = new SecurityManager(slaveConf)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = slaveConf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)

    assert(securityManagerBad.isAuthenticationEnabled() === true)

    masterTracker.registerShuffle(10, 1)
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    val size1000 = MapStatus.decompressSize(MapStatus.compressSize(1000L))
    masterTracker.registerMapOutput(10, 0,
      MapStatus(BlockManagerId("a", "hostA", 1000), Array(1000L)))
    masterTracker.incrementEpoch()
    slaveTracker.updateEpoch(masterTracker.getEpoch)

    assert(slaveTracker.getServerStatuses(10, 0).toSeq ===
      Seq((BlockManagerId("a", "hostA", 1000), size1000)))

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }


  test("remote fetch ssl on and security enabled - bad credentials") {
    val conf = sparkSSLConfig()
    conf.set("spark.authenticate", "true")
    conf.set("spark.authenticate.secret", "good")
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === true)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val slaveConf = sparkSSLConfig()
    slaveConf.set("spark.authenticate", "true")
    slaveConf.set("spark.authenticate.secret", "bad")
    val securityManagerBad = new SecurityManager(slaveConf)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = slaveConf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    intercept[akka.actor.ActorNotFound] {
      slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)
    }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }


  test("remote fetch ssl on - untrusted server") {
    val conf = sparkSSLConfigUntrusted()
    val securityManager = new SecurityManager(conf)

    val hostname = "localhost"
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, 0,
      conf = conf, securityManager = securityManager)
    System.setProperty("spark.hostPort", hostname + ":" + boundPort)

    assert(securityManager.isAuthenticationEnabled() === false)

    val masterTracker = new MapOutputTrackerMaster(conf)
    masterTracker.trackerActor = actorSystem.actorOf(
      Props(new MapOutputTrackerMasterActor(masterTracker, conf)), "MapOutputTracker")

    val slaveConf = sparkSSLConfig()
    val securityManagerBad = new SecurityManager(slaveConf)

    val (slaveSystem, _) = AkkaUtils.createActorSystem("spark-slave", hostname, 0,
      conf = slaveConf, securityManager = securityManagerBad)
    val slaveTracker = new MapOutputTrackerWorker(conf)
    val selection = slaveSystem.actorSelection(
      AkkaUtils.address("spark", "localhost", boundPort, "MapOutputTracker", conf))
    val timeout = AkkaUtils.lookupTimeout(conf)
    intercept[TimeoutException] {
      slaveTracker.trackerActor = Await.result(selection.resolveOne(timeout), timeout)
    }

    actorSystem.shutdown()
    slaveSystem.shutdown()
  }

}
