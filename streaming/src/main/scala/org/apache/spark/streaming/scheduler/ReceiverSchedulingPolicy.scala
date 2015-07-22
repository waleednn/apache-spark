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

package org.apache.spark.streaming.scheduler

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.streaming.receiver.Receiver

/**
 * A ReceiverScheduler trying to balance executors' load. Here is the approach to schedule executors
 * for a receiver.
 * <ol>
 *   <li>
 *     If preferredLocation is set, preferredLocation should be one of the candidate executors.
 *   </li>
 *   <li>
 *     Every executor will be assigned to a weight according to the receivers running or scheduling
 *     on it.
 *     <ul>
 *       <li>
 *         If a receiver is running on an executor, it contributes 1.0 to the executor's weight.
 *       </li>
 *       <li>
 *         If a receiver is scheduled to an executor but has not yet run, it contributes
 *         `1.0 / #candidate_executors_of_this_receiver` to the executor's weight.</li>
 *     </ul>
 *     At last, if there are more than 3 idle executors (weight = 0), returns all idle executors.
 *     Otherwise, we only return 3 best options according to the weights.
 *   </li>
 * </ol>
 *
 */
private[streaming] class ReceiverSchedulingPolicy {

  /**
   * Try our best to schedule receivers with evenly distributed. However, if the
   * `preferredLocation`s of receivers are not even, we may not be able to schedule them evenly
   * because we have to respect them.
   *
   * This method is called when we start to launch receivers at the first time.
   */
  def scheduleReceivers(
      receivers: Seq[Receiver[_]], executors: Seq[String]): Map[Int, Seq[String]] = {
    if (receivers.isEmpty) {
      return Map.empty
    }

    require(executors.nonEmpty, "There is no executor up")

    val hostToExecutors = executors.groupBy(_.split(":")(0))
    val locations = new Array[mutable.ArrayBuffer[String]](receivers.length)
    val numReceiversOnExecutor = mutable.HashMap[String, Int]()
    // Set the initial value to 0
    executors.foreach(numReceiversOnExecutor(_) = 0)

    // Firstly, we need to respect "preferredLocation". So if a receiver has "preferredLocation",
    // we need to make sure the "preferredLocation" is in the candidate location list.
    for (i <- 0 until receivers.length) {
      locations(i) = new mutable.ArrayBuffer[String]()
      // Note: preferredLocation is host but executors are host:port
      receivers(i).preferredLocation.foreach { host =>
        hostToExecutors.get(host) match {
          case Some(executorsOnHost) =>
            // preferredLocation is a known host. Select an executor that has the least receivers in
            // this host
            val scheduledLocation =
              executorsOnHost.minBy(executor => numReceiversOnExecutor(executor))
            locations(i) += scheduledLocation
            numReceiversOnExecutor(scheduledLocation) =
              numReceiversOnExecutor(scheduledLocation) + 1
          case None =>
            // preferredLocation is an unknown host.
            // Note: There are two cases:
            // 1. This executor is not up. But it may be up later.
            // 2. This executor is dead, or it's not a host in the cluster.
            // Currently, simply add host to the scheduled locations
            locations(i) += host
        }
      }
    }

    // For those receivers that don't have preferredLocation, make sure we assign at least one
    // executor to them.
    for (scheduledLocations <- locations.filter(_.isEmpty)) {
      // Select the executor that has the least receivers
      val (executor, numReceivers) = numReceiversOnExecutor.minBy(_._2)
      scheduledLocations += executor
      numReceiversOnExecutor(executor) = numReceivers + 1
    }

    // Assign idle executors to receivers that have less executors
    val idleExecutors = numReceiversOnExecutor.filter(_._2 == 0).map(_._1)
    for (executor <- idleExecutors) {
      // Assign an idle executor to the receiver that has least locations.
      val scheduledLocations = locations.minBy(_.size)
      scheduledLocations += executor
    }

    receivers.map(_.streamId).zip(locations).toMap
  }

  /**
   * Return a list of candidate executors to run the receiver. If the list is empty, the caller can
   * run this receiver in arbitrary executor.
   *
   * This method is called when a receiver is registering with ReceiverTracker or is restarting.
   */
  def rescheduleReceiver(
      receiverId: Int,
      preferredLocation: Option[String],
      receiverTrackingInfoMap: Map[Int, ReceiverTrackingInfo],
      executors: Seq[String]): Seq[String] = {
    if (executors.isEmpty) {
      return Seq.empty
    }

    // Always try to schedule to the preferred locations
    val locations = mutable.Set[String]()
    locations ++= preferredLocation

    val executorWeights = receiverTrackingInfoMap.values.flatMap { receiverTrackingInfo =>
      receiverTrackingInfo.state match {
        case ReceiverState.INACTIVE => Nil
        case ReceiverState.SCHEDULED =>
          val scheduledLocations = receiverTrackingInfo.scheduledLocations.get
          // The probability that a scheduled receiver will run in an executor is
          // 1.0 / scheduledLocations.size
          scheduledLocations.map(location => location -> (1.0 / scheduledLocations.size))
        case ReceiverState.ACTIVE => Seq(receiverTrackingInfo.runningLocation.get -> 1.0)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).sum) // Sum weights for each executor

    val idleExecutors = (executors.toSet -- executorWeights.keys).toSeq
    if (idleExecutors.size >= 3) {
      // If there are more than 3 idle executors, return all of them
      locations ++= idleExecutors
    } else {
      // If there are less than 3 idle executors, return 3 best options
      locations ++= idleExecutors
      val sortedExecutors = executorWeights.toSeq.sortBy(_._2).map(_._1)
      locations ++= (idleExecutors ++ sortedExecutors).take(3)
    }
    locations.toSeq
  }
}
