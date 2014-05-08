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

package org.apache.spark.streaming.twitter

import twitter4j._
import twitter4j.auth.Authorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.Receiver
import scala.language.higherKinds

/* A stream of Twitter statuses, potentially filtered by one or more keywords.
*
* @constructor create a new Twitter stream using the supplied Twitter4J authentication credentials.
* An optional set of string filters can be used to restrict the set of tweets. The Twitter API is
* such that this may return a sampled subset of all tweets during each interval.
*
* If no Authorization object is provided, initializes OAuth authorization using the system
* properties twitter4j.oauth.consumerKey, .consumerSecret, .accessToken and .accessTokenSecret.
*/
private[streaming]
class TwitterInputDStream(
                           @transient ssc_ : StreamingContext,
                           twitterAuth: Option[Authorization],
                           keywordFilters: Seq[String],
                           storageLevel: StorageLevel,
                           geoLocationFilter: Seq[Seq[Double]]
                           ) extends ReceiverInputDStream[Status](ssc_)  {

  private def createOAuthAuthorization(): Authorization = {
    new OAuthAuthorization(new ConfigurationBuilder().build())
  }

  private val authorization = twitterAuth.getOrElse(createOAuthAuthorization())

  override def getReceiver(): Receiver[Status] = {
    new TwitterReceiver(authorization, keywordFilters, storageLevel, geoLocationFilter)
  }
}

private[streaming]
class TwitterReceiver(
                       twitterAuth: Authorization,
                       keywordFilters: Seq[String],
                       storageLevel: StorageLevel,
                       geoLocationFilter: Seq[Seq[Double]]
                       ) extends Receiver[Status](storageLevel) with Logging {

  var twitterStream: TwitterStream = _

  def onStart() {
    twitterStream = new TwitterStreamFactory().getInstance(twitterAuth)
    twitterStream.addListener(new StatusListener {
      def onStatus(status: Status) = {
        store(status)
      }
      // Unimplemented
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(i: Int) {}
      def onScrubGeo(l: Long, l1: Long) {}
      def onStallWarning(stallWarning: StallWarning) {}
      def onException(e: Exception) {
        restart("Error receiving tweets", e)
      }
    })

    val query = new FilterQuery
    if (keywordFilters.size > 0) {
      query.track(keywordFilters.toArray)
    }

    if (geoLocationFilter.size > 0){
      println(arrayToString(ss2aa(geoLocationFilter)))
      query.locations(ss2aa(geoLocationFilter))

    }

    if(keywordFilters.size + geoLocationFilter.size > 0){
      println(query.toString())
      twitterStream.filter(query)
    }
    else {
      twitterStream.sample()
    }
    logInfo("Twitter receiver started")
  }

  def onStop() {
    twitterStream.shutdown()
    logInfo("Twitter receiver stopped")
  }

  def arrayToString(a: Array[Array[Double]]) : String = {
    val str = for (l <- a) yield l.mkString("{", ",", "}")
    str.mkString("{",",\n","}")
  }

  def ss2aa[A,B[_],C[_]](c: C[B[A]])(
    implicit b2seq: B[A] => Seq[A], c2seq: C[B[A]] => Seq[B[A]], ma: ClassManifest[A]
    ) = c2seq(c).map(b => b2seq(b).toArray).toArray
}
