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

package org.apache.spark.streaming.zeromq

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import akka.actor.{ActorSystem, Props, SupervisorStrategy}
import akka.util.ByteString
import akka.zeromq.Subscribe

import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.SparkEnv
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.akka.{ActorSupervisorStrategy, ActorSystemFactory, AkkaUtils}
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object ZeroMQUtils {
  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param ssc StreamingContext object
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic
   *                       and each frame has sequence of byte thus it needs the converter
   *                       (which might be deserializer of bytes) to translate from sequence
   *                       of sequence of bytes, where sequence refer to a frame
   *                       and sub sequence refer to its payload.
   * @param storageLevel RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param supervisorStrategy the supervisor strategy (default:
   *                           ActorSupervisorStrategy.defaultStrategy)
   */
  @deprecated("Use createStream with actorSystemCreator instead", "1.6.0")
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: Seq[ByteString] => Iterator[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      supervisorStrategy: SupervisorStrategy = ActorSupervisorStrategy.defaultStrategy
    ): ReceiverInputDStream[T] = {
    createStream(ssc, () => SparkEnv.get.actorSystem, publisherUrl, subscribe, bytesToObjects,
      storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param ssc StreamingContext object
   * @param actorSystemCreator a function to create ActorSystem in executors
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic
   *                       and each frame has sequence of byte thus it needs the converter
   *                       (which might be deserializer of bytes) to translate from sequence
   *                       of sequence of bytes, where sequence refer to a frame
   *                       and sub sequence refer to its payload.
   * @param storageLevel RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param supervisorStrategy the supervisor strategy (default:
   *                           ActorSupervisorStrategy.defaultStrategy)
   */
  def createStream[T: ClassTag](
      ssc: StreamingContext,
      actorSystemCreator: () => ActorSystem,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: Seq[ByteString] => Iterator[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      supervisorStrategy: SupervisorStrategy = ActorSupervisorStrategy.defaultStrategy
      ): ReceiverInputDStream[T] = {
    val cleanF = ssc.sc.clean(bytesToObjects)
    AkkaUtils.createStream[T](ssc, actorSystemCreator,
      Props(new ZeroMQReceiver(publisherUrl, subscribe, cleanF)),
      "ZeroMQReceiver", storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc JavaStreamingContext object
   * @param publisherUrl Url of remote ZeroMQ publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might be
   *                       deserializer of bytes) to translate from sequence of sequence of bytes,
   *                       where sequence refer to a frame and sub sequence refer to its payload.
   * @param storageLevel Storage level to use for storing the received objects
   * @param supervisorStrategy the supervisor strategy
   */
  @deprecated("Use createStream with actorSystemFactory instead", "1.6.0")
  def createStream[T](
      jssc: JavaStreamingContext,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]],
      storageLevel: StorageLevel,
      supervisorStrategy: SupervisorStrategy
      ): JavaReceiverInputDStream[T] = {
    val actorSystemFactory = new ActorSystemFactory {
      override def create(): ActorSystem = SparkEnv.get.actorSystem
    }
    createStream(jssc, actorSystemFactory, publisherUrl, subscribe, bytesToObjects, storageLevel,
      supervisorStrategy)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc JavaStreamingContext object
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param publisherUrl Url of remote ZeroMQ publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might be
   *                       deserializer of bytes) to translate from sequence of sequence of bytes,
   *                       where sequence refer to a frame and sub sequence refer to its payload.
   * @param storageLevel Storage level to use for storing the received objects
   * @param supervisorStrategy the supervisor strategy
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]],
      storageLevel: StorageLevel,
      supervisorStrategy: SupervisorStrategy
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val fn = (x: Seq[ByteString]) =>
      bytesToObjects.call(x.map(_.toArray).toArray).iterator().asScala
    createStream[T](jssc.ssc, () => actorSystemFactory.create(),
      publisherUrl, subscribe, fn, storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc           JavaStreamingContext object
   * @param publisherUrl   Url of remote zeromq publisher
   * @param subscribe      Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might be
   *                       deserializer of bytes) to translate from sequence of sequence of bytes,
   *                       where sequence refer to a frame and sub sequence refer to its payload.
   * @param storageLevel   RDD storage level.
   */
  @deprecated("Use createStream with actorSystemFactory instead", "1.6.0")
  def createStream[T](
      jssc: JavaStreamingContext,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]],
      storageLevel: StorageLevel
      ): JavaReceiverInputDStream[T] = {
    val actorSystemFactory = new ActorSystemFactory {
      override def create(): ActorSystem = SparkEnv.get.actorSystem
    }
    createStream(jssc, actorSystemFactory, publisherUrl, subscribe, bytesToObjects, storageLevel)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc JavaStreamingContext object
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might be
   *                       deserializer of bytes) to translate from sequence of sequence of bytes,
   *                       where sequence refer to a frame and sub sequence refer to its payload.
   * @param storageLevel RDD storage level.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val fn = (x: Seq[ByteString]) =>
      bytesToObjects.call(x.map(_.toArray).toArray).iterator().asScala
    createStream[T](
      jssc.ssc, () => actorSystemFactory.create(), publisherUrl, subscribe, fn, storageLevel)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc           JavaStreamingContext object
   * @param publisherUrl   Url of remote zeromq publisher
   * @param subscribe      Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might
   *                       be deserializer of bytes) to translate from sequence of sequence of
   *                       bytes, where sequence refer to a frame and sub sequence refer to its
   *                       payload.
   */
  @deprecated("Use createStream with actorSystemFactory instead", "1.6.0")
  def createStream[T](
      jssc: JavaStreamingContext,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]]
      ): JavaReceiverInputDStream[T] = {
    val actorSystemFactory = new ActorSystemFactory {
      override def create(): ActorSystem = SparkEnv.get.actorSystem
    }
    createStream(jssc, actorSystemFactory, publisherUrl, subscribe, bytesToObjects)
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param jssc  JavaStreamingContext object
   * @param actorSystemFactory an ActorSystemFactory to create ActorSystem in executors
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe Topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic and each
   *                       frame has sequence of byte thus it needs the converter(which might
   *                       be deserializer of bytes) to translate from sequence of sequence of
   *                       bytes, where sequence refer to a frame and sub sequence refer to its
   *                       payload.
   */
  def createStream[T](
      jssc: JavaStreamingContext,
      actorSystemFactory: ActorSystemFactory,
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: JFunction[Array[Array[Byte]], java.lang.Iterable[T]]
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val fn = (x: Seq[ByteString]) =>
      bytesToObjects.call(x.map(_.toArray).toArray).iterator().asScala
    createStream[T](jssc.ssc, () => actorSystemFactory.create(), publisherUrl, subscribe, fn)
  }
}
