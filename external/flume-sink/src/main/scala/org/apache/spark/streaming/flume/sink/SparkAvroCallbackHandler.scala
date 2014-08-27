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
package org.apache.spark.streaming.flume.sink

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.flume.Channel
import org.apache.commons.lang.RandomStringUtils
import com.google.common.util.concurrent.ThreadFactoryBuilder

/**
 * Class that implements the SparkFlumeProtocol, that is used by the Avro Netty Server to process
 * requests. Each getEvents, ack and nack call is forwarded to an instance of this class.
 * @param threads Number of threads to use to process requests.
 * @param channel The channel that the sink pulls events from
 * @param transactionTimeout Timeout in millis after which the transaction if not acked by Spark
 *                           is rolled back.
 */
// Flume forces transactions to be thread-local. So each transaction *must* be committed, or
// rolled back from the thread it was originally created in. So each getEvents call from Spark
// creates a TransactionProcessor which runs in a new thread, in which the transaction is created
// and events are pulled off the channel. Once the events are sent to spark,
// that thread is blocked and the TransactionProcessor is saved in a map,
// until an ACK or NACK comes back or the transaction times out (after the specified timeout).
// When the response comes or a timeout is hit, the TransactionProcessor is retrieved and then
// unblocked, at which point the transaction is committed or rolled back.

private[flume] class SparkAvroCallbackHandler(val threads: Int, val channel: Channel,
  val transactionTimeout: Int, val backOffInterval: Int) extends SparkFlumeProtocol with Logging {
  val transactionExecutorOpt = Option(Executors.newFixedThreadPool(threads,
    new ThreadFactoryBuilder().setDaemon(true)
      .setNameFormat("Spark Sink Processor Thread - %d").build()))
  private val sequenceNumberToProcessor = new
      ConcurrentHashMap[CharSequence, TransactionProcessor]()
  // sequenceNumberToProcessor only contains processors that have to be ack-ed.
  // There may be some processors whose getEventBatch method is being called when shutdown happens.
  // They need to be closed as well.
  private val activeProcessors = new mutable.HashSet[TransactionProcessor]()

  private val activeProcessorMapLock = new ReentrantLock()
  // This sink will not persist sequence numbers and reuses them if it gets restarted.
  // So it is possible to commit a transaction which may have been meant for the sink before the
  // restart.
  // Since the new txn may not have the same sequence number we must guard against accidentally
  // committing a new transaction. To reduce the probability of that happening a random string is
  // prepended to the sequence number. Does not change for life of sink
  private val seqBase = RandomStringUtils.randomAlphanumeric(8)
  private val seqCounter = new AtomicLong(0)

  @volatile private var stopped = false

  /**
   * Returns a bunch of events to Spark over Avro RPC.
   * @param n Maximum number of events to return in a batch
   * @return [[EventBatch]] instance that has a sequence number and an array of at most n events
   */
  override def getEventBatch(n: Int): EventBatch = {
    logDebug("Got getEventBatch call from Spark.")
    if (stopped) {
      new EventBatch("Spark sink has been stopped!", "", java.util.Collections.emptyList())
    } else {
      val sequenceNumber = seqBase + seqCounter.incrementAndGet()
      activeProcessorMapLock.lock()
      val processor = new TransactionProcessor(channel, sequenceNumber,
        n, transactionTimeout, backOffInterval, this)
      try {
        activeProcessors.add(processor)
      } finally {
        activeProcessorMapLock.unlock()
      }
      transactionExecutorOpt.foreach(executor => {
        executor.submit(processor)
      })
      // Wait until a batch is available - will be an error if error message is non-empty
      val batch = processor.getEventBatch
      if (!SparkSinkUtils.isErrorBatch(batch)) {
        sequenceNumberToProcessor.put(sequenceNumber.toString, processor)
        logDebug("Sending event batch with sequence number: " + sequenceNumber)
      }
      batch

    }
  }

  /**
   * Called by Spark to indicate successful commit of a batch
   * @param sequenceNumber The sequence number of the event batch that was successful
   */
  override def ack(sequenceNumber: CharSequence): Void = {
    logDebug("Received Ack for batch with sequence number: " + sequenceNumber)
    completeTransaction(sequenceNumber, success = true)
    null
  }

  /**
   * Called by Spark to indicate failed commit of a batch
   * @param sequenceNumber The sequence number of the event batch that failed
   * @return
   */
  override def nack(sequenceNumber: CharSequence): Void = {
    completeTransaction(sequenceNumber, success = false)
    logInfo("Spark failed to commit transaction. Will reattempt events.")
    null
  }

  /**
   * Helper method to commit or rollback a transaction.
   * @param sequenceNumber The sequence number of the batch that was completed
   * @param success Whether the batch was successful or not.
   */
  private def completeTransaction(sequenceNumber: CharSequence, success: Boolean) {
    Option(removeAndGetProcessor(sequenceNumber)).foreach(processor => {
      processor.batchProcessed(success)
    })
  }

  /**
   * Helper method to remove the TxnProcessor for a Sequence Number. Can be used to avoid a leak.
   * @param sequenceNumber
   * @return The transaction processor for the corresponding batch. Note that this instance is no
   *         longer tracked and the caller is responsible for that txn processor.
   */
  private[sink] def removeAndGetProcessor(sequenceNumber: CharSequence): TransactionProcessor = {
    // The toString is required!
    val processor = sequenceNumberToProcessor.remove(sequenceNumber.toString)
    activeProcessorMapLock.lock()
    try {
      activeProcessors.remove(processor) // Remove it from the ones to be shutdown as well
    } finally {
      activeProcessorMapLock.unlock()
    }
    processor
  }

  /**
   * Shuts down the executor used to process transactions.
   */
  def shutdown() {
    logInfo("Shutting down Spark Avro Callback Handler")
    stopped = true
    activeProcessorMapLock.lock()
    try {
      activeProcessors.foreach(_.shutdown())
    } finally {
      activeProcessorMapLock.unlock()
    }
    transactionExecutorOpt.foreach(executor => {
      executor.shutdownNow()
    })
  }
}
