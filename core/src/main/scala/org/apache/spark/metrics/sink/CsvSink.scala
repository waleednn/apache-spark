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

package org.apache.spark.metrics.sink

import java.io.File
import java.util.{Locale, Properties}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{CsvReporter, MetricRegistry}

import org.apache.spark.SecurityManager

private[spark] class CsvSink(
  override val properties: Properties,
  val registry: MetricRegistry,
  securityMgr: SecurityManager
) extends Sink with HasPollingPeriod {

  val DIR_KEY = "directory"
  val DEFAULT_DIR = "/tmp/"

  val pollDir = Option(properties.getProperty(DIR_KEY)) match {
    case Some(s) => s
    case None => DEFAULT_DIR
  }

  override val reporter = CsvReporter.forRegistry(registry)
      .formatFor(Locale.US)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .convertRatesTo(TimeUnit.SECONDS)
      .build(new File(pollDir))

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}

