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

package org.apache.spark.sql.catalyst.util

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZoneOffset}
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.getZoneId

/**
 * Helper functions for testing date and time functionality.
 */
object DateTimeTestUtils {

  val cestTz = getZoneId("+02:00")
  val cetTz = getZoneId("+01:00")
  val jstTz = getZoneId("+09:00")
  val laTz = getZoneId("America/Los_Angeles")
  val mitTz = getZoneId("-09:30")
  val pstTz = getZoneId("-08:00")
  val utcTz = getZoneId("+00:00")

  val ALL_TIMEZONES: Seq[TimeZone] = TimeZone.getAvailableIDs.toSeq.map(TimeZone.getTimeZone)

  val outstandingTimezonesIds: Seq[String] = Seq(
    utcTz.getId,
    pstTz.getId,
    cetTz.getId,
    "Africa/Dakar",
    laTz.getId,
    "Antarctica/Vostok",
    "Asia/Hong_Kong",
    "Europe/Amsterdam")
  val outstandingTimezones: Seq[TimeZone] = outstandingTimezonesIds.map(TimeZone.getTimeZone)
  val outstandingZoneIds: Seq[ZoneId] = outstandingTimezonesIds.map(DateTimeUtils.getZoneId)

  def withDefaultTimeZone[T](newDefaultTimeZone: TimeZone)(block: => T): T = {
    val originalDefaultTimeZone = TimeZone.getDefault
    try {
      TimeZone.setDefault(newDefaultTimeZone)
      block
    } finally {
      TimeZone.setDefault(originalDefaultTimeZone)
    }
  }

  def localDateTimeToMicros(localDateTime: LocalDateTime, zoneId: ZoneId): Long = {
    val instant = localDateTime.atZone(zoneId).toInstant
    DateTimeUtils.instantToMicros(instant)
  }

  // Returns microseconds since epoch for the given date
  def date(
      year: Int,
      month: Byte = 1,
      day: Byte = 1,
      hour: Byte = 0,
      minute: Byte = 0,
      sec: Byte = 0,
      micros: Int = 0,
      zid: ZoneId = ZoneOffset.UTC): Long = {
    val nanos = TimeUnit.MICROSECONDS.toNanos(micros).toInt
    val localDateTime = LocalDateTime.of(year, month, day, hour, minute, sec, nanos)
    localDateTimeToMicros(localDateTime, zid)
  }

  // Returns number of days since epoch for the given date
  def days(
      year: Int,
      month: Byte = 1,
      day: Byte = 1,
      hour: Byte = 0,
      minute: Byte = 0,
      sec: Byte = 0): Int = {
    val micros = date(year, month, day, hour, minute, sec)
    TimeUnit.MICROSECONDS.toDays(micros).toInt
  }

  // Returns microseconds since epoch for current date and give time
  def time(
      hour: Byte = 0,
      minute: Byte = 0,
      sec: Byte = 0,
      micros: Int = 0,
      zid: ZoneId = ZoneOffset.UTC): Long = {
    val nanos = TimeUnit.MICROSECONDS.toNanos(micros).toInt
    val localDate = LocalDate.now(zid)
    val localTime = LocalTime.of(hour, minute, sec, nanos)
    val localDateTime = LocalDateTime.of(localDate, localTime)
    localDateTimeToMicros(localDateTime, zid)
  }

  def secFrac(seconds: Int, milliseconds: Int, microseconds: Int): Long = {
    var result: Long = microseconds
    result = Math.addExact(result, Math.multiplyExact(milliseconds, MICROS_PER_MILLIS))
    result = Math.addExact(result, Math.multiplyExact(seconds, MICROS_PER_SECOND))
    result
  }
}
