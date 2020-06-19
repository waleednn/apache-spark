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

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.time.temporal.ChronoField
import java.util.{Calendar, TimeZone}
import java.util.Calendar.{DAY_OF_MONTH, DST_OFFSET, ERA, HOUR_OF_DAY, MINUTE, MONTH, SECOND, YEAR, ZONE_OFFSET}

import scala.collection.mutable.AnyRefMap

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import org.apache.spark.sql.catalyst.util.DateTimeConstants._
import org.apache.spark.sql.catalyst.util.DateTimeUtils._

/**
 * The collection of functions for rebasing days and microseconds from/to the hybrid calendar
 * (Julian + Gregorian since 1582-10-15) which is used by Spark 2.4 and earlier versions
 * to/from Proleptic Gregorian calendar which is used by Spark since version 3.0. See SPARK-26651.
 */
object RebaseDateTime {
  /**
   * Rebases days since the epoch from an original to an target calendar, for instance,
   * from a hybrid (Julian + Gregorian) to Proleptic Gregorian calendar.
   *
   * It finds the latest switch day which is less than the given `days`, and adds the difference
   * in days associated with the switch days to the given `days`.
   * The function is based on linear search which starts from the most recent switch days.
   * This allows to perform less comparisons for modern dates.
   *
   * @param switches The days when difference in days between original and target calendar
   *                 was changed.
   * @param diffs The differences in days between calendars.
   * @param days The number of days since the epoch 1970-01-01 to be rebased
   *             to the target calendar.
   * @return The rebased days.
   */
  private def rebaseDays(switches: Array[Int], diffs: Array[Int], days: Int): Int = {
    var i = switches.length
    do { i -= 1 } while (i > 0 && days < switches(i))
    days + diffs(i)
  }

  // The differences in days between Julian and Proleptic Gregorian dates.
  // The diff at the index `i` is applicable for all days in the date interval:
  // [julianGregDiffSwitchDay(i), julianGregDiffSwitchDay(i+1))
  private val julianGregDiffs = Array(2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0)
  // The sorted days in Julian calendar when difference in days between Julian and
  // Proleptic Gregorian calendars was changed.
  // The starting point is the `0001-01-01` (-719164 days since the epoch in
  // Julian calendar). This array is not applicable for dates before the staring point.
  // Rebasing switch days and diffs `julianGregDiffSwitchDay` and `julianGregDiffs`
  // was generated by the `localRebaseJulianToGregorianDays` function.
  private val julianGregDiffSwitchDay = Array(
    -719164, -682945, -646420, -609895, -536845, -500320, -463795,
    -390745, -354220, -317695, -244645, -208120, -171595, -141427)

  final val lastSwitchJulianDay: Int = julianGregDiffSwitchDay.last

  // The first days of Common Era (CE) which is mapped to the '0001-01-01' date in Julian calendar.
  private final val julianCommonEraStartDay = julianGregDiffSwitchDay(0)

  /**
   * Converts the given number of days since the epoch day 1970-01-01 to a local date in Julian
   * calendar, interprets the result as a local date in Proleptic Gregorian calendar, and takes the
   * number of days since the epoch from the Gregorian local date.
   *
   * This is used to guarantee backward compatibility, as Spark 2.4 and earlier versions use
   * Julian calendar for dates before 1582-10-15, while Spark 3.0 and later use Proleptic Gregorian
   * calendar. See SPARK-26651.
   *
   * For example:
   *   Julian calendar: 1582-01-01 -> -141704
   *   Proleptic Gregorian calendar: 1582-01-01 -> -141714
   * The code below converts -141704 to -141714.
   *
   * @param days The number of days since the epoch in Julian calendar. It can be negative.
   * @return The rebased number of days in Gregorian calendar.
   */
  private[sql] def localRebaseJulianToGregorianDays(days: Int): Int = {
    val utcCal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both
      // the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setTimeZone(TimeZoneUTC)
      .setInstant(Math.multiplyExact(days, MILLIS_PER_DAY))
      .build()
    val localDate = LocalDate.of(
      utcCal.get(YEAR),
      utcCal.get(MONTH) + 1,
      // The number of days will be added later to handle non-existing
      // Julian dates in Proleptic Gregorian calendar.
      // For example, 1000-02-29 exists in Julian calendar because 1000
      // is a leap year but it is not a leap year in Gregorian calendar.
      1)
      .`with`(ChronoField.ERA, utcCal.get(ERA))
      .plusDays(utcCal.get(DAY_OF_MONTH) - 1)
    Math.toIntExact(localDate.toEpochDay)
  }

  /**
   * An optimized version of [[localRebaseJulianToGregorianDays(Int)]]. This method leverages the
   * pre-calculated rebasing array to save calculation. For dates of Before Common Era, the
   * function falls back to the regular unoptimized version.
   *
   * @param days The number of days since the epoch in Julian calendar. It can be negative.
   * @return The rebased number of days in Gregorian calendar.
   */
  def rebaseJulianToGregorianDays(days: Int): Int = {
    if (days < julianCommonEraStartDay) {
      localRebaseJulianToGregorianDays(days)
    } else {
      rebaseDays(julianGregDiffSwitchDay, julianGregDiffs, days)
    }
  }

  // The differences in days between Proleptic Gregorian and Julian dates.
  // The diff at the index `i` is applicable for all days in the date interval:
  // [gregJulianDiffSwitchDay(i), gregJulianDiffSwitchDay(i+1))
  private val gregJulianDiffs = Array(
    -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
  // The sorted days in Proleptic Gregorian calendar when difference in days between
  // Proleptic Gregorian and Julian was changed.
  // The starting point is the `0001-01-01` (-719162 days since the epoch in
  // Proleptic Gregorian calendar). This array is not applicable for dates before the staring point.
  // Rebasing switch days and diffs `gregJulianDiffSwitchDay` and `gregJulianDiffs`
  // was generated by the `localRebaseGregorianToJulianDays` function.
  private val gregJulianDiffSwitchDay = Array(
    -719162, -682944, -646420, -609896, -536847, -500323, -463799, -390750,
    -354226, -317702, -244653, -208129, -171605, -141436, -141435, -141434,
    -141433, -141432, -141431, -141430, -141429, -141428, -141427)

  final val lastSwitchGregorianDay: Int = gregJulianDiffSwitchDay.last

  // The first days of Common Era (CE) which is mapped to the '0001-01-01' date
  // in Proleptic Gregorian calendar.
  private final val gregorianCommonEraStartDay = gregJulianDiffSwitchDay(0)

  private final val gregorianStartDate = LocalDate.of(1582, 10, 15)
  private final val julianEndDate = LocalDate.of(1582, 10, 4)

  /**
   * Converts the given number of days since the epoch day 1970-01-01 to a local date in Proleptic
   * Gregorian calendar, interprets the result as a local date in Julian calendar, and takes the
   * number of days since the epoch from the Julian local date.
   *
   * This is used to guarantee backward compatibility, as Spark 2.4 and earlier versions use
   * Julian calendar for dates before 1582-10-15, while Spark 3.0 and later use Proleptic Gregorian
   * calendar. See SPARK-26651.
   *
   * For example:
   *   Proleptic Gregorian calendar: 1582-01-01 -> -141714
   *   Julian calendar: 1582-01-01 -> -141704
   * The code below converts -141714 to -141704.
   *
   * @param days The number of days since the epoch in Proleptic Gregorian calendar.
   *             It can be negative.
   * @return The rebased number of days in Julian calendar.
   */
  private[sql] def localRebaseGregorianToJulianDays(days: Int): Int = {
    var localDate = LocalDate.ofEpochDay(days)
    if (localDate.isAfter(julianEndDate) && localDate.isBefore(gregorianStartDate)) {
      localDate = gregorianStartDate
    }
    val utcCal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both
      // the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setTimeZone(TimeZoneUTC)
      .setDate(localDate.getYear, localDate.getMonthValue - 1, localDate.getDayOfMonth)
      .build()
    Math.toIntExact(Math.floorDiv(utcCal.getTimeInMillis, MILLIS_PER_DAY))
  }

  /**
   * An optimized version of [[localRebaseGregorianToJulianDays(Int)]]. This method leverages the
   * pre-calculated rebasing array to save calculation. For dates of Before Common Era, the
   * function falls back to the regular unoptimized version.
   *
   * @param days The number of days since the epoch in Gregorian calendar. It can be negative.
   * @return The rebased number of days since the epoch in Julian calendar.
   */
  def rebaseGregorianToJulianDays(days: Int): Int = {
    if (days < gregorianCommonEraStartDay) {
      localRebaseGregorianToJulianDays(days)
    } else {
      rebaseDays(gregJulianDiffSwitchDay, gregJulianDiffs, days)
    }
  }


  /**
   * The class describes JSON records with microseconds rebasing info.
   * Here is an example of JSON file:
   * {{{
   *   [
   *     {
   *       "tz": "Europe/Paris",
   *       "switches": [-123, 0],
   *       "diffs": [422000000, 0]
   *     }
   *   ]
   * }}}
   *
   * @param tz One of time zone ID which is expected to be acceptable by `ZoneId.of`.
   * @param switches An ordered array of seconds since the epoch when the diff between
   *                 two calendars are changed.
   * @param diffs Differences in seconds associated with elements of `switches`.
   */
  private case class JsonRebaseRecord(tz: String, switches: Array[Long], diffs: Array[Long])

  /**
   * Rebasing info used to convert microseconds from an original to a target calendar.
   *
   * @param switches An ordered array of microseconds since the epoch when the diff between
   *                 two calendars are changed.
   * @param diffs Differences in microseconds associated with elements of `switches`.
   */
  private[sql] case class RebaseInfo(switches: Array[Long], diffs: Array[Long])

  /**
   * Rebases micros since the epoch from an original to an target calendar, for instance,
   * from a hybrid (Julian + Gregorian) to Proleptic Gregorian calendar.
   *
   * It finds the latest switch micros which is less than the given `micros`, and adds the
   * difference in micros associated with the switch micros to the given `micros`.
   * The function is based on linear search which starts from the most recent switch micros.
   * This allows to perform less comparisons for modern timestamps.
   *
   * @param rebaseInfo The rebasing info contains an ordered micros when difference in micros
   *                   between original and target calendar was changed,
   *                   and differences in micros between calendars
   * @param micros The number of micros since the epoch 1970-01-01T00:00:00Z to be rebased
   *               to the target calendar. It can be negative.
   * @return The rebased micros.
   */
  private def rebaseMicros(rebaseInfo: RebaseInfo, micros: Long): Long = {
    val switches = rebaseInfo.switches
    var i = switches.length
    do { i -= 1 } while (i > 0 && micros < switches(i))
    micros + rebaseInfo.diffs(i)
  }

  // Loads rebasing info from an JSON file. JSON records in the files should conform to
  // `JsonRebaseRecord`. AnyRefMap is used here instead of Scala's immutable map because
  // it is 2 times faster in DateTimeRebaseBenchmark.
  private[sql] def loadRebaseRecords(fileName: String): AnyRefMap[String, RebaseInfo] = {
    val file = Thread.currentThread().getContextClassLoader.getResource(fileName)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val jsonRebaseRecords = mapper.readValue[Seq[JsonRebaseRecord]](file)
    val anyRefMap = new AnyRefMap[String, RebaseInfo]((3 * jsonRebaseRecords.size) / 2)
    jsonRebaseRecords.foreach { jsonRecord =>
      val rebaseInfo = RebaseInfo(jsonRecord.switches, jsonRecord.diffs)
      var i = 0
      while (i < rebaseInfo.switches.length) {
        rebaseInfo.switches(i) = rebaseInfo.switches(i) * MICROS_PER_SECOND
        rebaseInfo.diffs(i) = rebaseInfo.diffs(i) * MICROS_PER_SECOND
        i += 1
      }
      anyRefMap.update(jsonRecord.tz, rebaseInfo)
    }
    anyRefMap
  }

  /**
   * A map of time zone IDs to its ordered time points (instants in microseconds since the epoch)
   * when the difference between 2 instances associated with the same local timestamp in
   * Proleptic Gregorian and the hybrid calendar was changed, and to the diff at the index `i` is
   * applicable for all microseconds in the time interval:
   *   [gregJulianDiffSwitchMicros(i), gregJulianDiffSwitchMicros(i+1))
   */
  private val gregJulianRebaseMap = loadRebaseRecords("gregorian-julian-rebase-micros.json")

  private def getLastSwitchTs(rebaseMap: AnyRefMap[String, RebaseInfo]): Long = {
    val latestTs = rebaseMap.values.map(_.switches.last).max
    require(rebaseMap.values.forall(_.diffs.last == 0),
      s"Differences between Julian and Gregorian calendar after ${microsToInstant(latestTs)} " +
      "are expected to be zero for all available time zones.")
    latestTs
  }
  // The switch time point after which all diffs between Gregorian and Julian calendars
  // across all time zones are zero
  final val lastSwitchGregorianTs: Long = getLastSwitchTs(gregJulianRebaseMap)

  private final val gregorianStartTs = LocalDateTime.of(gregorianStartDate, LocalTime.MIDNIGHT)
  private final val julianEndTs = LocalDateTime.of(
    julianEndDate,
    LocalTime.of(23, 59, 59, 999999999))

  /**
   * Converts the given number of microseconds since the epoch '1970-01-01T00:00:00Z', to a local
   * date-time in Proleptic Gregorian calendar with timezone identified by `zoneId`, interprets the
   * result as a local date-time in Julian calendar with the same timezone, and takes microseconds
   * since the epoch from the Julian local date-time.
   *
   * This is used to guarantee backward compatibility, as Spark 2.4 and earlier versions use
   * Julian calendar for dates before 1582-10-15, while Spark 3.0 and later use Proleptic Gregorian
   * calendar. See SPARK-26651.
   *
   * For example:
   *   Proleptic Gregorian calendar: 1582-01-01 00:00:00.123456 -> -12244061221876544
   *   Julian calendar: 1582-01-01 00:00:00.123456 -> -12243196799876544
   * The code below converts -12244061221876544 to -12243196799876544.
   *
   * @param tz The time zone at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in Proleptic Gregorian calendar. It can be negative.
   * @return The rebased microseconds since the epoch in Julian calendar.
   */
  private[sql] def rebaseGregorianToJulianMicros(tz: TimeZone, micros: Long): Long = {
    val instant = microsToInstant(micros)
    val zoneId = tz.toZoneId
    val zonedDateTime = instant.atZone(zoneId)
    var ldt = zonedDateTime.toLocalDateTime
    if (ldt.isAfter(julianEndTs) && ldt.isBefore(gregorianStartTs)) {
      ldt = LocalDateTime.of(gregorianStartDate, ldt.toLocalTime)
    }
    val cal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setDate(ldt.getYear, ldt.getMonthValue - 1, ldt.getDayOfMonth)
      .setTimeOfDay(ldt.getHour, ldt.getMinute, ldt.getSecond)
      .setTimeZone(tz)
      .build()
    // A local timestamp can have 2 instants in the cases of switching from:
    //  1. Summer to winter time.
    //  2. One standard time zone to another one. For example, Asia/Hong_Kong switched from JST
    //     to HKT on 18 November, 1945 01:59:59 AM.
    // Below we check that the original `instant` is earlier or later instant. If it is an earlier
    // instant, we take the standard and DST offsets of the previous day otherwise of the next one.
    val trans = zoneId.getRules.getTransition(ldt)
    if (trans != null && trans.isOverlap) {
      val cloned = cal.clone().asInstanceOf[Calendar]
      // Does the current offset belong to the offset before the transition.
      // If so, we will take zone offsets from the previous day otherwise from the next day.
      // This assumes that transitions cannot happen often than once per 2 days.
      val shift = if (trans.getOffsetBefore == zonedDateTime.getOffset) -1 else 1
      cloned.add(DAY_OF_MONTH, shift)
      cal.set(ZONE_OFFSET, cloned.get(ZONE_OFFSET))
      cal.set(DST_OFFSET, cloned.get(DST_OFFSET))
    }
    millisToMicros(cal.getTimeInMillis) + ldt.get(ChronoField.MICRO_OF_SECOND)
  }

  /**
   * An optimized version of [[rebaseGregorianToJulianMicros(ZoneId, Long)]]. This method leverages
   * the pre-calculated rebasing maps to save calculation. If the rebasing map doesn't contain
   * information about the current JVM system time zone or `micros` is related to Before Common Era,
   * the function falls back to the regular unoptimized version.
   *
   * Note: The function assumes that the input micros was derived from a local timestamp
   *       at the default system JVM time zone in Proleptic Gregorian calendar.
   *
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in Proleptic Gregorian calendar. It can be negative.
   * @return The rebased microseconds since the epoch in Julian calendar.
   */
  def rebaseGregorianToJulianMicros(micros: Long): Long = {
    if (micros >= lastSwitchGregorianTs) {
      micros
    } else {
      val timeZone = TimeZone.getDefault
      val tzId = timeZone.getID
      val rebaseRecord = gregJulianRebaseMap.getOrNull(tzId)
      if (rebaseRecord == null || micros < rebaseRecord.switches(0)) {
        rebaseGregorianToJulianMicros(timeZone, micros)
      } else {
        rebaseMicros(rebaseRecord, micros)
      }
    }
  }

  /**
   * Converts the given number of microseconds since the epoch '1970-01-01T00:00:00Z', to a local
   * date-time in Julian calendar with timezone identified by `zoneId`, interprets the result as a
   * local date-time in Proleptic Gregorian calendar with the same timezone, and takes microseconds
   * since the epoch from the Gregorian local date-time.
   *
   * This is used to guarantee backward compatibility, as Spark 2.4 and earlier versions use
   * Julian calendar for dates before 1582-10-15, while Spark 3.0 and later use Proleptic Gregorian
   * calendar. See SPARK-26651.
   *
   * For example:
   *   Julian calendar: 1582-01-01 00:00:00.123456 -> -12243196799876544
   *   Proleptic Gregorian calendar: 1582-01-01 00:00:00.123456 -> -12244061221876544
   * The code below converts -12243196799876544 to -12244061221876544.
   *
   * @param tz The time zone at which the rebasing should be performed.
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in the Julian calendar. It can be negative.
   * @return The rebased microseconds since the epoch in Proleptic Gregorian calendar.
   */
  private[sql] def rebaseJulianToGregorianMicros(tz: TimeZone, micros: Long): Long = {
    val cal = new Calendar.Builder()
      // `gregory` is a hybrid calendar that supports both the Julian and Gregorian calendar systems
      .setCalendarType("gregory")
      .setInstant(microsToMillis(micros))
      .setTimeZone(tz)
      .build()
    val localDateTime = LocalDateTime.of(
      cal.get(YEAR),
      cal.get(MONTH) + 1,
      // The number of days will be added later to handle non-existing
      // Julian dates in Proleptic Gregorian calendar.
      // For example, 1000-02-29 exists in Julian calendar because 1000
      // is a leap year but it is not a leap year in Gregorian calendar.
      1,
      cal.get(HOUR_OF_DAY),
      cal.get(MINUTE),
      cal.get(SECOND),
      (Math.floorMod(micros, MICROS_PER_SECOND) * NANOS_PER_MICROS).toInt)
      .`with`(ChronoField.ERA, cal.get(ERA))
      .plusDays(cal.get(DAY_OF_MONTH) - 1)
    val zoneId = tz.toZoneId
    val zonedDateTime = localDateTime.atZone(zoneId)
    // In the case of local timestamp overlapping, we need to choose the correct time instant
    // which is related to the original local timestamp. We look ahead of 1 day, and if the next
    // date has the same standard zone and DST offsets, the current local timestamp belongs to
    // the period after the transition. In that case, we take the later zoned date time otherwise
    // earlier one. Here, we assume that transitions happen not often than once per day.
    val trans = zoneId.getRules.getTransition(localDateTime)
    val adjustedZdt = if (trans != null && trans.isOverlap) {
      val dstOffset = cal.get(DST_OFFSET)
      val zoneOffset = cal.get(ZONE_OFFSET)
      cal.add(DAY_OF_MONTH, 1)
      if (zoneOffset == cal.get(ZONE_OFFSET) && dstOffset == cal.get(DST_OFFSET)) {
        zonedDateTime.withLaterOffsetAtOverlap()
      } else {
        zonedDateTime.withEarlierOffsetAtOverlap()
      }
    } else zonedDateTime
    instantToMicros(adjustedZdt.toInstant)
  }

  // The rebasing maps to convert microseconds from the hybrid calendar (Julian + Gregorian)
  // to Proleptic Gregorian calendar. It maps time zone IDs to ordered timestamps (ascending order)
  // where at every timestamps the difference between 2 calendars was changed, and to ordered
  // differences between 2 calendars. The diff at the index `i` is applicable for all timestamps
  // in the interval: [julianGregDiffSwitchMicros(i), julianGregDiffSwitchMicros(i+1))
  private val julianGregRebaseMap = loadRebaseRecords("julian-gregorian-rebase-micros.json")

  // The switch time point after which all diffs between Julian and Gregorian calendars
  // across all time zones are zero
  final val lastSwitchJulianTs: Long = getLastSwitchTs(julianGregRebaseMap)

  /**
   * An optimized version of [[rebaseJulianToGregorianMicros(ZoneId, Long)]]. This method leverages
   * the pre-calculated rebasing maps to save calculation. If the rebasing map doesn't contain
   * information about the current JVM system time zone or `micros` is related to Before Common Era,
   * the function falls back to the regular unoptimized version.
   *
   * Note: The function assumes that the input micros was derived from a local timestamp
   *       at the default system JVM time zone in the Julian calendar.
   *
   * @param micros The number of microseconds since the epoch '1970-01-01T00:00:00Z'
   *               in the Julian calendar. It can be negative.
   * @return The rebased microseconds since the epoch in Proleptic Gregorian calendar.
   */
  def rebaseJulianToGregorianMicros(micros: Long): Long = {
    if (micros >= lastSwitchJulianTs) {
      micros
    } else {
      val timeZone = TimeZone.getDefault
      val tzId = timeZone.getID
      val rebaseRecord = julianGregRebaseMap.getOrNull(tzId)
      if (rebaseRecord == null || micros < rebaseRecord.switches(0)) {
        rebaseJulianToGregorianMicros(timeZone, micros)
      } else {
        rebaseMicros(rebaseRecord, micros)
      }
    }
  }
}
