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

package org.apache.spark.sql.catalyst.json

import java.nio.charset.{Charset, StandardCharsets}
import java.util.{Locale, TimeZone}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util._

/**
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
 */
private[sql] class JSONOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends Logging with Serializable  {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
      this(
        CaseInsensitiveMap(parameters),
        defaultTimeZoneId,
        defaultColumnNameOfCorruptRecord)
  }

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get("prefersDecimal").map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get("allowComments").map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get("allowBackslashEscapingAnyCharacter").map(_.toBoolean).getOrElse(false)
  private val allowUnquotedControlChars =
    parameters.get("allowUnquotedControlChars").map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)

  /**
   * A string between two consecutive JSON records.
   */
  val lineSeparator: Option[String] = parameters.get("lineSep").map { sep =>
    require(sep.nonEmpty, "'lineSep' cannot be an empty string.")
    sep
  }

  /**
   * Standard encoding (charset) name. For example UTF-8, UTF-16LE and UTF-32BE.
   * If the encoding is not specified (None), it will be detected automatically
   * when the multiLine option is set to `true`.
   */
  val encoding: Option[String] = parameters.get("encoding")
    .orElse(parameters.get("charset")).map { enc =>
      // The following encodings are not supported in per-line mode (multiline is false)
      // because they cause some problems in reading files with BOM which is supposed to
      // present in the files with such encodings. After splitting input files by lines,
      // only the first lines will have the BOM which leads to impossibility for reading
      // the rest lines. Besides of that, the lineSep option must have the BOM in such
      // encodings which can never present between lines.
      val blacklist = Seq(Charset.forName("UTF-16"), Charset.forName("UTF-32"))
      val isBlacklisted = blacklist.contains(Charset.forName(enc))
      require(multiLine || !isBlacklisted,
        s"""The ${enc} encoding must not be included in the blacklist when multiLine is disabled:
           | ${blacklist.mkString(", ")}""".stripMargin)

      val isLineSepRequired = !(multiLine == false &&
        Charset.forName(enc) != StandardCharsets.UTF_8 && lineSeparator.isEmpty)
      require(isLineSepRequired, s"The lineSep option must be specified for the $enc encoding")

      enc
  }

  /**
   * A sequence of bytes between two consecutive json records in read.
   * Format of the `lineSep` option is:
   *   selector (1 char) + separator spec (any length) | sequence of chars
   *
   * Currently the following selectors are supported:
   * - 'x' + sequence of bytes in hexadecimal format. For example: "x0a 0d".
   *   Hex pairs can be separated by any chars different from 0-9,A-F,a-f
   * - '\' - reserved for a sequence of control chars like "\r\n"
   *         and unicode escape like "\u000D\u000A"
   * - 'r' and '/' - reserved for future use
   */
  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator.collect {
    case hexs if hexs.startsWith("x") =>
      hexs.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray
        .map(Integer.parseInt(_, 16).toByte)
    case reserved if reserved.startsWith("r") || reserved.startsWith("/") =>
      throw new NotImplementedError(s"The $reserved selector has not supported yet")
    case lineSep =>
      lineSep.getBytes(encoding.map(Charset.forName(_)).getOrElse(StandardCharsets.UTF_8))
  }
  val lineSeparatorInWrite: String = lineSeparator.getOrElse("\n")

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, allowUnquotedControlChars)
  }
}
