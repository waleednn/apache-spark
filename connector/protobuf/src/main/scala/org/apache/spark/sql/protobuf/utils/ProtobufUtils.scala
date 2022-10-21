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

package org.apache.spark.sql.protobuf.utils

import java.io.{BufferedInputStream, FileInputStream, IOException}
import java.util.Locale

import scala.collection.JavaConverters._

import com.google.protobuf.{DescriptorProtos, Descriptors, InvalidProtocolBufferException}
import com.google.protobuf.Descriptors.{Descriptor, FieldDescriptor}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

private[sql] object ProtobufUtils extends Logging {

  /** Wrapper for a pair of matched fields, one Catalyst and one corresponding Protobuf field. */
  private[sql] case class ProtoMatchedField(
      catalystField: StructField,
      catalystPosition: Int,
      fieldDescriptor: FieldDescriptor)

  /**
   * Helper class to perform field lookup/matching on Protobuf schemas.
   *
   * This will match `descriptor` against `catalystSchema`, attempting to find a matching field in
   * the Protobuf descriptor for each field in the Catalyst schema and vice-versa, respecting
   * settings for case sensitivity. The match results can be accessed using the getter methods.
   *
   * @param descriptor
   *   The descriptor in which to search for fields. Must be of type Descriptor.
   * @param catalystSchema
   *   The Catalyst schema to use for matching.
   * @param protoPath
   *   The seq of parent field names leading to `protoSchema`.
   * @param catalystPath
   *   The seq of parent field names leading to `catalystSchema`.
   */
  class ProtoSchemaHelper(
      descriptor: Descriptor,
      catalystSchema: StructType,
      protoPath: Seq[String],
      catalystPath: Seq[String]) {
    if (descriptor.getName == null) {
      throw QueryCompilationErrors.unknownProtobufMessageTypeError(
        descriptor.getName,
        descriptor.getContainingType)
    }

    private[this] val protoFieldArray = descriptor.getFields.asScala.toArray
    private[this] val fieldMap = descriptor.getFields.asScala
      .groupBy(_.getName.toLowerCase(Locale.ROOT))
      .mapValues(_.toSeq) // toSeq needed for scala 2.13

    /** The fields which have matching equivalents in both Protobuf and Catalyst schemas. */
    val matchedFields: Seq[ProtoMatchedField] = catalystSchema.zipWithIndex.flatMap {
      case (sqlField, sqlPos) =>
        getFieldByName(sqlField.name).map(ProtoMatchedField(sqlField, sqlPos, _))
    }

    /**
     * Validate that there are no Catalyst fields which don't have a matching Protobuf field,
     * throwing [[AnalysisException]] if such extra fields are found. If
     * `ignoreNullable` is false, consider nullable Catalyst fields to be eligible to be an extra
     * field; otherwise, ignore nullable Catalyst fields when checking for extras.
     */
    def validateNoExtraCatalystFields(ignoreNullable: Boolean): Unit =
      catalystSchema.fields.foreach { sqlField =>
        if (getFieldByName(sqlField.name).isEmpty &&
          (!ignoreNullable || !sqlField.nullable)) {
          throw QueryCompilationErrors.cannotFindCatalystTypeInProtobufSchemaError(
            toFieldStr(catalystPath :+ sqlField.name))
        }
      }

    /**
     * Validate that there are no Protobuf fields which don't have a matching Catalyst field,
     * throwing [[AnalysisException]] if such extra fields are found. Only required
     * (non-nullable) fields are checked; nullable fields are ignored.
     */
    def validateNoExtraRequiredProtoFields(): Unit = {
      val extraFields = protoFieldArray.toSet -- matchedFields.map(_.fieldDescriptor)
      extraFields.filterNot(isNullable).foreach { extraField =>
        throw QueryCompilationErrors.cannotFindProtobufFieldInInCatalystError(
          toFieldStr(protoPath :+ extraField.getName()))
      }
    }

    /**
     * Extract a single field from the contained Protobuf schema which has the desired field name,
     * performing the matching with proper case sensitivity according to SQLConf.resolver.
     *
     * @param name
     *   The name of the field to search for.
     * @return
     *   `Some(match)` if a matching Protobuf field is found, otherwise `None`.
     */
    private[protobuf] def getFieldByName(name: String): Option[FieldDescriptor] = {

      // get candidates, ignoring case of field name
      val candidates = fieldMap.getOrElse(name.toLowerCase(Locale.ROOT), Seq.empty)

      // search candidates, taking into account case sensitivity settings
      candidates.filter(f => SQLConf.get.resolver(f.getName(), name)) match {
        case Seq(protoField) => Some(protoField)
        case Seq() => None
        case matches =>
          throw QueryCompilationErrors.protobufFieldMatchError(
            name,
            toFieldStr(protoPath),
            matches.size,
            matches.map(_.getName()).mkString("[", ", ", "]"))
      }
    }
  }

  def buildDescriptor(descFilePath: String, messageName: String): Descriptor = {
    val fileDescriptor: Descriptors.FileDescriptor = parseFileDescriptor(descFilePath)
    var result: Descriptors.Descriptor = null;

    for (descriptor <- fileDescriptor.getMessageTypes.asScala) {
      if (descriptor.getName().equals(messageName)) {
        result = descriptor
      }
    }

    if (null == result) {
      throw QueryCompilationErrors.unableToLocateProtobuMessageError(messageName)
    }
    result
  }

  def parseFileDescriptor(descFilePath: String): Descriptors.FileDescriptor = {
    var fileDescriptorSet: DescriptorProtos.FileDescriptorSet = null
    try {
      val dscFile = new BufferedInputStream(new FileInputStream(descFilePath))
      fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(dscFile)
    } catch {
      case ex: InvalidProtocolBufferException =>
        throw QueryCompilationErrors.descrioptorParseError(ex)
      case ex: IOException =>
        throw QueryCompilationErrors.cannotFindDescriptorFileError(descFilePath, ex)
    }

    val descriptorProto: DescriptorProtos.FileDescriptorProto = fileDescriptorSet.getFile(0)
    try {
      val fileDescriptor: Descriptors.FileDescriptor = Descriptors.FileDescriptor.buildFrom(
        descriptorProto,
        new Array[Descriptors.FileDescriptor](0))
      if (fileDescriptor.getMessageTypes().isEmpty()) {
        throw QueryCompilationErrors.noMessageTypeReturnError(fileDescriptor.getName())
      }
      fileDescriptor
    } catch {
      case e: Descriptors.DescriptorValidationException =>
        throw QueryCompilationErrors.failedParsingDescriptorError(e)
    }
  }

  /**
   * Convert a sequence of hierarchical field names (like `Seq(foo, bar)`) into a human-readable
   * string representing the field, like "field 'foo.bar'". If `names` is empty, the string
   * "top-level record" is returned.
   */
  private[protobuf] def toFieldStr(names: Seq[String]): String = names match {
    case Seq() => "top-level record"
    case n => s"field '${n.mkString(".")}'"
  }

  /** Return true if `fieldDescriptor` is optional. */
  private[protobuf] def isNullable(fieldDescriptor: FieldDescriptor): Boolean =
    !fieldDescriptor.isOptional

}
