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

package org.apache.spark.sql.connector.catalog

import java.util

import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue, NamedReference, Transform}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.connector.read.{InputPartition, Scan, ScanBuilder, SupportsRuntimeV2Filtering}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class InMemoryTableWithV2Filter(
    name: String,
    schema: StructType,
    partitioning: Array[Transform],
    properties: util.Map[String, String])
  extends InMemoryBaseTable(name, schema, partitioning, properties) with SupportsDeleteV2 {

  override def canDeleteWhere(filters: Array[Predicate]): Boolean = {
    InMemoryTableWithV2Filter.supportsFilters(filters)
  }

  override def deleteWhere(filters: Array[Predicate]): Unit = dataMap.synchronized {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
    dataMap --= InMemoryTableWithV2Filter
      .filtersToKeys(dataMap.keys, partCols.map(_.toSeq.quoted), filters)
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new InMemoryV2FilterScanBuilder(schema)
  }

  class InMemoryV2FilterScanBuilder(tableSchema: StructType)
    extends InMemoryScanBuilder(tableSchema) {
    override def build: Scan =
      InMemoryV2FilterBatchScan(data.map(_.asInstanceOf[InputPartition]), schema, tableSchema)
  }

  case class InMemoryV2FilterBatchScan(
      var _data: Seq[InputPartition],
      readSchema: StructType,
      tableSchema: StructType)
    extends BatchScanBaseClass (_data, readSchema, tableSchema) with SupportsRuntimeV2Filtering {

    override def filterAttributes(): Array[NamedReference] = {
      val scanFields = readSchema.fields.map(_.name).toSet
      partitioning.flatMap(_.references)
        .filter(ref => scanFields.contains(ref.fieldNames.mkString(".")))
    }

    override def filter(filters: Array[Predicate]): Unit = {
      if (partitioning.length == 1 && partitioning.head.references().length == 1) {
        val ref = partitioning.head.references().head
        filters.foreach {
          case p : Predicate if p.name().equals("IN") =>
            if (p.children().length > 1) {
              val filterRef = p.children()(0).asInstanceOf[FieldReference].references.head
              if (filterRef.toString.equals(ref.toString)) {
                val matchingKeys =
                  p.children().drop(1).map(_.asInstanceOf[LiteralValue[_]].value.toString).toSet
                data = data.filter(partition => {
                  val key = partition.asInstanceOf[BufferedRows].keyString
                  matchingKeys.contains(key)
                })
              }
            }
        }
      }
    }
  }
}

object InMemoryTableWithV2Filter {

  def filtersToKeys(
      keys: Iterable[Seq[Any]],
      partitionNames: Seq[String],
      filters: Array[Predicate]): Iterable[Seq[Any]] = {
    keys.filter { partValues =>
      filters.flatMap(splitAnd).forall {
        case p: Predicate if p.name().equals("=") =>
          p.children()(1).asInstanceOf[LiteralValue[_]].value ==
            extractValue(p.children()(0).toString, partitionNames, partValues)
        case p: Predicate if p.name().equals("<=>") =>
          val attrVal = extractValue(p.children()(0).toString, partitionNames, partValues)
          val value = p.children()(1).asInstanceOf[LiteralValue[_]].value
          if (attrVal == null && value == null) {
            true
          } else if (attrVal == null || value == null) {
            false
          } else {
            value == attrVal
          }
        case p: Predicate if p.name().equals("IS NULL") =>
          val attr = p.children()(0).toString
          null == extractValue(attr, partitionNames, partValues)
        case p: Predicate if p.name().equals("IS NOT NULL") =>
          val attr = p.children()(0).toString
          null != extractValue(attr, partitionNames, partValues)
        case p: Predicate if p.name().equals("ALWAYS_TRUE") => true
        case f =>
          throw new IllegalArgumentException(s"Unsupported filter type: $f")
      }
    }
  }

  def supportsFilters(filters: Array[Predicate]): Boolean = {
    filters.flatMap(splitAnd).forall {
      case p: Predicate if p.name().equals("=") => true
      case p: Predicate if p.name().equals("<=>") => true
      case p: Predicate if p.name().equals("IS NULL") => true
      case p: Predicate if p.name().equals("IS NOT NULL") => true
      case p: Predicate if p.name().equals("ALWAYS_TRUE") => true
      case _ => false
    }
  }

  private def extractValue(
      attr: String,
      partFieldNames: Seq[String],
      partValues: Seq[Any]): Any = {
    partFieldNames.zipWithIndex.find(_._1 == attr) match {
      case Some((_, partIndex)) =>
        partValues(partIndex)
      case _ =>
        throw new IllegalArgumentException(s"Unknown filter attribute: $attr")
    }
  }

  private def splitAnd(filter: Predicate): Seq[Predicate] = {
    filter match {
      case and: And => splitAnd(and.left()) ++ splitAnd(and.right())
      case _ => filter :: Nil
    }
  }
}
