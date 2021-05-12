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

package org.apache.spark.sql.catalyst.plans.logical.statsEstimation

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics, Union}
import org.apache.spark.sql.types._

/**
 * Estimate the number of output rows by doing the sum of output rows for each child of union,
 * and estimate min and max stats for each column by finding the overall min and max of that
 * column coming from its children.
 */
object UnionEstimation {
  import EstimationUtils._

  private def createStatComparator(dt: DataType): (Any, Any) => Boolean = dt match {
    case ByteType => (a: Any, b: Any) =>
      ByteType.ordering.lt(a.asInstanceOf[Byte], b.asInstanceOf[Byte])
    case ShortType => (a: Any, b: Any) =>
      ShortType.ordering.lt(a.asInstanceOf[Short], b.asInstanceOf[Short])
    case IntegerType => (a: Any, b: Any) =>
      IntegerType.ordering.lt(a.asInstanceOf[Int], b.asInstanceOf[Int])
    case LongType => (a: Any, b: Any) =>
      LongType.ordering.lt(a.asInstanceOf[Long], b.asInstanceOf[Long])
    case FloatType => (a: Any, b: Any) =>
      FloatType.ordering.lt(a.asInstanceOf[Float], b.asInstanceOf[Float])
    case DoubleType => (a: Any, b: Any) =>
      DoubleType.ordering.lt(a.asInstanceOf[Double], b.asInstanceOf[Double])
    case _: DecimalType => (a: Any, b: Any) =>
      dt.asInstanceOf[DecimalType].ordering.lt(a.asInstanceOf[Decimal], b.asInstanceOf[Decimal])
    case DateType => (a: Any, b: Any) =>
      DateType.ordering.lt(a.asInstanceOf[DateType.InternalType],
        b.asInstanceOf[DateType.InternalType])
    case TimestampType => (a: Any, b: Any) =>
      TimestampType.ordering.lt(a.asInstanceOf[TimestampType.InternalType],
        b.asInstanceOf[TimestampType.InternalType])
    case _ =>
      throw new IllegalStateException(s"Unsupported data type: ${dt.catalogString}")
  }

  private def isTypeSupported(dt: DataType): Boolean = dt match {
    case ByteType | IntegerType | ShortType | FloatType | LongType |
         DoubleType | DateType | _: DecimalType | TimestampType => true
    case _ => false
  }

  def estimate(union: Union): Option[Statistics] = {
    val sizeInBytes = union.children.map(_.stats.sizeInBytes).sum
    val outputRows = if (rowCountsExist(union.children: _*)) {
      Some(union.children.map(_.stats.rowCount.get).sum)
    } else {
      None
    }

    val unionOutput = union.output

    val attrToComputeMinMaxStats = union.children.map(_.output).transpose.zipWithIndex.filter {
      case (attrs, outputIndex) => isTypeSupported(unionOutput(outputIndex).dataType) &&
        // checks if all the children has min/max stats for an attribute
        attrs.zipWithIndex.forall {
          case (attr, childIndex) =>
            val attrStats = union.children(childIndex).stats.attributeStats
            attrStats.get(attr).isDefined && attrStats(attr).hasMinMaxStats
        }
    }

    val attrStatsWithMinMax = if (attrToComputeMinMaxStats.nonEmpty) {
      val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()
      attrToComputeMinMaxStats.foreach {
        case (attrs, outputIndex) =>
          val dataType = unionOutput(outputIndex).dataType
          val statComparator = createStatComparator(dataType)
          val colStatValues = attrs.zipWithIndex
            .foldLeft[(Option[Any], Option[Any])]((None, None)) {
              case ((minVal, maxVal), (attr, childIndex)) =>
                val colStat = union.children(childIndex).stats.attributeStats(attr)
                val min = if (minVal.isEmpty || statComparator(colStat.min.get, minVal.get)) {
                  colStat.min
                } else {
                  minVal
                }
                val max = if (maxVal.isEmpty || statComparator(maxVal.get, colStat.max.get)) {
                  colStat.max
                } else {
                  maxVal
                }
                (min, max)
            }
          val newStat = ColumnStat(min = colStatValues._1, max = colStatValues._2)
          outputAttrStats += unionOutput(outputIndex) -> newStat
      }
      AttributeMap(outputAttrStats.toSeq)
    } else {
      AttributeMap.empty[ColumnStat]
    }

    val attrToComputeNullCount = union.children.map(_.output).transpose.zipWithIndex.filter {
      case (attrs, _) => attrs.zipWithIndex.forall {
        case (attr, childIndex) =>
          val attrStats = union.children(childIndex).stats.attributeStats
          attrStats.get(attr).isDefined && attrStats(attr).nullCount.isDefined
      }
    }

    val newAttrStats = if (attrToComputeNullCount.nonEmpty) {
      val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()
      attrToComputeNullCount.foreach {
        case (attrs, outputIndex) =>
          val colWithNullStatValues = attrs.zipWithIndex.foldLeft[Option[BigInt]](None) {
            case (totalNullCount, (attr, childIndex)) =>
              val colStat = union.children(childIndex).stats.attributeStats(attr)
              if (totalNullCount.isDefined) {
                Some(totalNullCount.get + colStat.nullCount.get)
              } else {
                colStat.nullCount
              }
          }

          if (attrStatsWithMinMax.get(unionOutput(outputIndex)).isDefined) {
            val updatedColStat = attrStatsWithMinMax.get(unionOutput(outputIndex))
              .get.copy(nullCount = colWithNullStatValues)
            outputAttrStats += (unionOutput(outputIndex) -> updatedColStat)
          } else {
            outputAttrStats += (unionOutput(outputIndex) ->
              ColumnStat(nullCount = colWithNullStatValues))
          }
      }
      AttributeMap(attrStatsWithMinMax.toSeq ++ outputAttrStats.toSeq)
    } else {
      attrStatsWithMinMax
    }

    Some(
      Statistics(
        sizeInBytes = sizeInBytes,
        rowCount = outputRows,
        attributeStats = newAttrStats))
  }
}
