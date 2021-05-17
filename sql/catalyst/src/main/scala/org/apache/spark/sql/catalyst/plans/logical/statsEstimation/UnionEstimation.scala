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

import org.apache.spark.sql.catalyst.expressions.AttributeMap
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

    val newMinMaxStats = computeMinMaxStats(union)
    val newNullCountStats = computeNullCountStats(union)
    val newAttrStats = combineStats(newMinMaxStats, newNullCountStats)

    Some(
      Statistics(
        sizeInBytes = sizeInBytes,
        rowCount = outputRows,
        attributeStats = newAttrStats))
  }

  /** This method computes the min-max statistics and return the attribute stats Map. */
  private def computeMinMaxStats(union: Union) = {
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
    val outputAttrStats = attrToComputeMinMaxStats.map {
        case (attrs, outputIndex) =>
          val dataType = unionOutput(outputIndex).dataType
          val statComparator = createStatComparator(dataType)
          val minMaxValue = attrs.zipWithIndex.foldLeft[(Option[Any], Option[Any])]((None, None)) {
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
          val newStat = ColumnStat(min = minMaxValue._1, max = minMaxValue._2)
          unionOutput(outputIndex) -> newStat
      }
    if (outputAttrStats.nonEmpty) {
      AttributeMap(outputAttrStats.toSeq)
    } else {
      AttributeMap.empty[ColumnStat]
    }
  }

  /** This method computes the null count statistics and return the attribute stats Map. */
  private def computeNullCountStats(union: Union) = {
    val unionOutput = union.output
    val attrToComputeNullCount = union.children.map(_.output).transpose.zipWithIndex.filter {
      case (attrs, _) => attrs.zipWithIndex.forall {
        case (attr, childIndex) =>
          val attrStats = union.children(childIndex).stats.attributeStats
          attrStats.get(attr).isDefined && attrStats(attr).nullCount.isDefined
      }
    }
    val outputAttrStats = attrToComputeNullCount.map {
      case (attrs, outputIndex) =>
        val firstStat = union.children.head.stats.attributeStats(attrs.head)
        val firstNullCount = firstStat.nullCount.get
        val colWithNullStatValues = attrs.zipWithIndex.tail.foldLeft[BigInt](firstNullCount) {
          case (totalNullCount, (attr, childIndex)) =>
            val colStat = union.children(childIndex).stats.attributeStats(attr)
            totalNullCount + colStat.nullCount.get
        }
        val newStat = ColumnStat(nullCount = Some(colWithNullStatValues))
        unionOutput(outputIndex) -> newStat
    }
    if (outputAttrStats.nonEmpty) {
      AttributeMap(outputAttrStats.toSeq)
    } else {
      AttributeMap.empty[ColumnStat]
    }
  }

  // Combine the two maps by updating the min-max stats map with null count stats.
  private def combineStats(
      minMaxStats: AttributeMap[ColumnStat],
      nullCountStats: AttributeMap[ColumnStat]) = {
    val updatedNullCountStats = nullCountStats.keys.map { key =>
      if (minMaxStats.get(key).isDefined) {
        val updatedColsStats = minMaxStats(key).copy(nullCount = nullCountStats(key).nullCount)
        key -> updatedColsStats
      } else {
        key -> nullCountStats(key)
      }
    }
    AttributeMap(minMaxStats.toSeq ++ updatedNullCountStats)
  }
}
