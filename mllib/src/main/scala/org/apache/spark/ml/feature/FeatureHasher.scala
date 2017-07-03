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

package org.apache.spark.ml.feature

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCols, HasNumFeatures, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature.{HashingTF => OldHashingTF}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap


@Since("2.3.0")
class FeatureHasher(@Since("2.3.0") override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with HasNumFeatures with DefaultParamsWritable {

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("featureHasher"))

  /** @group setParam */
  @Since("2.3.0")
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(values: String*): this.type = setInputCols(values.toArray)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val hashFunc: Any => Int = OldHashingTF.murmur3Hash
    val n = $(numFeatures)

    val os = transformSchema(dataset.schema)

    val featureCols = $(inputCols).map { colName =>
      val field = dataset.schema(colName)
      field.dataType match {
        case DoubleType | StringType => dataset(field.name)
        case _: NumericType | BooleanType => dataset(field.name).cast(DoubleType).alias(field.name)
      }
    }

    val realFields = os.fields.filter(f => f.dataType.isInstanceOf[NumericType]).map(_.name).toSet

    def hashFeatures = udf { row: Row =>
      val map = new OpenHashMap[Int, Double]()
      $(inputCols).foreach { case colName =>
        val fieldIndex = row.fieldIndex(colName)
        if (!row.isNullAt(fieldIndex)) {
          val (rawIdx, value) = if (realFields(colName)) {
            val value = row.getDouble(fieldIndex)
            val hash = hashFunc(colName)
            (hash, value)
          } else {
            val value = row.getString(fieldIndex)
            val fieldName = s"$colName=$value"
            val hash = hashFunc(fieldName)
            (hash, 1.0)
          }
          val idx = Utils.nonNegativeMod(rawIdx, n)
          map.changeValue(idx, value, v => v + value)
        }
      }
      Vectors.sparse(n, map.toSeq)
    }

    val metadata = os($(outputCol)).metadata
    dataset.select(
      col("*"),
      hashFeatures(struct(featureCols: _*)).as($(outputCol), metadata))
  }

  override def copy(extra: ParamMap): FeatureHasher = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    require(fields.map(_.dataType).forall { case dt =>
      dt.isInstanceOf[NumericType] || dt.isInstanceOf[StringType]
    })
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

@Since("2.3.0")
object FeatureHasher extends DefaultParamsReadable[FeatureHasher] {

  @Since("2.3.0")
  override def load(path: String): FeatureHasher = super.load(path)
}
