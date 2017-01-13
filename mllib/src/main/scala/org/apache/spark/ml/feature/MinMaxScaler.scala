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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[MinMaxScaler]] and [[MinMaxScalerModel]].
 */
private[feature] trait MinMaxScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * lower bound after transformation, shared by all features
   * Default: 0.0
   * @group param
   */
  val min: DoubleParam = new DoubleParam(this, "min",
    "lower bound of the output feature range")

  /** @group getParam */
  def getMin: Double = $(min)

  /**
   * upper bound after transformation, shared by all features
   * Default: 1.0
   * @group param
   */
  val max: DoubleParam = new DoubleParam(this, "max",
    "upper bound of the output feature range")

  /** @group getParam */
  def getMax: Double = $(max)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(min) < $(max), s"The specified min(${$(min)}) is larger or equal to max(${$(max)})")
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

}

/**
 * Rescale each feature individually to a common range [min, max] linearly using column summary
 * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
 * feature E is calculated as:
 *
 * <blockquote>
 *    $$
 *    Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 *    $$
 * </blockquote>
 *
 * For the case $E_{max} == E_{min}$, $Rescaled(e_i) = 0.5 * (max + min)$.
 *
 * @note Since zero values will probably be transformed to non-zero values, output of the
 * transformer will be DenseVector even for sparse input.
 */
@Since("1.5.0")
class MinMaxScaler @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Estimator[MinMaxScalerModel] with MinMaxScalerParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("minMaxScal"))

  setDefault(min -> 0.0, max -> 1.0)

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): MinMaxScalerModel = {
    transformSchema(dataset.schema, logging = true)

    val (mins, maxs, nnz, cnt) = dataset.select($(inputCol)).rdd.map {
      row => row.getAs[Vector](0)
    }.treeAggregate[(Array[Double], Array[Double], Array[Long], Long)](
      (Array.emptyDoubleArray, Array.emptyDoubleArray, Array.emptyLongArray, 0L))(
      seqOp = {
        case ((min, max, nnz, cnt), vec) if cnt == 0 =>
          val n = vec.size
          val min_ = Array.fill[Double](n)(Double.MaxValue)
          val max_ = Array.fill[Double](n)(Double.MinValue)
          val nnz_ = Array.fill[Long](n)(0L)
          vec.foreachActive {
            case (i, v) if v != 0.0 =>
              min_(i) = v
              max_(i) = v
              nnz_(i) = 1L
            case _ =>
          }
          (min_, max_, nnz_, 1L)
        case ((min, max, nnz, cnt), vec) =>
          require(min.length == vec.size,
            s"Dimensions mismatch when adding new sample: ${min.length} != ${vec.size}")
          vec.foreachActive {
            case (i, v) if v != 0.0 =>
              if (v < min(i)) {
                min(i) = v
              } else if (v > max(i)) {
                max(i) = v
              }
              nnz(i) += 1
            case _ =>
          }
          (min, max, nnz, cnt + 1)
      }, combOp = {
        case ((min1, max1, nnz1, cnt1), (min2, max2, nnz2, cnt2)) if cnt1 == 0 =>
          (min2, max2, nnz2, cnt2)
        case ((min1, max1, nnz1, cnt1), (min2, max2, nnz2, cnt2)) if cnt2 == 0 =>
          (min1, max1, nnz1, cnt1)
        case ((min1, max1, nnz1, cnt1), (min2, max2, nnz2, cnt2)) =>
          require(min1.length == min2.length,
            s"Dimensions mismatch when merging: ${min1.length} != ${min2.length}")
          for (i <- 0 until min1.length) {
            min1(i) = math.min(min1(i), min2(i))
            max1(i) = math.max(max1(i), max2(i))
            nnz1(i) += nnz2(i)
          }
          (min1, max1, nnz1, cnt1 + cnt2)
      })

    require(cnt > 0, "Input dataset must be non-empty")

    for(i <- 0 until mins.length) {
      if (nnz(i) < cnt) {
        if (mins(i) > 0.0) {
          mins(i) = 0.0
        } else if (maxs(i) < 0.0) {
          maxs(i) = 0.0
        }
      }
    }

    copyValues(new MinMaxScalerModel(uid, Vectors.dense(mins), Vectors.dense(maxs))
      .setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScaler = defaultCopy(extra)
}

@Since("1.6.0")
object MinMaxScaler extends DefaultParamsReadable[MinMaxScaler] {

  @Since("1.6.0")
  override def load(path: String): MinMaxScaler = super.load(path)
}

/**
 * Model fitted by [[MinMaxScaler]].
 *
 * @param originalMin min value for each original column during fitting
 * @param originalMax max value for each original column during fitting
 *
 * TODO: The transformer does not yet set the metadata in the output column (SPARK-8529).
 */
@Since("1.5.0")
class MinMaxScalerModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val originalMin: Vector,
    @Since("2.0.0") val originalMax: Vector)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams with MLWritable {

  import MinMaxScalerModel._

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val originalRange = (originalMax.asBreeze - originalMin.asBreeze).toArray
    val minArray = originalMin.toArray

    val reScale = udf { (vector: Vector) =>
      val scale = $(max) - $(min)

      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.length
      var i = 0
      while (i < size) {
        if (!values(i).isNaN) {
          val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
          values(i) = raw * scale + $(min)
        }
        i += 1
      }
      Vectors.dense(values)
    }

    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScalerModel = {
    val copied = new MinMaxScalerModel(uid, originalMin, originalMax)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new MinMaxScalerModelWriter(this)
}

@Since("1.6.0")
object MinMaxScalerModel extends MLReadable[MinMaxScalerModel] {

  private[MinMaxScalerModel]
  class MinMaxScalerModelWriter(instance: MinMaxScalerModel) extends MLWriter {

    private case class Data(originalMin: Vector, originalMax: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.originalMin, instance.originalMax)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MinMaxScalerModelReader extends MLReader[MinMaxScalerModel] {

    private val className = classOf[MinMaxScalerModel].getName

    override def load(path: String): MinMaxScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(originalMin: Vector, originalMax: Vector) =
        MLUtils.convertVectorColumnsToML(data, "originalMin", "originalMax")
          .select("originalMin", "originalMax")
          .head()
      val model = new MinMaxScalerModel(metadata.uid, originalMin, originalMax)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[MinMaxScalerModel] = new MinMaxScalerModelReader

  @Since("1.6.0")
  override def load(path: String): MinMaxScalerModel = super.load(path)
}
