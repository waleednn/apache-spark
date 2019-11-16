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

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Params for [[Imputer]] and [[ImputerModel]].
 */
private[feature] trait ImputerParams extends Params with HasInputCol with HasInputCols
  with HasOutputCol with HasOutputCols with HasRelativeError {

  /**
   * The imputation strategy. Currently only "mean" and "median" are supported.
   * If "mean", then replace missing values using the mean value of the feature.
   * If "median", then replace missing values using the approximate median value of the feature.
   * Default: mean
   *
   * @group param
   */
  final val strategy: Param[String] = new Param(this, "strategy", s"strategy for imputation. " +
    s"If ${Imputer.mean}, then replace missing values using the mean value of the feature. " +
    s"If ${Imputer.median}, then replace missing values using the median value of the feature.",
    ParamValidators.inArray[String](Array(Imputer.mean, Imputer.median)))

  /** @group getParam */
  def getStrategy: String = $(strategy)

  /**
   * The placeholder for the missing values. All occurrences of missingValue will be imputed.
   * Note that null values are always treated as missing.
   * Default: Double.NaN
   *
   * @group param
   */
  final val missingValue: DoubleParam = new DoubleParam(this, "missingValue",
    "The placeholder for the missing values. All occurrences of missingValue will be imputed")

  /** @group getParam */
  def getMissingValue: Double = $(missingValue)

  /** Returns the input and output column names corresponding in pair. */
  private[feature] def getInOutCols(): (Array[String], Array[String]) = {
    if (isSet(inputCol)) {
      (Array($(inputCol)), Array($(outputCol)))
    } else {
      ($(inputCols), $(outputCols))
    }
  }

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    ParamValidators.checkSingleVsMultiColumnParams(this, Seq(outputCol), Seq(outputCols))
    val (inputColNames, outputColNames) = getInOutCols()
    require(inputColNames.length == inputColNames.distinct.length, s"inputCols contains" +
      s" duplicates: (${inputColNames.mkString(", ")})")
    require(outputColNames.length == outputColNames.distinct.length, s"outputCols contains" +
      s" duplicates: (${outputColNames.mkString(", ")})")
    require(inputColNames.length == outputColNames.length, s"inputCols(${inputColNames.length})" +
      s" and outputCols(${outputColNames.length}) should have the same length")
    val outputFields = inputColNames.zip(outputColNames).map { case (inputCol, outputCol) =>
      val inputField = schema(inputCol)
      SchemaUtils.checkNumericType(schema, inputCol)
      StructField(outputCol, inputField.dataType, inputField.nullable)
    }
    StructType(schema ++ outputFields)
  }
}

/**
 * Imputation estimator for completing missing values, either using the mean or the median
 * of the columns in which the missing values are located. The input columns should be of
 * numeric type. Currently Imputer does not support categorical features
 * (SPARK-15041) and possibly creates incorrect values for a categorical feature.
 *
 * Note when an input column is integer, the imputed value is casted (truncated) to an integer type.
 * For example, if the input column is IntegerType (1, 2, 4, null),
 * the output will be IntegerType (1, 2, 4, 2) after mean imputation.
 *
 * Note that the mean/median value is computed after filtering out missing values.
 * All Null values in the input columns are treated as missing, and so are also imputed. For
 * computing median, DataFrameStatFunctions.approxQuantile is used with a relative error of 0.001.
 */
@Since("2.2.0")
class Imputer @Since("2.2.0") (@Since("2.2.0") override val uid: String)
  extends Estimator[ImputerModel] with ImputerParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("imputer"))

  /** @group setParam */
  @Since("3.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.2.0")
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /**
   * Imputation strategy. Available options are ["mean", "median"].
   * @group setParam
   */
  @Since("2.2.0")
  def setStrategy(value: String): this.type = set(strategy, value)

  /** @group setParam */
  @Since("2.2.0")
  def setMissingValue(value: Double): this.type = set(missingValue, value)

  /** @group expertSetParam */
  @Since("3.0.0")
  def setRelativeError(value: Double): this.type = set(relativeError, value)

  setDefault(strategy -> Imputer.mean, missingValue -> Double.NaN)

  override def fit(dataset: Dataset[_]): ImputerModel = {
    transformSchema(dataset.schema, logging = true)
    val spark = dataset.sparkSession

    val (inputColumns, _) = getInOutCols()

    val cols = inputColumns.map { inputCol =>
      when(col(inputCol).equalTo($(missingValue)), null)
        .when(col(inputCol).isNaN, null)
        .otherwise(col(inputCol))
        .cast("double")
        .as(inputCol)
    }

    val results = $(strategy) match {
      case Imputer.mean =>
        // Function avg will ignore null automatically.
        // For a column only containing null, avg will return null.
        val row = dataset.select(cols.map(avg): _*).head()
        Array.range(0, inputColumns.length).map { i =>
          if (row.isNullAt(i)) {
            Double.NaN
          } else {
            row.getDouble(i)
          }
        }

      case Imputer.median =>
        // Function approxQuantile will ignore null automatically.
        // For a column only containing null, approxQuantile will return an empty array.
        dataset.select(cols: _*).stat.approxQuantile(inputColumns, Array(0.5), $(relativeError))
          .map { array =>
            if (array.isEmpty) {
              Double.NaN
            } else {
              array.head
            }
          }
    }

    val emptyCols = inputColumns.zip(results).filter(_._2.isNaN).map(_._1)
    if (emptyCols.nonEmpty) {
      throw new SparkException(s"surrogate cannot be computed. " +
        s"All the values in ${emptyCols.mkString(",")} are Null, Nan or " +
        s"missingValue(${$(missingValue)})")
    }

    val rows = spark.sparkContext.parallelize(Seq(Row.fromSeq(results)))
    val schema = StructType(inputColumns.map(col => StructField(col, DoubleType, nullable = false)))
    val surrogateDF = spark.createDataFrame(rows, schema)
    copyValues(new ImputerModel(uid, surrogateDF).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): Imputer = defaultCopy(extra)
}

@Since("2.2.0")
object Imputer extends DefaultParamsReadable[Imputer] {

  /** strategy names that Imputer currently supports. */
  private[feature] val mean = "mean"
  private[feature] val median = "median"

  @Since("2.2.0")
  override def load(path: String): Imputer = super.load(path)
}

/**
 * Model fitted by [[Imputer]].
 *
 * @param surrogateDF a DataFrame containing inputCols and their corresponding surrogates,
 *                    which are used to replace the missing values in the input DataFrame.
 */
@Since("2.2.0")
class ImputerModel private[ml] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") val surrogateDF: DataFrame)
  extends Model[ImputerModel] with ImputerParams with MLWritable {

  import ImputerModel._

  /** @group setParam */
  @Since("3.0.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("3.0.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val (inputColumns, outputColumns) = getInOutCols
    val surrogates = surrogateDF.select(inputColumns.map(col): _*).head().toSeq


    val newCols = inputColumns.zip(outputColumns).zip(surrogates).map {
      case ((inputCol, outputCol), surrogate) =>
        val inputType = dataset.schema(inputCol).dataType
        val ic = col(inputCol).cast(DoubleType)
        when(ic.isNull, surrogate)
          .when(ic === $(missingValue), surrogate)
          .otherwise(ic)
          .cast(inputType)
    }
    dataset.withColumns(outputColumns, newCols).toDF()
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): ImputerModel = {
    val copied = new ImputerModel(uid, surrogateDF)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new ImputerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"ImputerModel: uid=$uid, strategy=${$(strategy)}, missingValue=${$(missingValue)}" +
      get(inputCols).map(c => s", numInputCols=${c.length}").getOrElse("") +
      get(outputCols).map(c => s", numOutputCols=${c.length}").getOrElse("")
  }
}


@Since("2.2.0")
object ImputerModel extends MLReadable[ImputerModel] {

  private[ImputerModel] class ImputerModelWriter(instance: ImputerModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.surrogateDF.repartition(1).write.parquet(dataPath)
    }
  }

  private class ImputerReader extends MLReader[ImputerModel] {

    private val className = classOf[ImputerModel].getName

    override def load(path: String): ImputerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val surrogateDF = sqlContext.read.parquet(dataPath)
      val model = new ImputerModel(metadata.uid, surrogateDF)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("2.2.0")
  override def read: MLReader[ImputerModel] = new ImputerReader

  @Since("2.2.0")
  override def load(path: String): ImputerModel = super.load(path)
}
