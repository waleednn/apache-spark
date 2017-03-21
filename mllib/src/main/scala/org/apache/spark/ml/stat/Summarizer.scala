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

package org.apache.spark.ml.stat

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import breeze.numerics

import org.apache.spark.SparkException
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg.{BLAS, SQLDataTypes, Vector, Vectors}
import org.apache.spark.ml.stat.SummaryBuilderImpl.MetricsAggregate
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnsafeArrayData, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Final, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._

// scalastyle:off println

/**
 * A builder object that provides summary statistics about a given column.
 *
 * Users should not directly create such builders, but instead use one of the methods in
 * [[Summarizer]].
 */
@Since("2.2.0")
abstract class SummaryBuilder {
  /**
   * Returns an aggregate object that contains the summary of the column with the requested metrics.
   * @param column a column that contains Vector object.
   * @return an aggregate column that contains the statistics. The exact content of this
   *         structure is determined during the creation of the builder.
   */
  @Since("2.2.0")
  def summary(column: Column): Column
}

/**
 * Tools for vectorized statistics on MLlib Vectors.
 *
 * The methods in this package provide various statistics for Vectors contained inside DataFrames.
 *
 * This class lets users pick the statistics they would like to extract for a given column. Here is
 * an example in Scala:
 * {{{
 *   val dataframe = ... // Some dataframe containing a feature column
 *   val allStats = dataframe.select(Summarizer.metrics("min", "max").summary($"features"))
 *   val Row(min_, max_) = allStats.first()
 * }}}
 *
 * If one wants to get a single metric, shortcuts are also available:
 * {{{
 *   val meanDF = dataframe.select(Summarizer.mean($"features"))
 *   val Row(mean_) = meanDF.first()
 * }}}
 */
@Since("2.2.0")
object Summarizer extends Logging {

  import SummaryBuilderImpl._

  /**
   * Given a list of metrics, provides a builder that it turns computes metrics from a column.
   *
   * See the documentation of [[Summarizer]] for an example.
   *
   * The following metrics are accepted (case sensitive):
   *  - mean: a vector that contains the coefficient-wise mean.
   *  - variance: a vector tha contains the coefficient-wise variance.
   *  - count: the count of all vectors seen.
   *  - numNonzeros: a vector with the number of non-zeros for each coefficients
   *  - max: the maximum for each coefficient.
   *  - min: the minimum for each coefficient.
   *  - normL2: the Euclidian norm for each coefficient.
   *  - normL1: the L1 norm of each coefficient (sum of the absolute values).
   * @param firstMetric the metric being provided
   * @param metrics additional metrics that can be provided.
   * @return a builder.
   * @throws IllegalArgumentException if one of the metric names is not understood.
   */
  @Since("2.2.0")
  def metrics(firstMetric: String, metrics: String*): SummaryBuilder = {
    val (typedMetrics, computeMetrics) = getRelevantMetrics(Seq(firstMetric) ++ metrics)
    new SummaryBuilderImpl(typedMetrics, computeMetrics)
  }

  def mean(col: Column): Column = metrics("mean").summary(col)
}

private[ml] class SummaryBuilderImpl(
    requestedMetrics: Seq[SummaryBuilderImpl.Metrics],
    requestedCompMetrics: Seq[SummaryBuilderImpl.ComputeMetrics]) extends SummaryBuilder {

  override def summary(column: Column): Column = {
    val start = SummaryBuilderImpl.Buffer.fromMetrics(requestedCompMetrics)
    val agg = SummaryBuilderImpl.MetricsAggregate(
      requestedMetrics,
      start,
      column.expr,
      mutableAggBufferOffset = 0,
      inputAggBufferOffset = 0)
    new Column(AggregateExpression(agg, mode = Complete, isDistinct = false))
  }
}

private[ml]
object SummaryBuilderImpl extends Logging {

  def implementedMetrics: Seq[String] = allMetrics.map(_._1).sorted

  @throws[IllegalArgumentException]("When the list is empty or not a subset of known metrics")
  def getRelevantMetrics(requested: Seq[String]): (Seq[Metrics], Seq[ComputeMetrics]) = {
    val all = requested.map { req =>
      val (_, metric, deps) = allMetrics.find(tup => tup._1 == req).getOrElse {
        throw new IllegalArgumentException(s"Metric $req cannot be found." +
          s" Valid metris are $implementedMetrics")
      }
      metric -> deps
    }
    // Do not sort, otherwise the user has to look the schema to see the order that it
    // is going to be given in.
    val metrics = all.map(_._1)
    val computeMetrics = all.flatMap(_._2).distinct.sortBy(_.toString)
    metrics -> computeMetrics
  }

  def structureForMetrics(metrics: Seq[Metrics]): StructType = {
    val fields = metrics.map { m =>
      // All types are vectors, except for one.
//      val tpe = if (m == Count) { LongType } else { SQLDataTypes.VectorType }
      val tpe = if (m == Count) { LongType } else { ArrayType(DoubleType, containsNull = false) }
      // We know the metric is part of the list.
      val (name, _, _) = allMetrics.find(tup => tup._2 == m).get
      StructField(name, tpe, nullable = false)
    }
    StructType(fields)
  }


  /**
   * All the metrics that can be currently computed by Spark for vectors.
   *
   * This list associates the user name, the internal (typed) name, and the list of computation
   * metrics that need to de computed internally to get the final result.
   */
  private val allMetrics = Seq(
    ("mean", Mean, Seq(ComputeMean, ComputeWeightSum, ComputeTotalWeightSum)), // Vector
    ("variance", Variance, Seq(ComputeTotalWeightSum, ComputeWeightSquareSum, ComputeMean,
                               ComputeM2n)), // Vector
    ("count", Count, Seq(ComputeCount)), // Long
    ("numNonZeros", NumNonZeros, Seq(ComputeNNZ)), // Vector
    ("max", Max, Seq(ComputeMax, ComputeTotalWeightSum, ComputeNNZ, ComputeCount)), // Vector
    ("min", Min, Seq(ComputeMin, ComputeTotalWeightSum, ComputeNNZ, ComputeCount)), // Vector
    ("normL2", NormL2, Seq(ComputeTotalWeightSum, ComputeM2)), // Vector
    ("normL1", Min, Seq(ComputeTotalWeightSum, ComputeL1)) // Vector
  )

  /**
   * The metrics that are currently implemented.
   */
  sealed trait Metrics
  case object Mean extends Metrics
  case object Variance extends Metrics
  case object Count extends Metrics
  case object NumNonZeros extends Metrics
  case object Max extends Metrics
  case object Min extends Metrics
  case object NormL2 extends Metrics
  case object NormL1 extends Metrics

  /**
   * The running metrics that are going to be computed.
   *
   * There is a bipartite graph between the metrics and the computed metrics.
   */
  sealed trait ComputeMetrics
  case object ComputeMean extends ComputeMetrics
  case object ComputeM2n extends ComputeMetrics
  case object ComputeM2 extends ComputeMetrics
  case object ComputeL1 extends ComputeMetrics
  case object ComputeCount extends ComputeMetrics // Always computed
  case object ComputeTotalWeightSum extends ComputeMetrics // Always computed
  case object ComputeWeightSquareSum extends ComputeMetrics
  case object ComputeWeightSum extends ComputeMetrics
  case object ComputeNNZ extends ComputeMetrics
  case object ComputeMax extends ComputeMetrics
  case object ComputeMin extends ComputeMetrics

  /**
   * The buffer that contains all the summary statistics. If the value is null, it is considered
   * to be not required.
   *
   * If it is required but the size of the vectors (n) is not yet know, it is initialized to
   * an empty array.
   */
  case class Buffer private (
    var n: Int = -1,                          // 0
    var mean: Array[Double] = null,           // 1
    var m2n: Array[Double] = null,            // 2
    var m2: Array[Double] = null,             // 3
    var l1: Array[Double] = null,             // 4
    var totalCount: Long = 0,                 // 5
    var totalWeightSum: Double = 0.0,         // 6
    var totalWeightSquareSum: Double = 0.0,   // 7
    var weightSum: Array[Double] = null,      // 8
    var nnz: Array[Long] = null,              // 9
    var max: Array[Double] = null,            // 10
    var min: Array[Double] = null)            // 11

  object Buffer {
    // Recursive function, but the number of cases is really small.
    def fromMetrics(requested: Seq[ComputeMetrics]): Buffer = {
      if (requested.isEmpty) {
        new Buffer()
      } else {
        val b = fromMetrics(requested.tail)
        requested.head match {
          case ComputeMean => b.copy(mean = Array.empty)
          case ComputeM2n => b.copy(m2n = Array.empty)
          case ComputeM2 => b.copy(m2 = Array.empty)
          case ComputeL1 => b.copy(l1 = Array.empty)
          case ComputeWeightSum => b.copy(weightSum = Array.empty)
          case ComputeNNZ => b.copy(nnz = Array.empty)
          case ComputeMax => b.copy(max = Array.empty)
          case ComputeMin => b.copy(min = Array.empty)
          case _ => b // These cases are already being computed
        }
      }
    }

    def bufferSchema: StructType = {
      val at = ArrayType(DoubleType, containsNull = false)
      val al = ArrayType(LongType, containsNull = false)
      val fields = Seq(
        "n" -> IntegerType,
        "mean" -> at,
        "m2n" -> at,
        "m2" -> at,
        "l1" -> at,
        "totalCount" -> LongType,
        "totalWeightSum" -> DoubleType,
        "totalWeightSquareSum" -> DoubleType,
        "weightSum" -> at,
        "nnz" -> al,
        "max" -> at,
        "min" -> at
      )
      StructType(fields.map { case (name, t) => StructField(name, t, nullable = true)})
    }

    def updateInPlace(buffer: Buffer, v: Vector, w: Double): Unit = {
      val startN = buffer.n
      if (startN == -1) {
        buffer.n = v.size
      } else {
        require(startN == v.size,
          s"Trying to insert a vector of size $v into a buffer that " +
            s"has been sized with $startN")
      }
      val n = buffer.n
      assert(n > 0, n)
      // Always update the following fields.
      buffer.totalWeightSum += w
      buffer.totalCount += 1
      buffer.totalWeightSquareSum += w * w
      // All the fields that we compute on demand:
      // TODO: the most common case is dense vectors. In that case we should
      // directly use BLAS instructions instead of iterating through a scala iterator.
      v.foreachActive { (index, value) =>
        if (value != 0.0) {
          if (buffer.max != null && buffer.max(index) < value) {
            buffer.max(index) = value
          }
          if (buffer.min != null && buffer.min(index) > value) {
            buffer.min(index) = value
          }

          if (buffer.mean != null) {
            val prevMean = buffer.mean(index)
            val diff = value - prevMean
            buffer.mean(index) += w * diff / (buffer.weightSum(index) + w)
            if (buffer.m2n != null) {
              buffer.m2n(index) += w * (value - buffer.mean(index)) * diff
            }
          }
          if (buffer.m2 != null) {
            buffer.m2(index) += w * value * value
          }
          if (buffer.l1 != null) {
            buffer.l1(index) += w * math.abs(value)
          }
          if (buffer.weightSum != null) {
            buffer.weightSum(index) += w
          }
          if (buffer.nnz != null) {
            buffer.nnz(index) += 1
          }
        }
      }
    }

    /**
     * Updates 'buffer' with the content of 'other', and returns 'buffer'.
     */
    @throws[SparkException]("When the buffers are not compatible")
    def mergeBuffers(buffer: Buffer, other: Buffer): Buffer = {
      if (buffer.n == -1) {
        // buffer is not initialized.
        if (other.n == -1) {
          // Both are not initialized.
          buffer
        } else {
          // other is initialized
          other
        }
      } else {
        // Buffer is initialized.
        if (other.n == -1) {
          buffer
        } else {
          mergeBuffers0(buffer, other)
          buffer
        }
      }
    }

    /**
     * Reads a buffer from a serialized form, using the row object as an assistant.
     */
    def read(bytes: Array[Byte], row: UnsafeRow): Buffer = {
      row.pointTo(bytes, bytes.length)
      new Buffer(
        n = row.getInt(0),
        mean = nullableArrayD(row, 1),
        m2n = nullableArrayD(row, 2),
        m2 = nullableArrayD(row, 3),
        l1 = nullableArrayD(row, 4),
        totalCount = row.getLong(5),
        totalWeightSum = row.getDouble(6),
        totalWeightSquareSum = row.getDouble(7),
        weightSum = nullableArrayD(row, 8),
        nnz = nullableArrayL(row, 9),
        max = nullableArrayD(row, 10),
        min = nullableArrayD(row, 11)
      )
    }


    def write(buffer: Buffer): Array[Byte] = {
      val ir = InternalRow.apply(
        buffer.n,
        gadD(buffer.mean),
        gadD(buffer.m2n),
        gadD(buffer.m2),
        gadD(buffer.l1),
        buffer.totalCount,
        buffer.totalWeightSum,
        buffer.totalWeightSquareSum,
        gadD(buffer.weightSum),
        gadL(buffer.nnz),
        gadD(buffer.max),
        gadD(buffer.min)
      )
      projection.apply(ir).getBytes
    }

    def mean(buffer: Buffer): Array[Double] = {
      require(buffer.totalWeightSum > 0)
      require(buffer.mean != null)
      require(buffer.weightSum != null)
      val res = b(buffer.mean) :* b(buffer.weightSum) :/ buffer.totalWeightSum
      res.toArray
    }

    def variance(buffer: Buffer): Array[Double] = {
      import buffer._
      require(n >= 0)
      require(totalWeightSum > 0)
      require(totalWeightSquareSum > 0)
      require(buffer.mean != null)
      require(m2n != null)
      require(weightSum != null)
      val denom = totalWeightSum - (totalWeightSquareSum / totalWeightSum)
      if (denom > 0.0) {
        val normWs = b(weightSum) :/ totalWeightSum
        val x = b(buffer.mean) :* b(buffer.mean) :* b(weightSum) :* (- normWs :+ 1.0)
        val res = (b(m2n) :+ x) :/ denom
        res.toArray
      } else {
        Array.ofDim(n) // Return 0.0 instead.
      }
    }

    def totalCount(buffer: Buffer): Long = buffer.totalCount

    def nnz(buffer: Buffer): Array[Long] = {
      require(buffer.nnz != null)
      buffer.nnz
    }

    def max(buffer: Buffer): Array[Double] = {
      require(buffer.max != null)
      buffer.max
    }

    def min(buffer: Buffer): Array[Double] = {
      require(buffer.min != null)
      buffer.min
    }

    def l2(buffer: Buffer): Array[Double] = {
      import buffer._
      require(totalWeightSum > 0.0)
      require(m2 != null)
      numerics.sqrt(b(m2)).toArray
    }

    def l1(buffer: Buffer): Array[Double] = {
      require(buffer.l1 != null)
      buffer.l1
    }

    private def gadD(arr: Array[Double]): UnsafeArrayData = {
      if (arr == null) {
        null
      } else {
        UnsafeArrayData.fromPrimitiveArray(arr)
      }
    }

    private def gadL(arr: Array[Long]): UnsafeArrayData = {
      if (arr == null) {
        null
      } else {
        UnsafeArrayData.fromPrimitiveArray(arr)
      }
    }

    private[this] lazy val projection = UnsafeProjection.create(bufferSchema)

    // Returns the array at a given index, or null if the array is null.
    private def nullableArrayD(row: UnsafeRow, ordinal: Int): Array[Double] = {
      if (row.isNullAt(ordinal)) {
        null
      } else {
        row.getArray(ordinal).toDoubleArray
      }
    }

    // Returns the array at a given index, or null if the array is null.
    private def nullableArrayL(row: UnsafeRow, ordinal: Int): Array[Long] = {
      if (row.isNullAt(ordinal)) {
        null
      } else {
        row.getArray(ordinal).toLongArray
      }
    }

    private def b(x: Array[Double]): BV[Double] = Vectors.dense(x).asBreeze

    private def bl(x: Array[Long]): BV[Long] = BV.apply(x)

    private def maxInPlace(x: Array[Double], y: Array[Double]): Unit = {
      var i = 0
      while(i < x.length) {
        // Note: do not use conditions, it is wrong when handling NaNs.
        x(i) = Math.max(x(i), y(i))
        i += 1
      }
    }

    private def minInPlace(x: Array[Double], y: Array[Double]): Unit = {
      var i = 0
      while(i < x.length) {
        // Note: do not use conditions, it is wrong when handling NaNs.
        x(i) = Math.min(x(i), y(i))
        i += 1
      }
    }


    /**
     * Merges other into buffer.
     */
    private def mergeBuffers0(buffer: Buffer, other: Buffer): Unit = {
      // Each buffer needs to be properly initialized.
      require(buffer.n > 0 && other.n > 0, (buffer.n, other.n))
      require(buffer.n == other.n, (buffer.n, other.n))
      // Mandatory scalar values
      buffer.totalWeightSquareSum += other.totalWeightSquareSum
      buffer.totalWeightSum += other.totalWeightSum
      buffer.totalCount += buffer.totalCount
      // Keep the original weight sums.
      val weightSum1 = if (buffer.weightSum == null) null else { buffer.weightSum.clone() }
      val weightSum2 = if (other.weightSum == null) null else { other.weightSum.clone() }

      val weightSum = if (weightSum1 == null) null else {
        require(weightSum2 != null)
        val arr: Array[Double] = Array.ofDim(buffer.n)
        b(arr) :+= b(weightSum1) :- b(weightSum1)
        arr
      }


      // Since the operations are dense, we can directly use BLAS calls here.
      val deltaMean: Array[Double] = if (buffer.mean != null) {
        require(other.mean != null)
        val arr: Array[Double] = Array.ofDim(buffer.n)
        b(arr) :+= b(other.mean) :- b(buffer.mean)
        arr
      } else { null }

      if (buffer.mean != null) {
        require(other.mean != null)
        require(weightSum != null)
        b(buffer.mean) :+= b(deltaMean) :* (b(weightSum2) / b(weightSum))
      }

      if (buffer.m2n != null) {
        require(other.m2n != null)
        val w = (b(weightSum1) :* b(weightSum2)) :/ b(weightSum)
        b(buffer.m2n) :+= b(other.m2n) :+ (b(deltaMean) :* b(deltaMean)) :* w
      }

      if (buffer.m2 != null) {
        require(other.m2 != null)
        b(buffer.m2) :+= b(other.m2)
      }

      if (buffer.l1 != null) {
        require(other.l1 != null)
        b(buffer.l1) :+= b(other.l1)
      }

      if (buffer.max != null) {
        require(other.max != null)
        maxInPlace(buffer.max, other.max)
      }

      if (buffer.min != null) {
        require(other.min != null)
        minInPlace(buffer.min, other.min)
      }

      if (buffer.nnz != null) {
        require(other.nnz != null)
        bl(buffer.nnz) :+= bl(other.nnz)
      }

      if (buffer.weightSum != null) {
        require(other.weightSum != null)
        b(buffer.weightSum) :+= b(other.weightSum)
      }
    }
  }

  private case class MetricsAggregate(
      requested: Seq[Metrics],
      startBuffer: Buffer,
      child: Expression,
      mutableAggBufferOffset: Int,
      inputAggBufferOffset: Int)
    extends TypedImperativeAggregate[Buffer] {

    private lazy val row = new UnsafeRow(1)

    override def eval(buff: Buffer): Row = {
      val metrics = requested.map({
        case Mean => Buffer.mean(buff)
        case Variance => Buffer.variance(buff)
        case Count => Buffer.totalCount(buff)
        case NumNonZeros => Buffer.nnz(buff)
        case Max => Buffer.max(buff)
        case Min => Buffer.min(buff)
        case NormL2 => Buffer.l2(buff)
        case NormL1 => Buffer.l1(buff)
      })
      Row(metrics: _*)
    }

    override def children: Seq[Expression] = child :: Nil

    override def update(buff: Buffer, row: InternalRow): Buffer = {
      val v = row.get(0, SQLDataTypes.VectorType).asInstanceOf[Vector]

      val w = row.numFields match {
        case 1 => 1.0
        case 2 => row.getDouble(1)
        case x => throw new SparkException(s"Expected 1 or 2 fields, got $x fields.")
      }
      Buffer.updateInPlace(buff, v, w)
      buff
    }

    override def merge(buff: Buffer, other: Buffer): Buffer = {
      Buffer.mergeBuffers(buff, other)
    }

    override def nullable: Boolean = false

    // Make a copy of the start buffer so that the current aggregator can be safely copied around.
    override def createAggregationBuffer(): Buffer = startBuffer.copy()

    override def serialize(buff: Buffer): Array[Byte] = {
      Buffer.write(buff)
    }

    override def deserialize(bytes: Array[Byte]): Buffer = {
      Buffer.read(bytes, row)
    }

    override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): MetricsAggregate = {
      copy(mutableAggBufferOffset = newMutableAggBufferOffset)
    }

    override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): MetricsAggregate = {
      copy(inputAggBufferOffset = newInputAggBufferOffset)
    }

    override lazy val dataType: DataType = structureForMetrics(requested)

    override def prettyName: String = "aggregate_metrics"
  }

}
