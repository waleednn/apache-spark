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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.Serializable
import java.nio.ByteBuffer

import com.google.common.io.BaseEncoding
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate._
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.OriginalType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.sources
import org.apache.spark.sql.types._


private[sql] object ParquetFilters {
  val PARQUET_FILTER_DATA = "org.apache.spark.sql.parquet.row.filter"

  case class ParquetEqUDP[T <: Comparable[T], U <: Comparable[U]](udf: AnyRef, v: U)
      extends UserDefinedPredicate[T] with Serializable {
    private val f = udf.asInstanceOf[(T) => U]

    override def keep(value: T): Boolean = {
      f(value) == v
    }

    override def canDrop(statistics: Statistics[T]): Boolean = false
    override def inverseCanDrop(statistics: Statistics[T]): Boolean = false
  }

  case class SetInFilter[T <: Comparable[T]](
    valueSet: Set[T]) extends UserDefinedPredicate[T] with Serializable {

    override def keep(value: T): Boolean = {
      value != null && valueSet.contains(value)
    }

    override def canDrop(statistics: Statistics[T]): Boolean = false

    override def inverseCanDrop(statistics: Statistics[T]): Boolean = false
  }

  private val makeEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case BooleanType =>
      (n: String, v: Any) => FilterApi.eq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case IntegerType =>
      (n: String, v: Any) => FilterApi.eq(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.eq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.eq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.eq(doubleColumn(n), v.asInstanceOf[java.lang.Double])

    // Binary.fromString and Binary.fromByteArray don't accept null values
    case StringType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8"))).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.eq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeUDFEq: PartialFunction[DataType, (String, ScalaUDF, Any) => FilterPredicate] = {
    case BooleanType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[java.lang.Boolean, java.lang.Boolean](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[java.lang.Boolean, java.lang.Integer](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[java.lang.Boolean, java.lang.Long](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[java.lang.Boolean, java.lang.Float](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[java.lang.Boolean, java.lang.Double](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[java.lang.Boolean, java.lang.String](udf.exportedFunc(),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(booleanColumn(n), udp)

    case IntegerType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[java.lang.Integer, java.lang.Boolean](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[java.lang.Integer, java.lang.Integer](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[java.lang.Integer, java.lang.Long](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[java.lang.Integer, java.lang.Float](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[java.lang.Integer, java.lang.Double](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[java.lang.Integer, java.lang.String](udf.exportedFunc(),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(intColumn(n), udp)

    case LongType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[java.lang.Long, java.lang.Boolean](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[java.lang.Long, java.lang.Integer](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[java.lang.Long, java.lang.Long](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[java.lang.Long, java.lang.Float](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[java.lang.Long, java.lang.Double](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[java.lang.Long, java.lang.String](udf.exportedFunc(),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(longColumn(n), udp)

    case FloatType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[java.lang.Float, java.lang.Boolean](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[java.lang.Float, java.lang.Integer](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[java.lang.Float, java.lang.Long](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[java.lang.Float, java.lang.Float](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[java.lang.Float, java.lang.Double](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[java.lang.Float, java.lang.String](udf.exportedFunc(),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(floatColumn(n), udp)

    case DoubleType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[java.lang.Double, java.lang.Boolean](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[java.lang.Double, java.lang.Integer](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[java.lang.Double, java.lang.Long](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[java.lang.Double, java.lang.Float](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[java.lang.Double, java.lang.Double](udf.exportedFunc(),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[java.lang.Double, java.lang.String](udf.exportedFunc(),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(doubleColumn(n), udp)

    case StringType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[Binary, java.lang.Boolean](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[Binary, java.lang.Integer](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[Binary, java.lang.Long](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[Binary, java.lang.Float](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[Binary, java.lang.Double](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[Binary, java.lang.String](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(binaryColumn(n), udp)

    case BinaryType =>
      (n: String, udf: ScalaUDF, v: Any) =>
        val udp = v match {
          case x: Boolean =>
            ParquetEqUDP[Binary, java.lang.Boolean](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Boolean])
          case x: Integer =>
            ParquetEqUDP[Binary, java.lang.Integer](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Integer])
          case x: Long =>
            ParquetEqUDP[Binary, java.lang.Long](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Long])
          case x: Float =>
            ParquetEqUDP[Binary, java.lang.Float](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Float])
          case x: Double =>
            ParquetEqUDP[Binary, java.lang.Double](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.Double])
          case x: String =>
            ParquetEqUDP[Binary, java.lang.String](udf.exportedFunc(false),
              v.asInstanceOf[java.lang.String])
        }
        FilterApi.userDefined(binaryColumn(n), udp)

  }

  private val makeNotEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case BooleanType =>
      (n: String, v: Any) => FilterApi.notEq(booleanColumn(n), v.asInstanceOf[java.lang.Boolean])
    case IntegerType =>
      (n: String, v: Any) => FilterApi.notEq(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.notEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.notEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.notEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8"))).orNull)
    case BinaryType =>
      (n: String, v: Any) => FilterApi.notEq(
        binaryColumn(n),
        Option(v).map(b => Binary.fromByteArray(v.asInstanceOf[Array[Byte]])).orNull)
  }

  private val makeLt: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.lt(intColumn(n), v.asInstanceOf[Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.lt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.lt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.lt(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.lt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeLtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.ltEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.ltEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.ltEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.ltEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.ltEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeGt: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gt(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gt(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gt(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gt(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gt(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeGtEq: PartialFunction[DataType, (String, Any) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Any) => FilterApi.gtEq(intColumn(n), v.asInstanceOf[java.lang.Integer])
    case LongType =>
      (n: String, v: Any) => FilterApi.gtEq(longColumn(n), v.asInstanceOf[java.lang.Long])
    case FloatType =>
      (n: String, v: Any) => FilterApi.gtEq(floatColumn(n), v.asInstanceOf[java.lang.Float])
    case DoubleType =>
      (n: String, v: Any) => FilterApi.gtEq(doubleColumn(n), v.asInstanceOf[java.lang.Double])
    case StringType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n),
          Binary.fromByteArray(v.asInstanceOf[String].getBytes("utf-8")))
    case BinaryType =>
      (n: String, v: Any) =>
        FilterApi.gtEq(binaryColumn(n), Binary.fromByteArray(v.asInstanceOf[Array[Byte]]))
  }

  private val makeInSet: PartialFunction[DataType, (String, Set[Any]) => FilterPredicate] = {
    case IntegerType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(intColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Integer]]))
    case LongType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(longColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Long]]))
    case FloatType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(floatColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Float]]))
    case DoubleType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(doubleColumn(n), SetInFilter(v.asInstanceOf[Set[java.lang.Double]]))
    case StringType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(s => Binary.fromByteArray(s.asInstanceOf[String].getBytes("utf-8")))))
    case BinaryType =>
      (n: String, v: Set[Any]) =>
        FilterApi.userDefined(binaryColumn(n),
          SetInFilter(v.map(e => Binary.fromByteArray(e.asInstanceOf[Array[Byte]]))))
  }

  /**
   * Converts data sources filters to Parquet filter predicates.
   */
  def createFilter(schema: StructType, predicate: sources.Filter): Option[FilterPredicate] = {
    val dataTypeOf = schema.map(f => f.name -> f.dataType).toMap

    relaxParquetValidTypeMap

    // NOTE:
    //
    // For any comparison operator `cmp`, both `a cmp NULL` and `NULL cmp a` evaluate to `NULL`,
    // which can be casted to `false` implicitly. Please refer to the `eval` method of these
    // operators and the `SimplifyFilters` rule for details.

    // Hyukjin:
    // I added [[EqualNullSafe]] with [[org.apache.parquet.filter2.predicate.Operators.Eq]].
    // So, it performs equality comparison identically when given [[sources.Filter]] is [[EqualTo]].
    // The reason why I did this is, that the actual Parquet filter checks null-safe equality
    // comparison.
    // So I added this and maybe [[EqualTo]] should be changed. It still seems fine though, because
    // physical planning does not set `NULL` to [[EqualTo]] but changes it to [[IsNull]] and etc.
    // Probably I missed something and obviously this should be changed.

    predicate match {
      case sources.IsNull(name) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, null))
      case sources.IsNotNull(name) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, null))

      case sources.EqualTo(name, value) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.UDFEqualTo(name, udf, value) =>
        makeUDFEq.lift(dataTypeOf(name)).map(_(name, udf, value))

      case sources.Not(sources.EqualTo(name, value)) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.EqualNullSafe(name, value) =>
        makeEq.lift(dataTypeOf(name)).map(_(name, value))
      case sources.Not(sources.EqualNullSafe(name, value)) =>
        makeNotEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.LessThan(name, value) =>
        makeLt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.LessThanOrEqual(name, value) =>
        makeLtEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.GreaterThan(name, value) =>
        makeGt.lift(dataTypeOf(name)).map(_(name, value))
      case sources.GreaterThanOrEqual(name, value) =>
        makeGtEq.lift(dataTypeOf(name)).map(_(name, value))

      case sources.And(lhs, rhs) =>
        (createFilter(schema, lhs) ++ createFilter(schema, rhs)).reduceOption(FilterApi.and)

      case sources.Or(lhs, rhs) =>
        for {
          lhsFilter <- createFilter(schema, lhs)
          rhsFilter <- createFilter(schema, rhs)
        } yield FilterApi.or(lhsFilter, rhsFilter)

      case sources.Not(pred) =>
        createFilter(schema, pred).map(FilterApi.not)

      case _ => None
    }
  }

  // !! HACK ALERT !!
  //
  // This lazy val is a workaround for PARQUET-201, and should be removed once we upgrade to
  // parquet-mr 1.8.1 or higher versions.
  //
  // In Parquet, not all types of columns can be used for filter push-down optimization.  The set
  // of valid column types is controlled by `ValidTypeMap`.  Unfortunately, in parquet-mr 1.7.0 and
  // prior versions, the limitation is too strict, and doesn't allow `BINARY (ENUM)` columns to be
  // pushed down.
  //
  // This restriction is problematic for Spark SQL, because Spark SQL doesn't have a type that maps
  // to Parquet original type `ENUM` directly, and always converts `ENUM` to `StringType`.  Thus,
  // a predicate involving a `ENUM` field can be pushed-down as a string column, which is perfectly
  // legal except that it fails the `ValidTypeMap` check.
  //
  // Here we add `BINARY (ENUM)` into `ValidTypeMap` lazily via reflection to workaround this issue.
  private lazy val relaxParquetValidTypeMap: Unit = {
    val constructor = Class
      .forName(classOf[ValidTypeMap].getCanonicalName + "$FullTypeDescriptor")
      .getDeclaredConstructor(classOf[PrimitiveTypeName], classOf[OriginalType])

    constructor.setAccessible(true)
    val enumTypeDescriptor = constructor
      .newInstance(PrimitiveTypeName.BINARY, OriginalType.ENUM)
      .asInstanceOf[AnyRef]

    val addMethod = classOf[ValidTypeMap].getDeclaredMethods.find(_.getName == "add").get
    addMethod.setAccessible(true)
    addMethod.invoke(null, classOf[Binary], enumTypeDescriptor)
  }

  /**
   * Note: Inside the Hadoop API we only have access to `Configuration`, not to
   * [[org.apache.spark.SparkContext]], so we cannot use broadcasts to convey
   * the actual filter predicate.
   */
  def serializeFilterExpressions(filters: Seq[Expression], conf: Configuration): Unit = {
    if (filters.nonEmpty) {
      val serialized: Array[Byte] =
        SparkEnv.get.closureSerializer.newInstance().serialize(filters).array()
      val encoded: String = BaseEncoding.base64().encode(serialized)
      conf.set(PARQUET_FILTER_DATA, encoded)
    }
  }

  /**
   * Note: Inside the Hadoop API we only have access to `Configuration`, not to
   * [[org.apache.spark.SparkContext]], so we cannot use broadcasts to convey
   * the actual filter predicate.
   */
  def deserializeFilterExpressions(conf: Configuration): Seq[Expression] = {
    val data = conf.get(PARQUET_FILTER_DATA)
    if (data != null) {
      val decoded: Array[Byte] = BaseEncoding.base64().decode(data)
      SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(decoded))
    } else {
      Seq()
    }
  }
}
