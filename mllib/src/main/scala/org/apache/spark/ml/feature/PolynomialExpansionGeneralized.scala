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
import scala.collection.mutable
import org.apache.commons.math3.util.CombinatoricsUtils
import org.apache.spark.annotation.Since
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.DataType

/**
  * Class created starting from PolynomialExpansion
  *
  * Perform feature expansion in a polynomial space. As said in wikipedia of Polynomial Expansion,
  * which is available at
  * <a href="http://en.wikipedia.org/wiki/Polynomial_expansion">Polynomial expansion (Wikipedia)</a>
  * , "In mathematics, an expansion of a product of sums expresses it as a sum of products by using
  * the fact that multiplication distributes over addition". Take a 2-variable feature vector
  * as an example: `(x, y)`, if we want to expand it with degree 2, then we get
  * `(x, x * x, y, x * y, y * y)`.
  *
  * To this was added :
  * Polynomial Root expansion :
  * For example, for Vector (x, y) and degree 3 we obtain : (x^(1/2), x^(1/3), y^(1/2), y^(1/3))
  * The parsing is done with java.lang.Math.pow function, which does not allow negative basis when fractional power
  *
  * An exception is raised:
  *   If the first argument is finite and less than zero
  *   and if the second argument is finite and not an integer,
  *   (then the result is NaN)
  * according to the documentation :
  * <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/Math.html#pow-double-double-></a>

  */

@Since("1.4.0")
class PolynomialExpansionGeneralized @Since("1.4.0")(@Since("1.4.0") override val uid: String)
  extends UnaryTransformer[Vector, Vector, PolynomialExpansionGeneralized] with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("poly"))

  /**
    * The polynomial degree to expand, which should be greater than equal to 1. A value of 1 means
    * no expansion.
    * Default: 2
    * @group param
    */

  @Since("1.4.0")
  val degree = new IntParam(this, "degree", "the polynomial degree to expand (>= 1)",
    ParamValidators.gtEq(1))

  setDefault(degree -> 1)

  /** @group getParam */
  @Since("1.4.0")
  def getDegree: Int = $(degree)

  /** @group setParam */
  @Since("2.4.0")
  def setDegree(value: Int): this.type = set(degree, value)

  /*
    Fraction degree (radical of degree n)
   */
  val degreeRoot = new IntParam(this, "degreeRoot", "the polynomial root degree to expand (>= 1)",
    ParamValidators.gtEq(1))

  setDefault(degreeRoot -> 1)

  /** @group getParam */
  @Since("2.4.0")
  def getDegreeRoot: Int = $(degreeRoot)

  /** @group setParam */
  @Since("2.4.0")
  def setDegreeForRoot(value: Int): this.type = set(degreeRoot, value)

  override protected def createTransformFunc: Vector => Vector = { v =>
    PolynomialExpansionGeneralized.expand(v, $(degree), $(degreeRoot))
  }

  override protected def outputDataType: DataType = new VectorUDT()

  //@Since("2.4.0" PolynomialExpansionGeneralizedWithRoot)
  override def copy(extra: ParamMap): PolynomialExpansionGeneralized = defaultCopy(extra)
}

@Since("2.4.0")
object PolynomialExpansionGeneralized extends DefaultParamsReadable[PolynomialExpansionGeneralized] {
  @Since("1.6.0")
  private def getPolySize(numFeatures: Int, degree: Int): Int = {
    val n = CombinatoricsUtils.binomialCoefficient(numFeatures + degree, degree)
    require(n <= Integer.MAX_VALUE)
    n.toInt
  }

  /** Exponential degree: pow(X,n)
    */
  @Since("1.6.0")
  private def expandDense(
                           values: Array[Double],
                           lastIdx: Int,
                           degree: Int,
                           multiplier: Double,
                           polyValues: Array[Double],
                           curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyValues(curPolyIdx) = multiplier
      }
    } else {
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      var alpha = multiplier
      var i = 0
      var curStart = curPolyIdx
      while (i <= degree && alpha != 0.0) {
        curStart = expandDense(values, lastIdx1, degree - i, alpha, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastIdx + 1, degree)
  }

  @Since("1.6.0")
  private def expandSparse(
                            indices: Array[Int],
                            values: Array[Double],
                            lastIdx: Int,
                            lastFeatureIdx: Int,
                            degree: Int,
                            multiplier: Double,
                            polyIndices: mutable.ArrayBuilder[Int],
                            polyValues: mutable.ArrayBuilder[Double],
                            curPolyIdx: Int): Int = {
    if (multiplier == 0.0) {
      // do nothing
    } else if (degree == 0 || lastIdx < 0) {
      if (curPolyIdx >= 0) { // skip the very first 1
        polyIndices += curPolyIdx
        polyValues += multiplier
      }
    } else {
      // Skip all zeros at the tail.
      val v = values(lastIdx)
      val lastIdx1 = lastIdx - 1
      val lastFeatureIdx1 = indices(lastIdx) - 1
      var alpha = multiplier
      var curStart = curPolyIdx
      var i = 0
      while (i <= degree && alpha != 0.0) {
        curStart = expandSparse(indices, values, lastIdx1, lastFeatureIdx1, degree - i, alpha,
          polyIndices, polyValues, curStart)
        i += 1
        alpha *= v
      }
    }
    curPolyIdx + getPolySize(lastFeatureIdx + 1, degree)
  }


  /** Root degree : pow(X,1/n) */
  @Since("2.4.0")
  def expandDenseRoot(values: Array[Double], degreeRoot: Int): Array[Double] = {
    val n = values.length
    val polyValues = new Array[Double](n * (degreeRoot-1))
    var j:Int = 0
    for (v <- values) {
      var i:Int = 0
      while (i<degreeRoot-1){
        val resVal = Math.pow(v,1.toFloat/(degreeRoot-i))
        if (resVal.isNaN) {
          polyValues(j*(degreeRoot-1) + i) = 0.0
        } else {
          polyValues(j*(degreeRoot-1) + i) = resVal
        }
        i = i+1
      }
      j = j+1
    }

    polyValues
  }

  @Since("2.4.0")
  def expandSparseRoot(values: Array[Double], degreeRoot: Int): (Array[Double],Array[Int]) = {
    val n = values.length
    val polyValues = new Array[Double](n * (degreeRoot-1))
    var curPolyIdx:Int = 0
    val indices = new Array[Int](n * (degreeRoot-1))
    var count = 0
    var j:Int = 0
    for (v <- values) {
      var i:Int = 0
      while (i < degreeRoot-1){
        curPolyIdx = j*(degreeRoot-1) + i
        curPolyIdx = count
        indices(curPolyIdx) = curPolyIdx
        val resVal = Math.pow(v,1.toFloat/(degreeRoot-i))
        if (resVal.isNaN) {
          polyValues(j*(degreeRoot-1) + i) = 0.0
        } else {
          polyValues(j*(degreeRoot-1) + i) = resVal
        }
        i = i+1
        count = count+1
      }
      j = j+1
    }

    (polyValues,indices)
  }

  @Since("2.4.0")
  private def expand(sv: SparseVector, degree: Int, degreeRoot:Int): SparseVector = {
    val polySize = getPolySize(sv.size, degree)
    val nnz = sv.values.length
    val nnzPolySize = getPolySize(nnz, degree)
    val polyIndices = mutable.ArrayBuilder.make[Int]
    polyIndices.sizeHint(nnzPolySize - 1)
    val polyValues = mutable.ArrayBuilder.make[Double]
    polyValues.sizeHint(nnzPolySize - 1)

    // Simple exponent : x ^ exp
    expandSparse(
      sv.indices, sv.values, nnz - 1, sv.size - 1, degree, 1.0, polyIndices, polyValues, -1)

    // Root exponent (fractional) : x ^ (1/exp)
    val npv = expandSparseRoot(sv.values,degreeRoot)
    val indicesRoot = npv._2
    val polyValueRoot = npv._1

    for (i <- polySize until polySize+indicesRoot.length){
      polyIndices += i
    }
    new SparseVector(polySize - 1 + indicesRoot.length, polyIndices.result(), polyValues.result() ++ polyValueRoot)
  }

  @Since("2.4.0")
  private def expand(dv: DenseVector, degree: Int, degreeRoot:Int): DenseVector = {
    val n = dv.size
    val polySize = getPolySize(n, degree)
    val polyValues = new Array[Double](polySize - 1)
    val polyValuesNeg = new Array[Double](polySize - 1)

    expandDense(dv.values, n - 1, degree, 1.0, polyValues, -1)

    // Root :
    val newPolyValues:Array[Double] = expandDenseRoot(dv.values, degreeRoot)
    new DenseVector(polyValues ++ newPolyValues ++ polyValuesNeg)

  }

  @Since("2.4.0")
  private[feature] def expand(v: Vector, degree: Int, degreeRoot:Int): Vector = {
    v match {
      case `v` if v.toArray.exists(_<0) => throw new IllegalArgumentException("Cannot extract root from  negative basis !")
      case dv: DenseVector => expand(dv, degree, degreeRoot)
      case sv: SparseVector => expand(sv, degree, degreeRoot)
      case _ => throw new IllegalArgumentException
    }
  }

  @Since("1.6.0")
  override def load(path: String): PolynomialExpansionGeneralized = super.load(path)
}

