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

package org.apache.spark.mllib.linalg

import breeze.linalg.{DenseMatrix => BDM}
import org.scalatest.FunSuite

import org.apache.spark.SparkException

class VectorsSuite extends FunSuite {

  val arr = Array(0.1, 0.0, 0.3, 0.4)
  val n = 4
  val indices = Array(0, 2, 3)
  val values = Array(0.1, 0.3, 0.4)

  test("dense vector construction with varargs") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("dense vector construction from a double array") {
   val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.size === arr.length)
    assert(vec.values.eq(arr))
  }

  test("sparse vector construction") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices.eq(indices))
    assert(vec.values.eq(values))
  }

  test("sparse vector construction with unordered elements") {
    val vec = Vectors.sparse(n, indices.zip(values).reverse).asInstanceOf[SparseVector]
    assert(vec.size === n)
    assert(vec.indices === indices)
    assert(vec.values === values)
  }

  test("dense to array") {
    val vec = Vectors.dense(arr).asInstanceOf[DenseVector]
    assert(vec.toArray.eq(arr))
  }

  test("sparse to array") {
    val vec = Vectors.sparse(n, indices, values).asInstanceOf[SparseVector]
    assert(vec.toArray === arr)
  }

  test("vector equals") {
    val dv1 = Vectors.dense(arr.clone())
    val dv2 = Vectors.dense(arr.clone())
    val sv1 = Vectors.sparse(n, indices.clone(), values.clone())
    val sv2 = Vectors.sparse(n, indices.clone(), values.clone())

    val vectors = Seq(dv1, dv2, sv1, sv2)

    for (v <- vectors; u <- vectors) {
      assert(v === u)
      assert(v.## === u.##)
    }

    val another = Vectors.dense(0.1, 0.2, 0.3, 0.4)

    for (v <- vectors) {
      assert(v != another)
      assert(v.## != another.##)
    }
  }

  test("indexing dense vectors") {
    val vec = Vectors.dense(1.0, 2.0, 3.0, 4.0)
    assert(vec(0) === 1.0)
    assert(vec(3) === 4.0)
  }

  test("indexing sparse vectors") {
    val vec = Vectors.sparse(7, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec(0) === 1.0)
    assert(vec(1) === 0.0)
    assert(vec(2) === 2.0)
    assert(vec(3) === 0.0)
    assert(vec(6) === 4.0)
    val vec2 = Vectors.sparse(8, Array(0, 2, 4, 6), Array(1.0, 2.0, 3.0, 4.0))
    assert(vec2(6) === 4.0)
    assert(vec2(7) === 0.0)
  }

  test("parse vectors") {
    val vectors = Seq(
      Vectors.dense(Array.empty[Double]),
      Vectors.dense(1.0),
      Vectors.dense(1.0E6, 0.0, -2.0e-7),
      Vectors.sparse(0, Array.empty[Int], Array.empty[Double]),
      Vectors.sparse(1, Array(0), Array(1.0)),
      Vectors.sparse(3, Array(0, 2), Array(1.0, -2.0)))
    vectors.foreach { v =>
      val v1 = Vectors.parse(v.toString)
      assert(v.getClass === v1.getClass)
      assert(v === v1)
    }

    val malformatted = Seq("1", "[1,,]", "[1,2b]", "(1,[1,2])", "([1],[2.0,1.0])")
    malformatted.foreach { s =>
      intercept[SparkException] {
        Vectors.parse(s)
        println(s"Didn't detect malformatted string $s.")
      }
    }
  }

  test("zeros") {
    assert(Vectors.zeros(3) === Vectors.dense(0.0, 0.0, 0.0))
  }

  test("Vector.copy") {
    val sv = Vectors.sparse(4, Array(0, 2), Array(1.0, 2.0))
    val svCopy = sv.copy
    (sv, svCopy) match {
      case (sv: SparseVector, svCopy: SparseVector) =>
        assert(sv.size === svCopy.size)
        assert(sv.indices === svCopy.indices)
        assert(sv.values === svCopy.values)
        assert(!sv.indices.eq(svCopy.indices))
        assert(!sv.values.eq(svCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${svCopy.getClass} on ${sv.getClass}.")
    }

    val dv = Vectors.dense(1.0, 0.0, 2.0)
    val dvCopy = dv.copy
    (dv, dvCopy) match {
      case (dv: DenseVector, dvCopy: DenseVector) =>
        assert(dv.size === dvCopy.size)
        assert(dv.values === dvCopy.values)
        assert(!dv.values.eq(dvCopy.values))
      case _ =>
        throw new RuntimeException(s"copy returned ${dvCopy.getClass} on ${dv.getClass}.")
    }
  }

  test("VectorUDT") {
    val dv0 = Vectors.dense(Array.empty[Double])
    val dv1 = Vectors.dense(1.0, 2.0)
    val sv0 = Vectors.sparse(2, Array.empty, Array.empty)
    val sv1 = Vectors.sparse(2, Array(1), Array(2.0))
    val udt = new VectorUDT()
    for (v <- Seq(dv0, dv1, sv0, sv1)) {
      assert(v === udt.deserialize(udt.serialize(v)))
    }
  }

  test("fromBreeze") {
    val x = BDM.zeros[Double](10, 10)
    val v = Vectors.fromBreeze(x(::, 0))
    assert(v.size === x.rows)
  }

  test("activeIterator") {
    val dv = Vectors.dense(0.0, 1.2, 3.1, 0.0)
    val sv = Vectors.sparse(4, Seq((1, 1.2), (2, 3.1), (3, 0.0)))

    // Testing if the size of iterator is correct when the zeros are explicitly skipped.
    // The default setting will not skip any zero explicitly.
    assert(dv.activeIterator.size === 4)
    assert(dv.activeIterator(false).size === 4)
    assert(dv.activeIterator(true).size === 2)

    assert(sv.activeIterator.size === 3)
    assert(sv.activeIterator(false).size === 3)
    assert(sv.activeIterator(true).size === 2)

    // Testing `hasNext` and `next`
    val dvIter1 = dv.activeIterator(false)
    assert(dvIter1.hasNext === true && dvIter1.next === (0, 0.0))
    assert(dvIter1.hasNext === true && dvIter1.next === (1, 1.2))
    assert(dvIter1.hasNext === true && dvIter1.next === (2, 3.1))
    assert(dvIter1.hasNext === true && dvIter1.next === (3, 0.0))
    assert(dvIter1.hasNext === false)

    val dvIter2 = dv.activeIterator(true)
    assert(dvIter2.hasNext === true && dvIter2.next === (1, 1.2))
    assert(dvIter2.hasNext === true && dvIter2.next === (2, 3.1))
    assert(dvIter2.hasNext === false)

    val svIter1 = sv.activeIterator(false)
    assert(svIter1.hasNext === true && svIter1.next === (1, 1.2))
    assert(svIter1.hasNext === true && svIter1.next === (2, 3.1))
    assert(svIter1.hasNext === true && svIter1.next === (3, 0.0))
    assert(svIter1.hasNext === false)

    val svIter2 = sv.activeIterator(true)
    assert(svIter2.hasNext === true && svIter2.next === (1, 1.2))
    assert(svIter2.hasNext === true && svIter2.next === (2, 3.1))
    assert(svIter2.hasNext === false)

    // Testing `foreach`
    val dvMap1 = scala.collection.mutable.Map[Int, Double]()
    dvIter1.foreach{
      case (index, value) => dvMap1.put(index, value)
    }
    assert(dvMap1.size === 4)
    assert(dvMap1.get(0) === Some(0.0))
    assert(dvMap1.get(1) === Some(1.2))
    assert(dvMap1.get(2) === Some(3.1))
    assert(dvMap1.get(3) === Some(0.0))

    val dvMap2 = scala.collection.mutable.Map[Int, Double]()
    dvIter2.foreach{
      case (index, value) => dvMap2.put(index, value)
    }
    assert(dvMap2.size === 2)
    assert(dvMap2.get(1) === Some(1.2))
    assert(dvMap2.get(2) === Some(3.1))

    val svMap1 = scala.collection.mutable.Map[Int, Double]()
    svIter1.foreach{
      case (index, value) => svMap1.put(index, value)
    }
    assert(svMap1.size === 3)
    assert(svMap1.get(1) === Some(1.2))
    assert(svMap1.get(2) === Some(3.1))
    assert(svMap1.get(3) === Some(0.0))

    val svMap2 = scala.collection.mutable.Map[Int, Double]()
    svIter2.foreach{
      case (index, value) => svMap2.put(index, value)
    }
    assert(svMap2.size === 2)
    assert(svMap2.get(1) === Some(1.2))
    assert(svMap2.get(2) === Some(3.1))

  }
}
