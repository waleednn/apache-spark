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

import scala.util.Random

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasSeed
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType

/**
 * Model produced by [[MinHashLSH]], where multiple hash functions are stored. Each hash function
 * is picked from the following family of hash functions, where a_i and b_i are randomly chosen
 * integers less than prime:
 *    `h_i(x) = ((x \cdot a_i + b_i) \mod prime)`
 *
 * This hash family is approximately min-wise independent according to the reference.
 *
 * Reference:
 * Tom Bohman, Colin Cooper, and Alan Frieze. "Min-wise independent linear permutations."
 * Electronic Journal of Combinatorics 7 (2000): R26.
 *
 * @param randCoefficients Pairs of random coefficients. Each pair is used by one hash function.
 */
@Since("2.1.0")
class MinHashLSHModel private[ml](
    override val uid: String,
    private[ml] val randCoefficients: Array[(Int, Int)])
  extends LSHModel[MinHashLSHModel] {

  /** @group setParam */
  @Since("2.4.0")
  override def setInputCol(value: String): this.type = super.set(inputCol, value)

  /** @group setParam */
  @Since("2.4.0")
  override def setOutputCol(value: String): this.type = super.set(outputCol, value)

  @Since("2.1.0")
  override protected[ml] def hashFunction(elems: Vector): Array[Vector] = {
    require(elems.nonZeroIterator.nonEmpty, "Must have at least 1 non zero entry.")
    val hashValues = randCoefficients.map { case (a, b) =>
      elems.nonZeroIterator.map { case (i, _) =>
        ((1L + i) * a + b) % MinHashLSH.HASH_PRIME
      }.min.toDouble
    }
    // TODO: Output vectors of dimension numHashFunctions in SPARK-18450
    hashValues.map(Vectors.dense(_))
  }

  @Since("2.1.0")
  override protected[ml] def keyDistance(x: Vector, y: Vector): Double = {
    val xIter = x.nonZeroIterator.map(_._1)
    val yIter = y.nonZeroIterator.map(_._1)
    if (xIter.isEmpty) {
      require(yIter.hasNext, "The union of two input sets must have at least 1 elements")
      return 1.0
    } else if (yIter.isEmpty) {
      return 1.0
    }

    var xIndex = xIter.next()
    var yIndex = yIter.next()
    var xSize = 1
    var ySize = 1
    var intersectionSize = 0

    while (xIndex != -1 && yIndex != -1) {
      if (xIndex == yIndex) {
        intersectionSize += 1
        xIndex = if (xIter.hasNext) { xSize += 1; xIter.next() } else -1
        yIndex = if (yIter.hasNext) { ySize += 1; yIter.next() } else -1
      } else if (xIndex > yIndex) {
        yIndex = if (yIter.hasNext) { ySize += 1; yIter.next() } else -1
      } else {
        xIndex = if (xIter.hasNext) { xSize += 1; xIter.next() } else -1
      }
    }

    xSize += xIter.size
    ySize += yIter.size

    val unionSize = xSize + ySize - intersectionSize
    require(unionSize > 0, "The union of two input sets must have at least 1 elements")
    1 - intersectionSize.toDouble / unionSize
  }

  @Since("2.1.0")
  override protected[ml] def hashDistance(x: Seq[Vector], y: Seq[Vector]): Double = {
    // Since it's generated by hashing, it will be a pair of dense vectors.
    // TODO: This hashDistance function requires more discussion in SPARK-18454
    x.iterator.zip(y.iterator).map(vectorPair =>
      vectorPair._1.toArray.zip(vectorPair._2.toArray).count(pair => pair._1 != pair._2)
    ).min
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): MinHashLSHModel = {
    val copied = new MinHashLSHModel(uid, randCoefficients).setParent(parent)
    copyValues(copied, extra)
  }

  @Since("2.1.0")
  override def write: MLWriter = new MinHashLSHModel.MinHashLSHModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"MinHashLSHModel: uid=$uid, numHashTables=${$(numHashTables)}"
  }
}

/**
 * LSH class for Jaccard distance.
 *
 * The input can be dense or sparse vectors, but it is more efficient if it is sparse. For example,
 *    `Vectors.sparse(10, Array((2, 1.0), (3, 1.0), (5, 1.0)))`
 * means there are 10 elements in the space. This set contains elements 2, 3, and 5. Also, any
 * input vector must have at least 1 non-zero index, and all non-zero values are
 * treated as binary "1" values.
 *
 * References:
 * <a href="https://en.wikipedia.org/wiki/MinHash">Wikipedia on MinHash</a>
 */
@Since("2.1.0")
class MinHashLSH(override val uid: String) extends LSH[MinHashLSHModel] with HasSeed {

  @Since("2.1.0")
  override def setInputCol(value: String): this.type = super.setInputCol(value)

  @Since("2.1.0")
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  @Since("2.1.0")
  override def setNumHashTables(value: Int): this.type = super.setNumHashTables(value)

  @Since("2.1.0")
  def this() = {
    this(Identifiable.randomUID("mh-lsh"))
  }

  /** @group setParam */
  @Since("2.1.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.1.0")
  override protected[ml] def createRawLSHModel(inputDim: Int): MinHashLSHModel = {
    require(inputDim <= MinHashLSH.HASH_PRIME,
      s"The input vector dimension $inputDim exceeds the threshold ${MinHashLSH.HASH_PRIME}.")
    val rand = new Random($(seed))
    val randCoefs: Array[(Int, Int)] = Array.fill($(numHashTables)) {
        (1 + rand.nextInt(MinHashLSH.HASH_PRIME - 1), rand.nextInt(MinHashLSH.HASH_PRIME - 1))
      }
    new MinHashLSHModel(uid, randCoefs)
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    validateAndTransformSchema(schema)
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

@Since("2.1.0")
object MinHashLSH extends DefaultParamsReadable[MinHashLSH] {
  // A large prime smaller than sqrt(2^63 − 1)
  private[ml] val HASH_PRIME = 2038074743

  @Since("2.1.0")
  override def load(path: String): MinHashLSH = super.load(path)
}

@Since("2.1.0")
object MinHashLSHModel extends MLReadable[MinHashLSHModel] {

  @Since("2.1.0")
  override def read: MLReader[MinHashLSHModel] = new MinHashLSHModelReader

  @Since("2.1.0")
  override def load(path: String): MinHashLSHModel = super.load(path)

  private[MinHashLSHModel] class MinHashLSHModelWriter(instance: MinHashLSHModel)
    extends MLWriter {

    private case class Data(randCoefficients: Array[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.randCoefficients.flatMap(tuple => Array(tuple._1, tuple._2)))
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class MinHashLSHModelReader extends MLReader[MinHashLSHModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[MinHashLSHModel].getName

    override def load(path: String): MinHashLSHModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("randCoefficients").head()
      val randCoefficients = data.getSeq[Int](0).grouped(2)
        .map(tuple => (tuple(0), tuple(1))).toArray
      val model = new MinHashLSHModel(metadata.uid, randCoefficients)

      metadata.getAndSetParams(model)
      model
    }
  }
}
