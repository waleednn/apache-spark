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

package org.apache.spark.mllib.fpm

import java.{util => ju}
import java.lang.{Iterable => JavaIterable}

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.{fakeClassTag, fakeTypeTag}
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._

/**
 * Model trained by [[FPGrowth]], which holds frequent itemsets.
 * @param freqItemsets frequent itemset, which is an RDD of [[FreqItemset]]
 * @tparam Item item type
 */
@Since("1.3.0")
class FPGrowthModel[Item: ClassTag: TypeTag] @Since("1.3.0") (
    @Since("1.3.0") val freqItemsets: RDD[FreqItemset[Item]])
  extends Saveable with Serializable {
  /**
   * Generates association rules for the [[Item]]s in [[freqItemsets]].
   * @param confidence minimal confidence of the rules produced
   */
  @Since("1.5.0")
  def generateAssociationRules(confidence: Double): RDD[AssociationRules.Rule[Item]] = {
    val associationRules = new AssociationRules(confidence)
    associationRules.run(freqItemsets)
  }

  override def save(sc: SparkContext, path: String): Unit = {
    FPGrowthModel.SaveLoadV1_0.save(this, path)
  }

  override protected val formatVersion: String = "1.0"
}

object FPGrowthModel extends Loader[FPGrowthModel[_]] {

  override def load(sc: SparkContext, path: String): FPGrowthModel[_] = {
    val inferredItemset = FPGrowthModel.SaveLoadV1_0.inferItemType(sc, path)
    FPGrowthModel.SaveLoadV1_0.load(sc, path, inferredItemset)
  }

  private[fpm] object SaveLoadV1_0 {

    private val thisFormatVersion = "1.0"

    private val thisClassName = "org.apache.spark.mllib.fpm.FPGrowthModel"

    def save[Item: ClassTag: TypeTag](model: FPGrowthModel[Item], path: String): Unit = {
      val sc = model.freqItemsets.sparkContext
      val sqlContext = SQLContext.getOrCreate(sc)

      val metadata = compact(render(
        ("class" -> thisClassName) ~ ("version" -> thisFormatVersion)))
      sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))

      // Get the type of item class
      val sample = model.freqItemsets.take(1)(0).items(0)
      val className = sample.getClass.getCanonicalName
      val classSymbol = runtimeMirror(getClass.getClassLoader).staticClass(className)
      val tpe = classSymbol.selfType

      val itemType = ScalaReflection.schemaFor(tpe).dataType
      val fields = Array(StructField("items", ArrayType(itemType)),
        StructField("freq", LongType))
      val schema = StructType(fields)
      val rowDataRDD = model.freqItemsets.map { x =>
        Row(x.items, x.freq)
      }
      sqlContext.createDataFrame(rowDataRDD, schema).write.parquet(Loader.dataPath(path))
    }

    def inferItemType(sc: SparkContext, path: String): FreqItemset[_] = {
      val sqlContext = SQLContext.getOrCreate(sc)
      val freqItemsets = sqlContext.read.parquet(Loader.dataPath(path))
      val itemsetType = freqItemsets.schema("items").dataType
      val freqType = freqItemsets.schema("freq").dataType
      require(itemsetType.isInstanceOf[ArrayType],
        s"items should be ArrayType, but got $itemsetType")
      require(freqType.isInstanceOf[LongType], s"freq should be LongType, but got $freqType")
      val itemType = itemsetType.asInstanceOf[ArrayType].elementType
      val result = itemType match {
        case BooleanType => new FreqItemset(Array[Boolean](), 0L)
        case BinaryType => new FreqItemset(Array(Array[Byte]()), 0L)
        case StringType => new FreqItemset(Array[String](), 0L)
        case ByteType => new FreqItemset(Array[Byte](), 0L)
        case ShortType => new FreqItemset(Array[Short](), 0L)
        case IntegerType => new FreqItemset(Array[Int](), 0L)
        case LongType => new FreqItemset(Array[Long](), 0L)
        case FloatType => new FreqItemset(Array[Float](), 0L)
        case DoubleType => new FreqItemset(Array[Double](), 0L)
        case DateType => new FreqItemset(Array[java.sql.Date](), 0L)
        case DecimalType.SYSTEM_DEFAULT => new FreqItemset(Array[java.math.BigDecimal](), 0L)
        case TimestampType => new FreqItemset(Array[java.sql.Timestamp](), 0L)
        case _: ArrayType => new FreqItemset(Array[Seq[_]](), 0L)
        case _: MapType => new FreqItemset(Array[Map[_, _]](), 0L)
        case _: StructType => new FreqItemset(Array[Row](), 0L)
        case other =>
          throw new UnsupportedOperationException(s"Schema for type $other is not supported")
      }
      result
    }

    def load[Item: ClassTag: TypeTag](
        sc: SparkContext,
        path: String,
        inferredItemset: FreqItemset[Item]): FPGrowthModel[Item] = {
      implicit val formats = DefaultFormats
      val sqlContext = SQLContext.getOrCreate(sc)

      val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
      assert(className == thisClassName)
      assert(formatVersion == thisFormatVersion)

      val freqItemsets = sqlContext.read.parquet(Loader.dataPath(path))
      val freqItemsetsRDD = freqItemsets.select("items", "freq").map { x =>
        val items = x.getAs[Seq[Item]](0).toArray
        val freq = x.getLong(1)
        new FreqItemset(items, freq)
      }
      new FPGrowthModel(freqItemsetsRDD)
    }
  }
}

/**
 * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
 * [[http://dx.doi.org/10.1145/1454008.1454027 Li et al., PFP: Parallel FP-Growth for Query
 *  Recommendation]]. PFP distributes computation in such a way that each worker executes an
 * independent group of mining tasks. The FP-Growth algorithm is described in
 * [[http://dx.doi.org/10.1145/335191.335372 Han et al., Mining frequent patterns without candidate
 *  generation]].
 *
 * @param minSupport the minimal support level of the frequent pattern, any pattern appears
 *                   more than (minSupport * size-of-the-dataset) times will be output
 * @param numPartitions number of partitions used by parallel FP-growth
 *
 * @see [[http://en.wikipedia.org/wiki/Association_rule_learning Association rule learning
 *       (Wikipedia)]]
 *
 */
@Since("1.3.0")
class FPGrowth private (
    private var minSupport: Double,
    private var numPartitions: Int) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minSupport: `0.3`, numPartitions: same
   * as the input data}.
   *
   */
  @Since("1.3.0")
  def this() = this(0.3, -1)

  /**
   * Sets the minimal support level (default: `0.3`).
   *
   */
  @Since("1.3.0")
  def setMinSupport(minSupport: Double): this.type = {
    this.minSupport = minSupport
    this
  }

  /**
   * Sets the number of partitions used by parallel FP-growth (default: same as input data).
   *
   */
  @Since("1.3.0")
  def setNumPartitions(numPartitions: Int): this.type = {
    this.numPartitions = numPartitions
    this
  }

  /**
   * Computes an FP-Growth model that contains frequent itemsets.
   * @param data input data set, each element contains a transaction
   * @return an [[FPGrowthModel]]
   *
   */
  @Since("1.3.0")
  def run[Item: ClassTag: TypeTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("Input data is not cached.")
    }
    val count = data.count()
    val minCount = math.ceil(minSupport * count).toLong
    val numParts = if (numPartitions > 0) numPartitions else data.partitions.length
    val partitioner = new HashPartitioner(numParts)
    val freqItems = genFreqItems(data, minCount, partitioner)
    val freqItemsets = genFreqItemsets(data, minCount, freqItems, partitioner)
    new FPGrowthModel(freqItemsets)
  }

  /** Java-friendly version of [[run]]. */
  @Since("1.3.0")
  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
    implicit val tag = fakeClassTag[Item]
    implicit val typeTag = fakeTypeTag[Item]
    run(data.rdd.map(_.asScala.toArray))
  }

  /**
   * Generates frequent items by filtering the input data using minimal support level.
   * @param minCount minimum count for frequent itemsets
   * @param partitioner partitioner used to distribute items
   * @return array of frequent pattern ordered by their frequencies
   */
  private def genFreqItems[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      partitioner: Partitioner): Array[Item] = {
    data.flatMap { t =>
      val uniq = t.toSet
      if (t.size != uniq.size) {
        throw new SparkException(s"Items in a transaction must be unique but got ${t.toSeq}.")
      }
      t
    }.map(v => (v, 1L))
      .reduceByKey(partitioner, _ + _)
      .filter(_._2 >= minCount)
      .collect()
      .sortBy(-_._2)
      .map(_._1)
  }

  /**
   * Generate frequent itemsets by building FP-Trees, the extraction is done on each partition.
   * @param data transactions
   * @param minCount minimum count for frequent itemsets
   * @param freqItems frequent items
   * @param partitioner partitioner used to distribute transactions
   * @return an RDD of (frequent itemset, count)
   */
  private def genFreqItemsets[Item: ClassTag](
      data: RDD[Array[Item]],
      minCount: Long,
      freqItems: Array[Item],
      partitioner: Partitioner): RDD[FreqItemset[Item]] = {
    val itemToRank = freqItems.zipWithIndex.toMap
    data.flatMap { transaction =>
      genCondTransactions(transaction, itemToRank, partitioner)
    }.aggregateByKey(new FPTree[Int], partitioner.numPartitions)(
      (tree, transaction) => tree.add(transaction, 1L),
      (tree1, tree2) => tree1.merge(tree2))
    .flatMap { case (part, tree) =>
      tree.extract(minCount, x => partitioner.getPartition(x) == part)
    }.map { case (ranks, count) =>
      new FreqItemset(ranks.map(i => freqItems(i)).toArray, count)
    }
  }

  /**
   * Generates conditional transactions.
   * @param transaction a transaction
   * @param itemToRank map from item to their rank
   * @param partitioner partitioner used to distribute transactions
   * @return a map of (target partition, conditional transaction)
   */
  private def genCondTransactions[Item: ClassTag](
      transaction: Array[Item],
      itemToRank: Map[Item, Int],
      partitioner: Partitioner): mutable.Map[Int, Array[Int]] = {
    val output = mutable.Map.empty[Int, Array[Int]]
    // Filter the basket by frequent items pattern and sort their ranks.
    val filtered = transaction.flatMap(itemToRank.get)
    ju.Arrays.sort(filtered)
    val n = filtered.length
    var i = n - 1
    while (i >= 0) {
      val item = filtered(i)
      val part = partitioner.getPartition(item)
      if (!output.contains(part)) {
        output(part) = filtered.slice(0, i + 1)
      }
      i -= 1
    }
    output
  }
}

@Since("1.3.0")
object FPGrowth {

  /**
   * Frequent itemset.
   * @param items items in this itemset. Java users should call [[FreqItemset#javaItems]] instead.
   * @param freq frequency
   * @tparam Item item type
   *
   */
  @Since("1.3.0")
  class FreqItemset[Item] @Since("1.3.0") (
      @Since("1.3.0") val items: Array[Item],
      @Since("1.3.0") val freq: Long) extends Serializable {

    /**
     * Returns items in a Java List.
     *
     */
    @Since("1.3.0")
    def javaItems: java.util.List[Item] = {
      items.toList.asJava
    }
  }
}
