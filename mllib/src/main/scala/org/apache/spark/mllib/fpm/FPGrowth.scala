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
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect._

import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkException}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.fpm.FPGrowth._
import org.apache.spark.mllib.util.{Loader, Saveable}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.{Row, SQLContext}
import  org.apache.spark.sql.types._

/**
 * :: Experimental ::
 *
 * Model trained by [[FPGrowth]], which holds frequent itemsets.
 * @param freqItemsets frequent itemset, which is an RDD of [[FreqItemset]]
 * @tparam Item item type
 *
 */
@Since("1.3.0")
@Experimental
class FPGrowthModel[Item: ClassTag](val freqItemsets: RDD[FreqItemset[Item]])
      extends Saveable with Serializable{

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
    FPGrowthModel.SaveLoadV1_0.save(sc, this, path)
  }

  override protected def formatVersion: String = "1.0"
}

object FPGrowthModel extends Loader[FPGrowthModel[FreqItemset[Any]]] {

  override def load(sc: SparkContext, path: String): FPGrowthModel[FreqItemset[Any]] = {
        FPGrowthModel.SaveLoadV1_0.load(sc, path)
      }

      private case class itemCountPair[Item](items: Array[Item], freq: Long)

    private[fpm]
    object SaveLoadV1_0 {

      private val thisFormatVersion = "1.0"

      private[fpm]
      val thisClassName = "org.apache.spark.mllib.fpm.FPGrowthModel"

      def save[Item: ClassTag](sc: SparkContext, model: FPGrowthModel[Item], path: String): Unit = {
        val sqlContext = new SQLContext(sc)

        val metadata = compact(render(
          ("class" -> thisClassName) ~ ("version" -> thisFormatVersion) ~
            ("frequentItemsCount" -> model.freqItemsets.count())))
        sc.parallelize(Seq(metadata), 1).saveAsTextFile(Loader.metadataPath(path))
        val sqlType = model.freqItemsets.first().items(0) match {
          case _: java.lang.String => StringType
          case _: java.lang.Integer => IntegerType
          case _: java.lang.Long => LongType
          case _: java.lang.Double => DoubleType
          case _: java.lang.Float => FloatType
          case _: java.lang.Boolean => BooleanType
        }
        val fields = Array(StructField("item", ArrayType(sqlType), true),
          StructField("count", LongType, true))
        val schema = StructType(fields)
        val rowDataRDD = model.freqItemsets.map { freqItemSetObj =>
          Row(freqItemSetObj.items, freqItemSetObj.freq)
        }
        sqlContext.createDataFrame(rowDataRDD, schema).write.parquet(Loader.dataPath(path))

      }

      def load[Item: ClassTag](sc: SparkContext, path: String): FPGrowthModel[Item] = {
        implicit val formats = DefaultFormats
        val sqlContext = new SQLContext(sc)
        val (className, formatVersion, metadata) = Loader.loadMetadata(sc, path)
        assert(className == thisClassName)
        assert(formatVersion == thisFormatVersion)
        val frequentItemsCount = (metadata \ "frequentItemsCount").extract[Int]
        val itemsRDD = sqlContext.read.parquet(Loader.dataPath(path))
        Loader.checkSchema[itemCountPair[Any]](itemsRDD.schema)
        val dataArray = itemsRDD.select("item", "count")
        val freqRdd = dataArray.map { d =>
           val itemArray = d.getAs[Seq[Item]](0).toArray
           val countValue = d.getLong(1)
           new FreqItemset[Item](itemArray, countValue)
         }
        new FPGrowthModel(freqRdd)
      }
    }
  }

/**
 * :: Experimental ::
 *
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
@Experimental
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
  def run[Item: ClassTag](data: RDD[Array[Item]]): FPGrowthModel[Item] = {
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

  def run[Item, Basket <: JavaIterable[Item]](data: JavaRDD[Basket]): FPGrowthModel[Item] = {
    implicit val tag = fakeClassTag[Item]
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

/**
 * :: Experimental ::
 *
 */
@Since("1.3.0")
@Experimental
object FPGrowth {

  /**
   * Frequent itemset.
   * @param items items in this itemset. Java users should call [[FreqItemset#javaItems]] instead.
   * @param freq frequency
   * @tparam Item item type
   *
   */
  @Since("1.3.0")
  class FreqItemset[Item](val items: Array[Item], val freq: Long) extends Serializable {

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
