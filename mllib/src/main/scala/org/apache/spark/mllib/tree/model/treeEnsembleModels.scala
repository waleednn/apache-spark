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

package org.apache.spark.mllib.tree.model

import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.configuration.EnsembleCombiningStrategy._
import org.apache.spark.mllib.util.{Saveable, Loader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext}


/**
 * :: Experimental ::
 * Represents a random forest model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 */
@Experimental
class RandomForestModel(override val algo: Algo, override val trees: Array[DecisionTreeModel])
  extends TreeEnsembleModel(algo, trees, Array.fill(trees.size)(1.0),
    combiningStrategy = if (algo == Classification) Vote else Average)
  with Saveable {

  require(trees.forall(_.algo == algo))

  override def save(sc: SparkContext, path: String): Unit = {
    TreeEnsembleModel.SaveLoadV1_0.save(sc, path, this,
      RandomForestModel.SaveLoadV1_0.thisClassName)
  }

  override protected def formatVersion: String = TreeEnsembleModel.SaveLoadV1_0.thisFormatVersion
}

object RandomForestModel extends Loader[RandomForestModel] {

  override def load(sc: SparkContext, path: String): RandomForestModel = {
    val (loadedClassName, version, metadataRDD) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val metadata = TreeEnsembleModel.SaveLoadV1_0.readMetadata(metadataRDD, path)
        assert(metadata.treeWeights.forall(_ == 1.0))
        val trees =
          TreeEnsembleModel.SaveLoadV1_0.loadTrees(sc, path, classNameV1_0, metadata.treeAlgo)
        new RandomForestModel(Algo.fromString(metadata.algo), trees)
      case _ => throw new Exception(s"RandomForestModel.load did not recognize model" +
        s" with (className, format version): ($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }

  private object SaveLoadV1_0 {
    // Hard-code class name string in case it changes in the future
    def thisClassName = "org.apache.spark.mllib.tree.model.RandomForestModel"
  }

}

/**
 * :: Experimental ::
 * Represents a gradient boosted trees model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 * @param treeWeights tree ensemble weights
 */
@Experimental
class GradientBoostedTreesModel(
    override val algo: Algo,
    override val trees: Array[DecisionTreeModel],
    override val treeWeights: Array[Double])
  extends TreeEnsembleModel(algo, trees, treeWeights, combiningStrategy = Sum)
  with Saveable {

  require(trees.size == treeWeights.size)

  override def save(sc: SparkContext, path: String): Unit = {
    TreeEnsembleModel.SaveLoadV1_0.save(sc, path, this,
      GradientBoostedTreesModel.SaveLoadV1_0.thisClassName)
  }

  override protected def formatVersion: String = TreeEnsembleModel.SaveLoadV1_0.thisFormatVersion
}

object GradientBoostedTreesModel extends Loader[GradientBoostedTreesModel] {

  override def load(sc: SparkContext, path: String): GradientBoostedTreesModel = {
    val (loadedClassName, version, metadataRDD) = Loader.loadMetadata(sc, path)
    val classNameV1_0 = SaveLoadV1_0.thisClassName
    (loadedClassName, version) match {
      case (className, "1.0") if className == classNameV1_0 =>
        val metadata = TreeEnsembleModel.SaveLoadV1_0.readMetadata(metadataRDD, path)
        assert(metadata.combiningStrategy == Sum.toString)
        val trees =
          TreeEnsembleModel.SaveLoadV1_0.loadTrees(sc, path, classNameV1_0, metadata.treeAlgo)
        new GradientBoostedTreesModel(Algo.fromString(metadata.algo), trees, metadata.treeWeights)
      case _ => throw new Exception(s"GradientBoostedTreesModel.load did not recognize model" +
        s" with (className, format version): ($loadedClassName, $version).  Supported:\n" +
        s"  ($classNameV1_0, 1.0)")
    }
  }

  private object SaveLoadV1_0 {
    // Hard-code class name string in case it changes in the future
    def thisClassName = "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"
  }

}

/**
 * Represents a tree ensemble model.
 *
 * @param algo algorithm for the ensemble model, either Classification or Regression
 * @param trees tree ensembles
 * @param treeWeights tree ensemble weights
 * @param combiningStrategy strategy for combining the predictions, not used for regression.
 */
private[tree] sealed class TreeEnsembleModel(
    protected val algo: Algo,
    protected val trees: Array[DecisionTreeModel],
    protected val treeWeights: Array[Double],
    protected val combiningStrategy: EnsembleCombiningStrategy) extends Serializable {

  require(numTrees > 0, "TreeEnsembleModel cannot be created without trees.")

  private val sumWeights = math.max(treeWeights.sum, 1e-15)

  /**
   * Predicts for a single data point using the weighted sum of ensemble predictions.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  private def predictBySumming(features: Vector): Double = {
    val treePredictions = trees.map(_.predict(features))
    blas.ddot(numTrees, treePredictions, 1, treeWeights, 1)
  }

  /**
   * Classifies a single data point based on (weighted) majority votes.
   */
  private def predictByVoting(features: Vector): Double = {
    val votes = mutable.Map.empty[Int, Double]
    trees.view.zip(treeWeights).foreach { case (tree, weight) =>
      val prediction = tree.predict(features).toInt
      votes(prediction) = votes.getOrElse(prediction, 0.0) + weight
    }
    votes.maxBy(_._2)._1
  }

  /**
   * Predict values for a single data point using the model trained.
   *
   * @param features array representing a single data point
   * @return predicted category from the trained model
   */
  def predict(features: Vector): Double = {
    (algo, combiningStrategy) match {
      case (Regression, Sum) =>
        predictBySumming(features)
      case (Regression, Average) =>
        predictBySumming(features) / sumWeights
      case (Classification, Sum) => // binary classification
        val prediction = predictBySumming(features)
        // TODO: predicted labels are +1 or -1 for GBT. Need a better way to store this info.
        if (prediction > 0.0) 1.0 else 0.0
      case (Classification, Vote) =>
        predictByVoting(features)
      case _ =>
        throw new IllegalArgumentException(
          "TreeEnsembleModel given unsupported (algo, combiningStrategy) combination: " +
            s"($algo, $combiningStrategy).")
    }
  }

  /**
   * Predict values for the given data set.
   *
   * @param features RDD representing data points to be predicted
   * @return RDD[Double] where each entry contains the corresponding prediction
   */
  def predict(features: RDD[Vector]): RDD[Double] = features.map(x => predict(x))

  /**
   * Java-friendly version of [[org.apache.spark.mllib.tree.model.TreeEnsembleModel#predict]].
   */
  def predict(features: JavaRDD[Vector]): JavaRDD[java.lang.Double] = {
    predict(features.rdd).toJavaRDD().asInstanceOf[JavaRDD[java.lang.Double]]
  }

  /**
   * Print a summary of the model.
   */
  override def toString: String = {
    algo match {
      case Classification =>
        s"TreeEnsembleModel classifier with $numTrees trees\n"
      case Regression =>
        s"TreeEnsembleModel regressor with $numTrees trees\n"
      case _ => throw new IllegalArgumentException(
        s"TreeEnsembleModel given unknown algo parameter: $algo.")
    }
  }

  /**
   * Print the full model to a string.
   */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zipWithIndex.map { case (tree, treeIndex) =>
      s"  Tree $treeIndex:\n" + tree.topNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

  /**
   * Get number of trees in forest.
   */
  def numTrees: Int = trees.size

  /**
   * Get total number of nodes, summed over all trees in the forest.
   */
  def totalNumNodes: Int = trees.map(_.numNodes).sum
}

private[tree] object TreeEnsembleModel {

  object SaveLoadV1_0 {

    import DecisionTreeModel.SaveLoadV1_0.{InformationGainStatsData, PredictData, SplitData}

    def thisFormatVersion = "1.0"

    case class Metadata(
        algo: String,
        treeAlgo: String,
        combiningStrategy: String,
        treeWeights: Array[Double])

    /**
     * Model data for model import/export.
     * We have to duplicate NodeData here since Spark SQL does not yet support extracting subfields
     * of nested fields; once that is possible, we can use something like:
     *  case class EnsembleNodeData(treeId: Int, node: NodeData),
     *  where NodeData is from DecisionTreeModel.
     */
    case class NodeData(
        treeId: Int,
        id: Int,
        predict: PredictData,
        impurity: Double,
        isLeaf: Boolean,
        split: Option[SplitData],
        leftNodeId: Option[Int],
        rightNodeId: Option[Int],
        stats: Option[InformationGainStatsData])

    object NodeData {
      def apply(treeId: Int, n: Node): NodeData = {
        NodeData(treeId,
          n.id, PredictData(n.predict), n.impurity, n.isLeaf, n.split.map(SplitData.apply),
          n.leftNode.map(_.id), n.rightNode.map(_.id), n.stats.map(InformationGainStatsData.apply))
      }
    }

    def save(sc: SparkContext, path: String, model: TreeEnsembleModel, className: String): Unit = {
      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._

      // Create JSON metadata.
      val metadata = Metadata(model.algo.toString, model.trees(0).algo.toString,
        model.combiningStrategy.toString, model.treeWeights)
      val metadataRDD = sc.parallelize(Seq((className, thisFormatVersion, metadata)), 1)
        .toDataFrame("class", "version", "metadata")
      metadataRDD.toJSON.saveAsTextFile(Loader.metadataPath(path))

      // Create Parquet data.
      val dataRDD = sc.parallelize(model.trees.zipWithIndex).flatMap { case (tree, treeId) =>
        val nodeIterator = new DecisionTreeModel.NodeIterator(tree)
        nodeIterator.toSeq.map(node => NodeData(treeId, node))
      }.toDataFrame
      dataRDD.saveAsParquetFile(Loader.dataPath(path))
    }

    /**
     * Read metadata from the loaded metadata DataFrame.
     * @param path  Path for loading data, used for debug messages.
     */
    def readMetadata(metadata: DataFrame, path: String): Metadata = {
      try {
        // We rely on the try-catch for schema checking rather than creating a schema just for this.
        val metadataArray = metadata.select("metadata.algo", "metadata.treeAlgo",
          "metadata.combiningStrategy", "metadata.treeWeights").collect()
        assert(metadataArray.size == 1)
        Metadata(metadataArray(0).getString(0), metadataArray(0).getString(1),
          metadataArray(0).getString(2), metadataArray(0).getAs[Seq[Double]](3).toArray)
      } catch {
        // Catch both Error and Exception since the checks above can throw either.
        case e: Throwable =>
          throw new Exception(
            s"Unable to load TreeEnsembleModel metadata from: ${Loader.metadataPath(path)}."
              + s"  Error message: ${e.getMessage}")
      }
    }

    /**
     * Load trees for an ensemble, and return them in order.
     * @param className  Class name of the tree ensemble.
     * @param treeAlgo  Algorithm for individual trees (which may differ from the ensemble's
     *                  algorithm).
     */
    def loadTrees(
        sc: SparkContext,
        path: String,
        className: String,
        treeAlgo: String): Array[DecisionTreeModel] = {
      val datapath = Loader.dataPath(path)
      val sqlContext = new SQLContext(sc)
      val dataRDD = sqlContext.parquetFile(datapath)
      val nodesRDD = DecisionTreeModel.SaveLoadV1_0.readNodes(dataRDD)
      val treeIdRDD = dataRDD.select("treeId").map { case Row(treeId: Int) => treeId }
      val trees: RDD[(Int, DecisionTreeModel)] = treeIdRDD.zip(nodesRDD).groupBy(_._1).map {
        case (treeId, treeId_nodes) =>
          val tree =
            DecisionTreeModel.SaveLoadV1_0.constructTree(treeId_nodes.map(_._2), treeAlgo, datapath)
          (treeId, tree)
      }
      trees.collect().sortBy(_._1).map(_._2)
    }

  }

}
