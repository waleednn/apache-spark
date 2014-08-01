/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* Add a comment to this line
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

package org.apache.spark.mllib.feature

import scala.util.{Random => Random}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.HashPartitioner

/**
 *  Entry in vocabulary 
 */
private case class VocabWord(
  var word: String,
  var cn: Int,
  var point: Array[Int],
  var code: Array[Int],
  var codeLen:Int
)

/**
 *  Vector representation of word
 */
class Word2Vec(
    val size: Int,
    val startingAlpha: Double,
    val window: Int,
    val minCount: Int) 
  extends Serializable with Logging {
  
  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40
  private val MAX_SENTENCE_LENGTH = 1000
  private val layer1Size = size 
  private val modelPartitionNum = 100
  
  private var trainWordsCount = 0
  private var vocabSize = 0
  private var vocab: Array[VocabWord] = null
  private var vocabHash = mutable.HashMap.empty[String, Int]
  private var alpha = startingAlpha

  private def learnVocab(dataset: RDD[String]) {
    vocab = dataset.flatMap(line => line.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(x => VocabWord(x._1, x._2, new Array[Int](MAX_CODE_LENGTH), new Array[Int](MAX_CODE_LENGTH), 0))
      .filter(_.cn >= minCount)
      .collect()
      .sortWith((a, b)=> a.cn > b.cn)
    
    vocabSize = vocab.length
    var a = 0
    while (a < vocabSize) {
      vocabHash += vocab(a).word -> a
      trainWordsCount += vocab(a).cn
      a += 1
    }
    logInfo("trainWordsCount = " + trainWordsCount)
  }

  private def createExpTable(): Array[Double] = {
    val expTable = new Array[Double](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = tmp / (tmp + 1)
      i += 1
    }
    expTable
  }

  private def createBinaryTree() {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize
    
    var min1i = 0 
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }
  
  /**
   * Computes the vector representation of each word in 
   * vocabulary
   * @param dataset an RDD of strings
   * @return a Word2VecModel
   */

  def fit(dataset:RDD[String]): Word2VecModel = {

    learnVocab(dataset)
    
    createBinaryTree()
    
    val sc = dataset.context

    val expTable = sc.broadcast(createExpTable())
    val V = sc.broadcast(vocab)
    val VHash = sc.broadcast(vocabHash)
    
    val sentences = dataset.flatMap(line => line.split(" ")).mapPartitions {
      iter => { new Iterator[Array[Int]] {
          def hasNext = iter.hasNext
          def next = {
            var sentence = new ArrayBuffer[Int]
            var sentenceLength = 0
            while (iter.hasNext && sentenceLength < MAX_SENTENCE_LENGTH) {
              val word = VHash.value.get(iter.next)
              word match {
                case Some(w) => {
                  sentence += w
                  sentenceLength += 1
                }
                case None => 
              }
            }
            sentence.toArray
          }
        }
      }
    }
    
    val newSentences = sentences.repartition(1).cache()
    val temp = Array.fill[Double](vocabSize * layer1Size)((Random.nextDouble - 0.5) / layer1Size)
    val (aggSyn0, _, _, _) =
      // TODO: broadcast temp instead of serializing it directly or initialize the model in each executor
      newSentences.aggregate((temp.clone(), new Array[Double](vocabSize * layer1Size), 0, 0))(
        seqOp = (c, v) => (c, v) match { case ((syn0, syn1, lastWordCount, wordCount), sentence) =>
          var lwc = lastWordCount
          var wc = wordCount 
          if (wordCount - lastWordCount > 10000) {
            lwc = wordCount
            alpha = startingAlpha * (1 - wordCount.toDouble / (trainWordsCount + 1))
            if (alpha < startingAlpha * 0.0001) alpha = startingAlpha * 0.0001
            logInfo("wordCount = " + wordCount + ", alpha = " + alpha)
          }
          wc += sentence.size
          var pos = 0
          while (pos < sentence.size) {
            val word = sentence(pos)
            // TODO: fix random seed
            val b = Random.nextInt(window)
            // Train Skip-gram
            var a = b
            while (a < window * 2 + 1 - b) {
              if (a != window) {
                val c = pos - window + a
                if (c >= 0 && c < sentence.size) {
                  val lastWord = sentence(c)
                  val l1 = lastWord * layer1Size
                  val neu1e = new Array[Double](layer1Size)
                  //HS
                  var d = 0
                  while (d < vocab(word).codeLen) {
                    val l2 = vocab(word).point(d) * layer1Size
                    // Propagate hidden -> output
                    var f = blas.ddot(layer1Size, syn0, l1, 1, syn1, l2, 1)
                    if (f > -MAX_EXP && f < MAX_EXP) {
                      val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                      f = expTable.value(ind)
                      val g = (1 - vocab(word).code(d) - f) * alpha
                      blas.daxpy(layer1Size, g, syn1, l2, 1, neu1e, 0, 1)
                      blas.daxpy(layer1Size, g, syn0, l1, 1, syn1, l2, 1)
                    }
                    d += 1
                  }
                  blas.daxpy(layer1Size, 1.0, neu1e, 0, 1, syn0, l1, 1)
                }
              }
              a += 1
            }
            pos += 1
          }
          (syn0, syn1, lwc, wc)
        },
        combOp = (c1, c2) => (c1, c2) match { case ((syn0_1, syn1_1, lwc_1, wc_1), (syn0_2, syn1_2, lwc_2, wc_2)) =>
          val n = syn0_1.length
          blas.daxpy(n, 1.0, syn0_2, 1, syn0_1, 1)
          blas.daxpy(n, 1.0, syn1_2, 1, syn1_1, 1)
          (syn0_1, syn0_2, lwc_1 + lwc_2, wc_1 + wc_2)
        })
    
    val wordMap = new Array[(String, Array[Double])](vocabSize)
    var i = 0
    while (i < vocabSize) {
      val word = vocab(i).word
      val vector = new Array[Double](layer1Size)
      Array.copy(aggSyn0, i * layer1Size, vector, 0, layer1Size)
      wordMap(i) = (word, vector)
      i += 1
    }
    val modelRDD = sc.parallelize(wordMap, modelPartitionNum).partitionBy(new HashPartitioner(modelPartitionNum))
    new Word2VecModel(modelRDD)
  }
}

/**
* Word2Vec model
*/
class Word2VecModel (val _model:RDD[(String, Array[Double])]) extends Serializable {

  val model = _model

  private def distance(v1: Array[Double], v2: Array[Double]): Double = {
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.dnrm2(n, v1, 1)
    val norm2 = blas.dnrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.ddot(n, v1, 1, v2,1) / norm1 / norm2
  }
  
  /**
   * Transforms a word to its vector representation
   * @param word a word 
   * @return vector representation of word
   */

  def transform(word: String): Array[Double] = {
    val result = model.lookup(word) 
    if (result.isEmpty) Array[Double]()
    else result(0)
  }
  
  /**
   * Transforms an RDD to its vector representation
   * @param dataset a an RDD of words 
   * @return RDD of vector representation 
   */
  
  def transform(dataset: RDD[String]): RDD[Array[Double]] = {
    dataset.map(word => transform(word))
  }
  
  /**
   * Find synonyms of a word
   * @param word a word
   * @param num number of synonyms to find  
   * @return array of (word, similarity)
   */
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word)
    if (vector.isEmpty) Array[(String, Double)]()
    else findSynonyms(vector,num)
  }
  
  /**
   * Find synonyms of the vector representation of a word
   * @param vector vector representation of a word
   * @param num number of synonyms to find  
   * @return array of (word, similarity)
   */
  def findSynonyms(vector: Array[Double], num: Int): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    val topK = model.map(
      {case(w, vec) => (distance(vector, vec), w)})
    .sortByKey(ascending = false)
    .take(num + 1)
    .map({case (dist, w) => (w, dist)}).drop(1)
    
    topK
  }
}

object Word2Vec extends Serializable with Logging {
  /**
   * Train Word2Vec model
   * @param input RDD of words
   * @param size vectoer dimension
   * @param startingAlpha initial learning rate
   * @param window context words from [-window, window]
   * @param minCount minimum frequncy to consider a vocabulary word
   * @return Word2Vec model 
  */
  def train(
    input: RDD[String],
    size: Int,
    startingAlpha: Double,
    window: Int,
    minCount: Int): Word2VecModel = {
    new Word2Vec(size,startingAlpha, window, minCount).fit(input)
  }
}
