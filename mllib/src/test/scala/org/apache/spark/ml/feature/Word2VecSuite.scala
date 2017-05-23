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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.util.Utils

class Word2VecSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new Word2Vec)
    val model = new Word2VecModel("w2v", new OldWord2VecModel(Map("a" -> Array(0.0f))))
    ParamsSuite.checkParams(model)
  }

  test("Word2Vec") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val numOfWords = sentence.split(" ").size
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val w2v = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
    val model = w2v.fit(docDF)

    MLTestingUtils.checkCopyAndUids(w2v, model)

    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    val magicExp = Vectors.dense(0.30153007534417237, -0.6833061711354689, 0.5116530778733167)
    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== magicExp absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("Word2Vec-SkipGram-NegativeSampling") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val numOfWords = sentence.split(" ").size
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(0.2554479241371155, -0.7374975085258484, 0.501385509967804),
      "b" -> Array(-0.12191832810640335, -0.09346947073936462, 1.6055092811584473),
      "c" -> Array(-0.3807409405708313, -0.004864220507442951, 0.7893226146697998)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val w2v = new Word2Vec()
      .setSolver("cbow-ns")
      .setNegativeSamples(2)
      .setMaxUnigramTableSize(10000)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSolver("sg-ns")
      .setSeed(42L)
    val model = w2v.fit(docDF)

    MLTestingUtils.checkCopyAndUids(w2v, model)

    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== vector2 absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("Word2Vec-CBOW") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val numOfWords = sentence.split(" ").size
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(1.1255356073379517, -2.1618645191192627, 0.34989115595817566),
      "b" -> Array(-1.222102165222168, 1.4987282752990723, 2.844104766845703),
      "c" -> Array(-0.8431543111801147, 0.8495243787765503, 1.5596754550933838)
    )

    val expected = doc.map { sentence =>
      Vectors.dense(sentence.map(codes.apply).reduce((word1, word2) =>
        word1.zip(word2).map { case (v1, v2) => v1 + v2 }
      ).map(_ / numOfWords))
    }

    val docDF = doc.zip(expected).toDF("text", "expected")

    val w2v = new Word2Vec()
      .setSolver("cbow-ns")
      .setNegativeSamples(2)
      .setMaxUnigramTableSize(10000)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
    val model = w2v.fit(docDF)

    MLTestingUtils.checkCopyAndUids(w2v, model)

    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    model.transform(docDF).select("result", "expected").collect().foreach {
      case Row(vector1: Vector, vector2: Vector) =>
        assert(vector1 ~== vector2 absTol 1E-5, "Transformed vector is different with expected.")
    }
  }

  test("getVectors") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(-0.2811822295188904, -0.6356269121170044, -0.3020961284637451),
      "b" -> Array(1.0309048891067505, -1.29472815990448, 0.22276712954044342),
      "c" -> Array(-0.08456747233867645, 0.5137411952018738, 0.11731560528278351)
    )
    val expectedVectors = codes.toSeq.sortBy(_._1).map { case (w, v) => Vectors.dense(v) }

    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val realVectors = model.getVectors.sort("word").select("vector").rdd.map {
      case Row(v: Vector) => v
    }.collect()
    // These expectations are just magic values, characterizing the current
    // behavior.  The test needs to be updated to be more general, see SPARK-11502
    val magicExpected = Seq(
      Vectors.dense(0.3326166272163391, -0.5603077411651611, -0.2309209555387497),
      Vectors.dense(0.32463887333869934, -0.9306551218032837, 1.393115520477295),
      Vectors.dense(-0.27150997519493103, 0.4372006058692932, -0.13465698063373566)
    )

    realVectors.zip(magicExpected).foreach {
      case (real, expected) =>
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("getVectors-SkipGram-NegativeSampling") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(0.2554479241371155, -0.7374975085258484, 0.501385509967804),
      "b" -> Array(-0.12191832810640335, -0.09346947073936462, 1.6055092811584473),
      "c" -> Array(-0.3807409405708313, -0.004864220507442951, 0.7893226146697998)
    )
    val expectedVectors = codes.toSeq.sortBy(_._1).map { case (w, v) => Vectors.dense(v) }

    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val realVectors = model.getVectors.sort("word").select("vector").rdd.map {
      case Row(v: Vector) => v
    }.collect()

    realVectors.zip(expectedVectors).foreach {
      case (real, expected) =>
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("getVectors-CBOW") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))

    val codes = Map(
      "a" -> Array(1.1255356073379517, -2.1618645191192627, 0.34989115595817566),
      "b" -> Array(-1.222102165222168, 1.4987282752990723, 2.844104766845703),
      "c" -> Array(-0.8431543111801147, 0.8495243787765503, 1.5596754550933838)
    )
    val expectedVectors = codes.toSeq.sortBy(_._1).map { case (w, v) => Vectors.dense(v) }

    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val realVectors = model.getVectors.sort("word").select("vector").rdd.map {
      case Row(v: Vector) => v
    }.collect()

    realVectors.zip(expectedVectors).foreach {
      case (real, expected) =>
        assert(real ~== expected absTol 1E-5, "Actual vector is different from expected.")
    }
  }

  test("findSynonyms") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val expected = Map(("b", 0.2608488929093532), ("c", -0.8271274846926078))
    val findSynonymsResult = model.findSynonyms("a", 2).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collectAsMap()

    expected.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsResult.get(expectedSynonym).get absTol 1E-5)
    }

    val findSynonymsArrayResult = model.findSynonymsArray("a", 2).toMap
    findSynonymsResult.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsArrayResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsArrayResult.get(expectedSynonym).get absTol 1E-5)
    }
  }

  test("findSynonyms-SkipGram-NegativeSampling") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val expected = Map(("b", 0.5632874965667725), ("c", 0.37158143520355225))
    val findSynonymsResult = model.findSynonyms("a", 2).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collectAsMap()

    expected.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsResult.get(expectedSynonym).get absTol 1E-5)
    }

    val findSynonymsArrayResult = model.findSynonymsArray("a", 2).toMap
    findSynonymsResult.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsArrayResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsArrayResult.get(expectedSynonym).get absTol 1E-5)
    }
  }

  test("findSynonyms-CBOW") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a b " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val expected = Map(("b", -0.4275166988372803), ("c", -0.4626910090446472))
    val findSynonymsResult = model.findSynonyms("a", 2).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collectAsMap()

    expected.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsResult.get(expectedSynonym).get absTol 1E-5)
    }

    val findSynonymsArrayResult = model.findSynonymsArray("a", 2).toMap
    findSynonymsResult.foreach {
      case (expectedSynonym, expectedSimilarity) =>
        assert(findSynonymsArrayResult.contains(expectedSynonym))
        assert(expectedSimilarity ~== findSynonymsArrayResult.get(expectedSynonym).get absTol 1E-5)
    }
  }

  test("window size") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(10)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the larger window
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("window size - SkipGram-NegativeSampling") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(10)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the larger window
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("window size - CBOW") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(10)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the larger window
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("negative sampling - SkipGram - Negative Sampling") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(2)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the higher negative sampling
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("negative sampling - CBOW") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Increase the window size
    val biggerModel = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(5)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(2)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the higher negative sampling
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("sub sampling - SkipGram-NegativeSampling") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Set sub sampling
    val biggerModel = new Word2Vec()
      .setSolver("sg-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(2)
      .setSample(0.01)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the higher negative sampling
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("sub sampling - CBOW") {

    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q " * 100 + "a c " * 10
    val doc = sc.parallelize(Seq(sentence, sentence)).map(line => line.split(" "))
    val docDF = doc.zip(doc).toDF("text", "alsotext")

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setWindowSize(2)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .fit(docDF)

    val (synonyms, similarity) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip

    // Set sub sampling
    val biggerModel = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(3)
      .setInputCol("text")
      .setOutputCol("result")
      .setSeed(42L)
      .setWindowSize(2)
      .setSample(0.01)
      .fit(docDF)

    val (synonymsLarger, similarityLarger) = model.findSynonyms("a", 6).rdd.map {
      case Row(w: String, sim: Double) => (w, sim)
    }.collect().unzip
    // The similarity score should be very different with the subsampling
    assert(math.abs(similarity(5) - similarityLarger(5) / similarity(5)) > 1E-5)
  }

  test("Word2Vec read/write numPartitions calculation") {
    val smallModelNumPartitions = Word2VecModel.Word2VecModelWriter.calculateNumberOfPartitions(
      Utils.byteStringAsBytes("64m"), numWords = 10, vectorSize = 5)
    assert(smallModelNumPartitions === 1)
    val largeModelNumPartitions = Word2VecModel.Word2VecModelWriter.calculateNumberOfPartitions(
      Utils.byteStringAsBytes("64m"), numWords = 1000000, vectorSize = 5000)
    assert(largeModelNumPartitions > 1)
  }

  test("Word2Vec read/write") {
    val t = new Word2Vec()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMaxIter(2)
      .setMinCount(8)
      .setNumPartitions(1)
      .setSeed(42L)
      .setStepSize(0.01)
      .setVectorSize(100)
      .setMaxSentenceLength(500)
    testDefaultReadWrite(t)
  }

  test("Word2Vec read/write - CBOW") {
    val t = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(10)
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMaxIter(2)
      .setMinCount(8)
      .setNumPartitions(1)
      .setSeed(42L)
      .setStepSize(0.01)
      .setVectorSize(100)
      .setMaxSentenceLength(500)
    testDefaultReadWrite(t)
  }

  test("Word2VecModel read/write") {
    val word2VecMap = Map(
      ("china", Array(0.50f, 0.50f, 0.50f, 0.50f)),
      ("japan", Array(0.40f, 0.50f, 0.50f, 0.50f)),
      ("taiwan", Array(0.60f, 0.50f, 0.50f, 0.50f)),
      ("korea", Array(0.45f, 0.60f, 0.60f, 0.60f))
    )
    val oldModel = new OldWord2VecModel(word2VecMap)
    val instance = new Word2VecModel("myWord2VecModel", oldModel)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.getVectors.collect() === instance.getVectors.collect())
  }

  test("Word2Vec works with input that is non-nullable (NGram)") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q "
    val docDF = sc.parallelize(Seq(sentence, sentence)).map(_.split(" ")).toDF("text")

    val ngram = new NGram().setN(2).setInputCol("text").setOutputCol("ngrams")
    val ngramDF = ngram.transform(docDF)

    val model = new Word2Vec()
      .setVectorSize(2)
      .setInputCol("ngrams")
      .setOutputCol("result")
      .fit(ngramDF)

    // Just test that this transformation succeeds
    model.transform(ngramDF).collect()
  }

  test("Word2Vec works with input that is non-nullable (NGram) - CBOW") {
    val spark = this.spark
    import spark.implicits._

    val sentence = "a q s t q s t b b b s t m s t m q "
    val docDF = sc.parallelize(Seq(sentence, sentence)).map(_.split(" ")).toDF("text")

    val ngram = new NGram().setN(2).setInputCol("text").setOutputCol("ngrams")
    val ngramDF = ngram.transform(docDF)

    val model = new Word2Vec()
      .setSolver("cbow-ns")
      .setMaxUnigramTableSize(10000)
      .setNegativeSamples(2)
      .setVectorSize(2)
      .setInputCol("ngrams")
      .setOutputCol("result")
      .setMinCount(1)
      .fit(ngramDF)

    // Just test that this transformation succeeds
    model.transform(ngramDF).collect()
  }
}

