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

package org.apache.spark.ml.source

import java.io.File

import com.google.common.base.Charsets
import com.google.common.io.Files
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.source.libsvm._
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, DenseVector}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.Utils

class LibSVMRelationSuite extends SparkFunSuite with MLlibTestSparkContext {
  var path: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val lines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    path = tempDir.toURI.toString
  }

  test("select as sparse vector") {
    val df = sqlContext.read.libsvm(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("select as dense vector") {
    val df = sqlContext.read.options(Map("vectorType" -> "dense"))
      .libsvm(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    assert(df.count() == 3)
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[DenseVector](1)
    assert(v == Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0))
  }

  test("select long vector with specifying the number of features") {
    val lines =
      """
        |1 1:1 10:2 20:3 30:4 40:5 50:6 60:7 70:8 80:9 90:10 100:1
        |0 1:1 10:10 20:9 30:8 40:7 50:6 60:5 70:4 80:3 90:2 100:1
      """.stripMargin
    val tempDir = Utils.createTempDir()
    val file = new File(tempDir.getPath, "part-00001")
    Files.write(lines, file, Charsets.US_ASCII)
    val df = sqlContext.read.option("numFeatures", "100").libsvm(tempDir.toURI.toString)
    val row1 = df.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(100, Seq((0, 1.0), (9, 2.0), (19, 3.0), (29, 4.0), (39, 5.0),
      (49, 6.0), (59, 7.0), (69, 8.0), (79, 9.0), (89, 10.0), (99, 1.0))))
  }
}
