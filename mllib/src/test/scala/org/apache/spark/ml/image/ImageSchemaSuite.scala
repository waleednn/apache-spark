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

package org.apache.spark.ml.image

import java.nio.file.Paths
import java.util.Arrays

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.image.ImageSchema._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class ImageSchemaSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Single column of images named "image"
  private val imageDFSchema =
    StructType(StructField("image", ImageSchema.columnSchema, true) :: Nil)
  private lazy val imagePath =
    Thread.currentThread().getContextClassLoader.getResource("test-data/images").getPath

  test("Smoke test: create basic ImageSchema dataframe") {
    val origin = "path"
    val width = 1
    val height = 1
    val nChannels = 3
    val data = Array[Byte](0, 0, 0)
    val mode = "CV_8UC3"

    // Internal Row corresponds to image StructType
    val rows = Seq(Row(Row(origin, height, width, nChannels, mode, data)),
      Row(Row(null, height, width, nChannels, mode, data)))
    val rdd = sc.makeRDD(rows)
    val df = spark.createDataFrame(rdd, imageDFSchema)

    assert(df.count === 2, "incorrect image count")
    assert(ImageSchema.isImageColumn(df, "image"), "data do not fit ImageSchema")
  }

  test("readImages count test") {
    var df = readImages(imagePath, recursive = false)
    assert(df.count === 1)

    df = readImages(imagePath, recursive = true, dropImageFailures = false)
    assert(df.count === 9)

    df = readImages(imagePath, recursive = true, dropImageFailures = true)
    val count100 = df.count
    assert(count100 === 7)

    df = readImages(imagePath, recursive = true, sampleRatio = 0.5, dropImageFailures = true)
    // Random number about half of the size of the original dataset
    val count50 = df.count
    assert(count50 > 0.2 * count100 && count50 < 0.8 * count100)
  }

  test("readImages partition test") {
    val df = readImages(imagePath, recursive = true, dropImageFailures = true, numPartitions = 3)
    assert(df.rdd.getNumPartitions === 3)
  }

  // Images with the different number of channels
  test("readImages pixel values test") {

    val images = readImages(imagePath + "/multi-channel/", recursive = false).collect

    images.foreach { rrow =>
      val row = rrow.getAs[Row](0)
      val filename = Paths.get(getOrigin(row)).getFileName().toString()
      if (firstBytes20.contains(filename)) {
        val mode = getMode(row)
        val bytes20 = getData(row).slice(0, 20)

        val (expectedMode, expectedBytes) = firstBytes20(filename)
        assert(expectedMode === mode, "mode of the image is not read correctly")
        assert(Arrays.equals(expectedBytes, bytes20), "incorrect numeric value for flattened image")
      }
    }
  }

  // number of channels and first 20 bytes of OpenCV representation
  // - default representation for 3-channel RGB images is BGR row-wise:
  //   (B00, G00, R00,      B10, G10, R10,      ...)
  // - default representation for 4-channel RGB images is BGRA row-wise:
  //   (B00, G00, R00, A00, B10, G10, R10, A00, ...)
  private val firstBytes20 = Map(
    "grayscale.jpg" ->
      (("CV_8UC1", Array[Byte](-2, -33, -61, -60, -59, -59, -64, -59, -66, -67, -73, -73, -62,
        -57, -60, -63, -53, -49, -55, -69))),
    "chr30.4.184.jpg" -> (("CV_8UC3",
      Array[Byte](-9, -3, -1, -43, -32, -28, -75, -60, -57, -78, -59, -56, -74, -59, -57,
        -71, -58, -56, -73, -64))),
    "BGRA.png" -> (("CV_8UC4",
      Array[Byte](-128, -128, -8, -1, -128, -128, -8, -1, -128,
        -128, -8, -1, 127, 127, -9, -1, 127, 127, -9, -1)))
  )
}
