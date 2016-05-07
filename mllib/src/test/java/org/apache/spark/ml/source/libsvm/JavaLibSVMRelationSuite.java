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

package org.apache.spark.ml.source.libsvm;

import com.google.common.io.Files;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * Test LibSVMRelation in Java.
 */
public class JavaLibSVMRelationSuite {
  private transient SparkSession spark;

  private File tempDir;
  private String path;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
      .master("local")
      .appName("JavaLibSVMRelationSuite")
      .getOrCreate();

    tempDir = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "datasource");
    File file = new File(tempDir, "part-00000");
    String s = "1 1:1.0 3:2.0 5:3.0\n0\n0 2:4.0 4:5.0 6:6.0";
    Files.write(s, file, StandardCharsets.UTF_8);
    path = tempDir.toURI().toString();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
    Utils.deleteRecursively(tempDir);
  }

  @Test
  public void verifyLibSVMDF() {
    Dataset<Row> dataset = spark.read().format("libsvm").option("vectorType", "dense")
      .load(path);
    Assert.assertEquals("label", dataset.columns()[0]);
    Assert.assertEquals("features", dataset.columns()[1]);
    Row r = dataset.first();
    Assert.assertEquals(1.0, r.getDouble(0), 1e-15);
    DenseVector v = r.getAs(1);
    Assert.assertEquals(Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0), v);
  }
}
