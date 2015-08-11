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

package org.apache.spark.sql.hive.orc

import java.io.File

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.SharedHiveContext

case class OrcData(intField: Int, stringField: String)

abstract class OrcSuite extends QueryTest with SharedHiveContext {
  import testImplicits._

  var orcTableDir: File = null
  var orcTableAsDir: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    orcTableAsDir = File.createTempFile("orctests", "sparksql")
    orcTableAsDir.delete()
    orcTableAsDir.mkdir()

    // Hack: to prepare orc data files using hive external tables
    orcTableDir = File.createTempFile("orctests", "sparksql")
    orcTableDir.delete()
    orcTableDir.mkdir()

    ctx.sparkContext
      .makeRDD(1 to 10)
      .map(i => OrcData(i, s"part-$i"))
      .toDF()
      .registerTempTable(s"orc_temp_table")

    ctx.sql(
      s"""CREATE EXTERNAL TABLE normal_orc(
         |  intField INT,
         |  stringField STRING
         |)
         |STORED AS ORC
         |LOCATION '${orcTableAsDir.getCanonicalPath}'
       """.stripMargin)

    ctx.sql(
      s"""INSERT INTO TABLE normal_orc
         |SELECT intField, stringField FROM orc_temp_table
       """.stripMargin)
  }

  override def afterAll(): Unit = {
    orcTableDir.delete()
    orcTableAsDir.delete()
  }

  test("create temporary orc table") {
    checkAnswer(ctx.sql("SELECT COUNT(*) FROM normal_orc_source"), Row(10))

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_source where intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      ctx.sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("create temporary orc table as") {
    checkAnswer(ctx.sql("SELECT COUNT(*) FROM normal_orc_as_source"), Row(10))

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_source WHERE intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      ctx.sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("appending insert") {
    ctx.sql("INSERT INTO TABLE normal_orc_source SELECT * FROM orc_temp_table WHERE intField > 5")

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_source"),
      (1 to 5).map(i => Row(i, s"part-$i")) ++ (6 to 10).flatMap { i =>
        Seq.fill(2)(Row(i, s"part-$i"))
      })
  }

  test("overwrite insert") {
    ctx.sql(
      """INSERT OVERWRITE TABLE normal_orc_as_source
        |SELECT * FROM orc_temp_table WHERE intField > 5
      """.stripMargin)

    checkAnswer(
      ctx.sql("SELECT * FROM normal_orc_as_source"),
      (6 to 10).map(i => Row(i, s"part-$i")))
  }
}

class OrcSourceSuite extends OrcSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()

    ctx.sql(
      s"""CREATE TEMPORARY TABLE normal_orc_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).getCanonicalPath}'
         |)
       """.stripMargin)

    ctx.sql(
      s"""CREATE TEMPORARY TABLE normal_orc_as_source
         |USING org.apache.spark.sql.hive.orc
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).getCanonicalPath}'
         |)
       """.stripMargin)
  }
}
