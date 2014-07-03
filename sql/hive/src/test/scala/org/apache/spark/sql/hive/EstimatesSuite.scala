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

package org.apache.spark.sql.hive

import scala.reflect.ClassTag

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.BroadcastHashJoin
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.parquet.{ParquetRelation, ParquetTestData}
import org.apache.spark.util.Utils

class EstimatesSuite extends QueryTest {

  test("estimates the size of a test ParquetRelation") {
    ParquetTestData.writeFile()
    val testRDD = parquetFile(ParquetTestData.testDir.toString)

    val sizes = testRDD.logicalPlan.collect { case j: ParquetRelation =>
      (j.estimates.sizeInBytes, j.newInstance.estimates.sizeInBytes)
    }
    assert(sizes.size === 1)
    assert(sizes(0)._1 == sizes(0)._2, "after .newInstance, estimates are different from before")
    assert(sizes(0)._1 > 0)

    Utils.deleteRecursively(ParquetTestData.testDir)
  }

  test("estimates the size of a test MetastoreRelation") {
    val rdd = hql("""SELECT * FROM src""")
    val sizes = rdd.queryExecution.analyzed.collect { case mr: MetastoreRelation =>
      mr.estimates.sizeInBytes
    }
    assert(sizes.size === 1 && sizes(0) > 0)
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Any],
        ct: ClassTag[_]) = {
      before()

      var rdd = hql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = rdd.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) =>
          r.estimates.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= autoConvertJoinSize,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = rdd.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${rdd.queryExecution}")

      checkAnswer(rdd, expectedAnswer)

      // TODO(zongheng): synchronize on TestHive.settings, or use Sequential/Stepwise.
      val tmp = autoConvertJoinSize
      hql("""SET spark.sql.auto.convert.join.size=0""")
      rdd = hql(query)
      bhj = rdd.queryExecution.sparkPlan.collect { case j: BroadcastHashJoin => j }
      assert(bhj.isEmpty)

      hql(s"""SET spark.sql.auto.convert.join.size=$tmp""")

      after()
    }

    /** Tests for ParquetRelation */
    val parquetQuery =
      """SELECT a.mystring, b.myint
        |FROM psrc a
        |JOIN psrc b
        |ON a.mylong = 0 AND a.mylong = b.mylong""".stripMargin
    val parquetAnswer = Seq(("abc", 5))
    def parquetBefore(): Unit = {
      ParquetTestData.writeFile()
      val testRDD = parquetFile(ParquetTestData.testDir.toString)
      testRDD.registerAsTable("psrc")
    }
    def parquetAfter() = {
      Utils.deleteRecursively(ParquetTestData.testDir)
      reset()
    }
    mkTest(
      parquetBefore,
      parquetAfter,
      parquetQuery,
      parquetAnswer,
      implicitly[ClassTag[ParquetRelation]]
    )

    /** Tests for MetastoreRelation */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)((238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[MetastoreRelation]]
    )
  }

}
