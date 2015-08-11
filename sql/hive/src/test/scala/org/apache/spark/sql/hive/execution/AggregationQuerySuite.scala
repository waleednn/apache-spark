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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{stddev, stddevPop}
import org.scalatest.BeforeAndAfterAll
import _root_.test.org.apache.spark.sql.hive.aggregate.{MyDoubleAvg, MyDoubleSum}

abstract class AggregationQuerySuite extends QueryTest with SQLTestUtils with BeforeAndAfterAll {

  override val sqlContext = TestHive
  import sqlContext.implicits._

  var originalUseAggregate2: Boolean = _

  override def beforeAll(): Unit = {
    originalUseAggregate2 = sqlContext.conf.useSqlAggregate2
    sqlContext.setConf(SQLConf.USE_SQL_AGGREGATE2.key, "true")
    val data1 = Seq[(Integer, Integer)](
      (1, 10),
      (null, -60),
      (1, 20),
      (1, 30),
      (2, 0),
      (null, -10),
      (2, -1),
      (2, null),
      (2, null),
      (null, 100),
      (3, null),
      (null, null),
      (3, null)).toDF("key", "value")
    data1.write.saveAsTable("agg1")

    val data2 = Seq[(Integer, Integer, Integer)](
      (1, 10, -10),
      (null, -60, 60),
      (1, 30, -30),
      (1, 30, 30),
      (2, 1, 1),
      (null, -10, 10),
      (2, -1, null),
      (2, 1, 1),
      (2, null, 1),
      (null, 100, -10),
      (3, null, 3),
      (null, null, null),
      (3, null, null)).toDF("key", "value1", "value2")
    data2.write.saveAsTable("agg2")

    val emptyDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.emptyRDD[Row],
      StructType(StructField("key", StringType) :: StructField("value", IntegerType) :: Nil))
    emptyDF.registerTempTable("emptyTable")

    // Register UDAFs
    sqlContext.udf.register("mydoublesum", new MyDoubleSum)
    sqlContext.udf.register("mydoubleavg", new MyDoubleAvg)
  }

  override def afterAll(): Unit = {
    sqlContext.sql("DROP TABLE IF EXISTS agg1")
    sqlContext.sql("DROP TABLE IF EXISTS agg2")
    sqlContext.dropTempTable("emptyTable")
    sqlContext.setConf(SQLConf.USE_SQL_AGGREGATE2.key, originalUseAggregate2.toString)
  }

  test("empty table") {
    // If there is no GROUP BY clause and the table is empty, we will generate a single row.
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(key),
          |  COUNT(value),
          |  FIRST(key),
          |  LAST(value),
          |  MAX(key),
          |  MIN(value),
          |  SUM(key)
          |FROM emptyTable
        """.stripMargin),
      Row(null, 0, 0, 0, null, null, null, null, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(key),
          |  COUNT(value),
          |  FIRST(key),
          |  LAST(value),
          |  MAX(key),
          |  MIN(value),
          |  SUM(key),
          |  COUNT(DISTINCT value)
          |FROM emptyTable
        """.stripMargin),
      Row(null, 0, 0, 0, null, null, null, null, null, 0) :: Nil)

    // If there is a GROUP BY clause and the table is empty, there is no output.
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(value),
          |  COUNT(*),
          |  COUNT(value),
          |  FIRST(value),
          |  LAST(value),
          |  MAX(value),
          |  MIN(value),
          |  SUM(value),
          |  COUNT(DISTINCT value)
          |FROM emptyTable
          |GROUP BY key
        """.stripMargin),
      Nil)
  }

  test("null literal") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  AVG(null),
          |  COUNT(null),
          |  FIRST(null),
          |  LAST(null),
          |  MAX(null),
          |  MIN(null),
          |  SUM(null)
        """.stripMargin),
      Row(null, 0, null, null, null, null, null) :: Nil)
  }

  test("only do grouping") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT DISTINCT value1, key
          |FROM agg2
        """.stripMargin),
      Row(10, 1) ::
        Row(-60, null) ::
        Row(30, 1) ::
        Row(1, 2) ::
        Row(-10, null) ::
        Row(-1, 2) ::
        Row(null, 2) ::
        Row(100, null) ::
        Row(null, 3) ::
        Row(null, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT value1, key
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(10, 1) ::
        Row(-60, null) ::
        Row(30, 1) ::
        Row(1, 2) ::
        Row(-10, null) ::
        Row(-1, 2) ::
        Row(null, 2) ::
        Row(100, null) ::
        Row(null, 3) ::
        Row(null, null) :: Nil)
  }

  test("case in-sensitive resolution") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value), kEY - 100
          |FROM agg1
          |GROUP BY Key - 100
        """.stripMargin),
      Row(20.0, -99) :: Row(-0.5, -98) :: Row(null, -97) :: Row(10.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT sum(distinct value1), kEY - 100, count(distinct value1)
          |FROM agg2
          |GROUP BY Key - 100
        """.stripMargin),
      Row(40, -99, 2) :: Row(0, -98, 2) :: Row(null, -97, 0) :: Row(30, null, 3) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT valUe * key - 100
          |FROM agg1
          |GROUP BY vAlue * keY - 100
        """.stripMargin),
      Row(-90) ::
        Row(-80) ::
        Row(-70) ::
        Row(-100) ::
        Row(-102) ::
        Row(null) :: Nil)
  }

  test("test average no key in output") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(-0.5) :: Row(20.0) :: Row(null) :: Row(10.0) :: Nil)
  }

  test("test average") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT key, avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1, 20.0) :: Row(2, -0.5) :: Row(3, null) :: Row(null, 10.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value), key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(20.0, 1) :: Row(-0.5, 2) :: Row(null, 3) :: Row(10.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value) + 1.5, key + 10
          |FROM agg1
          |GROUP BY key + 10
        """.stripMargin),
      Row(21.5, 11) :: Row(1.0, 12) :: Row(null, 13) :: Row(11.5, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT avg(value) FROM agg1
        """.stripMargin),
      Row(11.125) :: Nil)
  }

  /** For resilience against rounding mismatches. */
  private def about(d: Double): BigDecimal =
    BigDecimal(d).setScale(10, BigDecimal.RoundingMode.HALF_UP)

  test("test standard deviation") {
    // All results generated in R. Comparisons will be performed up to 10 digits of precision.
    val df = Seq.tabulate(10)(i => (i, 1)).toDF("val", "key")
    checkAnswer(
      df.select(stddev("val").cast("decimal(12, 10)")),
      Row(about(3.0276503540974917)) :: Nil)

    checkAnswer(
      df.select(stddevPop("val").cast("decimal(12, 10)")),
      Row(about(2.8722813232690148)) :: Nil)

    // Make sure we can use stddev functions in SQL.
    {
      val expectedGroup1 =
        Row(1, about(10.0), about(10.0), about(10.0), about(8.16496580927726))
      val expectedGroup2 =
        Row(
          2,
          about(0.7071067811865476),
          about(0.7071067811865476),
          about(0.7071067811865476),
          about(0.5))
      val expectedGroup3 = Row(3, null, null, null, null)
      val expectedGroupNull =
        Row(
          null,
          about(81.8535277187245),
          about(81.8535277187245),
          about(81.8535277187245),
          about(66.83312551921139))

      checkAnswer(
        sqlContext.sql(
          """
            |SELECT
            |  key,
            |  cast(std(value) as decimal(12, 10)),
            |  cast(stddev(value) as decimal(12, 10)),
            |  cast(stddev_samp(value) as decimal(12, 10)),
            |  cast(stddev_pop(value) as decimal(12, 10))
            |FROM agg1 GROUP BY key
          """.stripMargin),
        expectedGroup1 ::
          expectedGroup2 ::
          expectedGroup3 ::
          expectedGroupNull :: Nil)
    }

    checkAnswer(
      sqlContext.table("agg1").groupBy("key").stddevPop("value")
        .select($"key", $"stddev_pop(value)".cast("decimal(12, 10)")),
      Row(1, about(8.16496580927726)) :: Row(2, about(0.5)) :: Row(3, null) ::
        Row(null, about(66.83312551921139)) :: Nil)

    checkAnswer(
      sqlContext.table("agg1").select(stddev("key").cast("decimal(12, 10)"),
        stddev("value").cast("decimal(12, 10)")),
      Row(about(0.7817359599705717), about(44.898098909801135)) :: Nil)

    checkAnswer(
      sqlContext.table("agg1").select(stddevPop("key").cast("decimal(12, 10)"),
        stddevPop("value").cast("decimal(12, 10)")),
      Row(about(0.7370277311900889), about(41.99832585949111)) :: Nil)

    checkAnswer(
      sqlContext.table("agg2").groupBy("key", "value1").stddev("value2")
        .select($"key", $"value1", $"stddev_samp(value2)".cast("decimal(12, 10)")),
      Row(1, 10, null) :: Row(1, 30, about(42.42640687119285)) :: Row(2, -1, null) ::
        Row(2, 1, about(0.0)) :: Row(2, null, null) :: Row(3, null, null) :: Row(null, -10, null) ::
        Row(null, -60, null) :: Row(null, 100, null) :: Row(null, null, null) :: Nil)

    checkAnswer(
      sqlContext.table("agg2").groupBy("key", "value1").stddevPop("value2")
        .select($"key", $"value1", $"stddev_pop(value2)".cast("decimal(12, 10)")),
      Row(1, 10, about(0.0)) :: Row(1, 30, about(30.0)) :: Row(2, -1, null) ::
        Row(2, 1, about(0.0)) :: Row(2, null, about(0.0)) :: Row(3, null, about(0.0)) ::
        Row(null, -10, about(0.0)) :: Row(null, -60, about(0.0)) :: Row(null, 100, about(0.0)) ::
        Row(null, null, null) :: Nil)

    checkAnswer(
      sqlContext.table("emptyTable").select(stddev("value")),
      Row(null) :: Nil)

    checkAnswer(
      sqlContext.table("emptyTable").select(stddevPop("value")),
      Row(null) :: Nil)

    // stddev_samp returns null when there is a single input.
    // While, stddev_pop returns 0.0 when there is a single input.
    checkAnswer(
      sqlContext.sql("SELECT stddev_samp(1), stddev_pop(1)"),
      Row(null, 0.0) :: Nil
    )

    // TODO: Because we will first resolve stddev to Hive's GenericUDAF and it will
    // complain about stddev_samp(null) and stddev_pop(null). So, we comment out this
    // test. Once we remove AggregateExpression1, we will resolve them directly to
    // our native implementation. We should re-enable this test at that time.
    /*
    checkAnswer(
      sqlContext.sql("SELECT stddev_samp(null), stddev_pop(null)"),
      Row(null, null) :: Nil
    )
    */
  }

  test("udaf") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  key,
          |  mydoublesum(value + 1.5 * key),
          |  mydoubleavg(value),
          |  avg(value - key),
          |  mydoublesum(value - 1.5 * key),
          |  avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(1, 64.5, 120.0, 19.0, 55.5, 20.0) ::
        Row(2, 5.0, 99.5, -2.5, -7.0, -0.5) ::
        Row(3, null, null, null, null, null) ::
        Row(null, null, 110.0, null, null, 10.0) :: Nil)
  }

  test("non-AlgebraicAggregate aggreguate function") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value), key
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1) :: Row(-1.0, 2) :: Row(null, 3) :: Row(30.0, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value) FROM agg1
        """.stripMargin),
      Row(89.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(null)
        """.stripMargin),
      Row(null) :: Nil)
  }

  test("non-AlgebraicAggregate and AlgebraicAggregate aggreguate function") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT mydoublesum(value), key, avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(60.0, 1, 20.0) ::
        Row(-1.0, 2, -0.5) ::
        Row(null, 3, null) ::
        Row(30.0, null, 10.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  mydoublesum(value + 1.5 * key),
          |  avg(value - key),
          |  key,
          |  mydoublesum(value - 1.5 * key),
          |  avg(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin),
      Row(64.5, 19.0, 1, 55.5, 20.0) ::
        Row(5.0, -2.5, 2, -7.0, -0.5) ::
        Row(null, null, 3, null, null) ::
        Row(null, null, null, null, 10.0) :: Nil)
  }

  test("single distinct column set") {
    // DISTINCT is not meaningful with Max and Min, so we just ignore the DISTINCT keyword.
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  min(distinct value1),
          |  sum(distinct value1),
          |  avg(value1),
          |  avg(value2),
          |  max(distinct value1)
          |FROM agg2
        """.stripMargin),
      Row(-60, 70.0, 101.0/9.0, 5.6, 100))

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  mydoubleavg(distinct value1),
          |  avg(value1),
          |  avg(value2),
          |  key,
          |  mydoubleavg(value1 - 1),
          |  mydoubleavg(distinct value1) * 0.1,
          |  avg(value1 + value2)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(120.0, 70.0/3.0, -10.0/3.0, 1, 67.0/3.0 + 100.0, 12.0, 20.0) ::
        Row(100.0, 1.0/3.0, 1.0, 2, -2.0/3.0 + 100.0, 10.0, 2.0) ::
        Row(null, null, 3.0, 3, null, null, null) ::
        Row(110.0, 10.0, 20.0, null, 109.0, 11.0, 30.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  key,
          |  mydoubleavg(distinct value1),
          |  mydoublesum(value2),
          |  mydoublesum(distinct value1),
          |  mydoubleavg(distinct value1),
          |  mydoubleavg(value1)
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(1, 120.0, -10.0, 40.0, 120.0, 70.0/3.0 + 100.0) ::
        Row(2, 100.0, 3.0, 0.0, 100.0, 1.0/3.0 + 100.0) ::
        Row(3, null, 3.0, null, null, null) ::
        Row(null, 110.0, 60.0, 30.0, 110.0, 110.0) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  count(value1),
          |  count(*),
          |  count(1),
          |  count(DISTINCT value1),
          |  key
          |FROM agg2
          |GROUP BY key
        """.stripMargin),
      Row(3, 3, 3, 2, 1) ::
        Row(3, 4, 4, 2, 2) ::
        Row(0, 2, 2, 0, 3) ::
        Row(3, 4, 4, 3, null) :: Nil)
  }

  test("test count") {
    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  count(value2),
          |  value1,
          |  count(*),
          |  count(1),
          |  key
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(1, 10, 1, 1, 1) ::
        Row(1, -60, 1, 1, null) ::
        Row(2, 30, 2, 2, 1) ::
        Row(2, 1, 2, 2, 2) ::
        Row(1, -10, 1, 1, null) ::
        Row(0, -1, 1, 1, 2) ::
        Row(1, null, 1, 1, 2) ::
        Row(1, 100, 1, 1, null) ::
        Row(1, null, 2, 2, 3) ::
        Row(0, null, 1, 1, null) :: Nil)

    checkAnswer(
      sqlContext.sql(
        """
          |SELECT
          |  count(value2),
          |  value1,
          |  count(*),
          |  count(1),
          |  key,
          |  count(DISTINCT abs(value2))
          |FROM agg2
          |GROUP BY key, value1
        """.stripMargin),
      Row(1, 10, 1, 1, 1, 1) ::
        Row(1, -60, 1, 1, null, 1) ::
        Row(2, 30, 2, 2, 1, 1) ::
        Row(2, 1, 2, 2, 2, 1) ::
        Row(1, -10, 1, 1, null, 1) ::
        Row(0, -1, 1, 1, 2, 0) ::
        Row(1, null, 1, 1, 2, 1) ::
        Row(1, 100, 1, 1, null, 1) ::
        Row(1, null, 2, 2, 3, 1) ::
        Row(0, null, 1, 1, null, 0) :: Nil)
  }

  test("error handling") {
    withSQLConf("spark.sql.useAggregate2" -> "false") {
      val errorMessage = intercept[AnalysisException] {
        sqlContext.sql(
          """
            |SELECT
            |  key,
            |  sum(value + 1.5 * key),
            |  mydoublesum(value),
            |  mydoubleavg(value)
            |FROM agg1
            |GROUP BY key
          """.stripMargin).collect()
      }.getMessage
      assert(errorMessage.contains("implemented based on the new Aggregate Function interface"))
    }

    // TODO: once we support Hive UDAF in the new interface,
    // we can remove the following two tests.
    withSQLConf("spark.sql.useAggregate2" -> "true") {
      val errorMessage = intercept[AnalysisException] {
        sqlContext.sql(
          """
            |SELECT
            |  key,
            |  mydoublesum(value + 1.5 * key),
            |  variance(value)
            |FROM agg1
            |GROUP BY key
          """.stripMargin).collect()
      }.getMessage
      assert(errorMessage.contains("implemented based on the new Aggregate Function interface"))

      // This will fall back to the old aggregate
      val newAggregateOperators = sqlContext.sql(
        """
          |SELECT
          |  key,
          |  sum(value + 1.5 * key),
          |  variance(value)
          |FROM agg1
          |GROUP BY key
        """.stripMargin).queryExecution.executedPlan.collect {
        case agg: aggregate.SortBasedAggregate => agg
        case agg: aggregate.TungstenAggregate => agg
      }
      val message =
        "We should fallback to the old aggregation code path if " +
          "there is any aggregate function that cannot be converted to the new interface."
      assert(newAggregateOperators.isEmpty, message)
    }
  }
}

class SortBasedAggregationQuerySuite extends AggregationQuerySuite {

  var originalUnsafeEnabled: Boolean = _

  override def beforeAll(): Unit = {
    originalUnsafeEnabled = sqlContext.conf.unsafeEnabled
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, "false")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, originalUnsafeEnabled.toString)
  }
}

class TungstenAggregationQuerySuite extends AggregationQuerySuite {

  var originalUnsafeEnabled: Boolean = _

  override def beforeAll(): Unit = {
    originalUnsafeEnabled = sqlContext.conf.unsafeEnabled
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, originalUnsafeEnabled.toString)
  }
}

class TungstenAggregationQueryWithControlledFallbackSuite extends AggregationQuerySuite {

  var originalUnsafeEnabled: Boolean = _

  override def beforeAll(): Unit = {
    originalUnsafeEnabled = sqlContext.conf.unsafeEnabled
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, "true")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    sqlContext.setConf(SQLConf.UNSAFE_ENABLED.key, originalUnsafeEnabled.toString)
    sqlContext.conf.unsetConf("spark.sql.TungstenAggregate.testFallbackStartsAt")
  }

  override protected def checkAnswer(actual: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    (0 to 2).foreach { fallbackStartsAt =>
      sqlContext.setConf(
        "spark.sql.TungstenAggregate.testFallbackStartsAt",
        fallbackStartsAt.toString)

      // Create a new df to make sure its physical operator picks up
      // spark.sql.TungstenAggregate.testFallbackStartsAt.
      val newActual = DataFrame(sqlContext, actual.logicalPlan)

      QueryTest.checkAnswer(newActual, expectedAnswer) match {
        case Some(errorMessage) =>
          val newErrorMessage =
            s"""
              |The following aggregation query failed when using TungstenAggregate with
              |controlled fallback (it falls back to sort-based aggregation once it has processed
              |$fallbackStartsAt input rows). The query is
              |${actual.queryExecution}
              |
              |$errorMessage
            """.stripMargin

          fail(newErrorMessage)
        case None =>
      }
    }
  }

  // Override it to make sure we call the actually overridden checkAnswer.
  override protected def checkAnswer(df: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  // Override it to make sure we call the actually overridden checkAnswer.
  override protected def checkAnswer(df: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }
}
