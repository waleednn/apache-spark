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

package org.apache.spark.sql.execution

import java.sql.{Date, Timestamp}

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TaskContext, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class BaseScriptTransformationSuite extends SparkPlanTest with SQLTestUtils
  with BeforeAndAfterEach {
  import testImplicits._
  import ScriptTransformationIOSchema._

  protected val uncaughtExceptionHandler = new TestUncaughtExceptionHandler

  private var defaultUncaughtExceptionHandler: Thread.UncaughtExceptionHandler = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    defaultUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    Thread.setDefaultUncaughtExceptionHandler(defaultUncaughtExceptionHandler)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    uncaughtExceptionHandler.cleanStatus()
  }

  def isHive23OrSpark: Boolean

  def createScriptTransformationExec(
      input: Seq[Expression],
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec

  test("cat without SerDe") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    checkAnswer(
      rowsDf,
      (child: SparkPlan) => createScriptTransformationExec(
        input = Seq(rowsDf.col("a").expr),
        script = "cat",
        output = Seq(AttributeReference("a", StringType)()),
        child = child,
        ioschema = defaultIOSchema
      ),
      rowsDf.collect())
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("script transformation should not swallow errors from upstream operators (no serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[TestFailedException] {
      checkAnswer(
        rowsDf,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "cat",
          output = Seq(AttributeReference("a", StringType)()),
          child = ExceptionInjectingOperator(child),
          ioschema = defaultIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
    // Before SPARK-25158, uncaughtExceptionHandler will catch IllegalArgumentException
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-25990: TRANSFORM should handle different data types correctly") {
    assume(TestUtils.testCommandAvailable("python"))
    val scriptFilePath = getTestResourcePath("test_script.py")

    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      val query = sql(
        s"""
           |SELECT
           |TRANSFORM(a, b, c, d, e)
           |USING 'python $scriptFilePath' AS (a, b, c, d, e)
           |FROM v
        """.stripMargin)

      // In Hive 1.2, the string representation of a decimal omits trailing zeroes.
      // But in Hive 2.3, it is always padded to 18 digits with trailing zeroes if necessary.
      val decimalToString: Column => Column = if (isHive23OrSpark) {
        c => c.cast("string")
      } else {
        c => c.cast("decimal(1, 0)").cast("string")
      }
      checkAnswer(query, identity, df.select(
        'a.cast("string"),
        'b.cast("string"),
        'c.cast("string"),
        decimalToString('d),
        'e.cast("string")).collect())
    }
  }

  test("SPARK-25990: TRANSFORM should handle schema less correctly") {
    assume(TestUtils.testCommandAvailable("python"))
    val scriptFilePath = getTestResourcePath("test_script.py")

    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)

      // In Hive 1.2, the string representation of a decimal omits trailing zeroes.
      // But in Hive 2.3, it is always padded to 18 digits with trailing zeroes if necessary.
      val decimalToString: Column => Column = if (isHive23OrSpark) {
        c => c.cast("string")
      } else {
        c => c.cast("decimal(1, 0)").cast("string")
      }

      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr,
            df.col("b").expr,
            df.col("c").expr,
            df.col("d").expr,
            df.col("e").expr),
          script = s"python $scriptFilePath",
          output = Seq(
            AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()),
          child = child,
          ioschema = defaultIOSchema.copy(schemaLess = true)
        ),
        df.select(
          'a.cast("string").as("key"),
          concat_ws("\t",
            'b.cast("string"),
            'c.cast("string"),
            decimalToString('d),
            'e.cast("string"))).collect())
    }
  }

  test("SPARK-30973: TRANSFORM should wait for the termination of the script (no serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[SparkException] {
      val plan =
        createScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "some_non_existent_command",
          output = Seq(AttributeReference("a", StringType)()),
          child = rowsDf.queryExecution.sparkPlan,
          ioschema = defaultIOSchema)
      SparkPlanTest.executePlan(plan, spark.sqlContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-32106: TRANSFORM should support more data types (no serde)") {
    assume(TestUtils.testCommandAvailable("python"))
    case class Struct(d: Int, str: String)
    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, 11.toByte, BigDecimal(1.0), new Timestamp(1),
          new Date(2020, 7, 1), new CalendarInterval(7, 1, 1000), Array(0, 1, 2),
          Map("a" -> 1), new TestUDT.MyDenseVector(Array(1, 2, 3)), new SimpleTuple(1, 1L)),
        (2, "2", 2.0, 22.toByte, BigDecimal(2.0), new Timestamp(2),
          new Date(2020, 7, 2), new CalendarInterval(7, 2, 2000), Array(3, 4, 5),
          Map("b" -> 2), new TestUDT.MyDenseVector(Array(1, 2, 3)), new SimpleTuple(1, 1L)),
        (3, "3", 3.0, 33.toByte, BigDecimal(3.0), new Timestamp(3),
          new Date(2020, 7, 3), new CalendarInterval(7, 3, 3000), Array(6, 7, 8),
          Map("c" -> 3), new TestUDT.MyDenseVector(Array(1, 2, 3)), new SimpleTuple(1, 1L))
      ).toDF("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l")
        .select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h, 'i, 'j, 'k, 'l,
          struct('a, 'b).as("m"), unhex('a).as("n"), lit(true).as("o")
        )
      // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      assert(spark.table("v").schema ==
        StructType(Seq(
          StructField("a", IntegerType, false),
          StructField("b", StringType, true),
          StructField("c", DoubleType, false),
          StructField("d", ByteType, false),
          StructField("e", DecimalType(38, 18), true),
          StructField("f", TimestampType, true),
          StructField("g", DateType, true),
          StructField("h", CalendarIntervalType, true),
          StructField("i", ArrayType(IntegerType, false), true),
          StructField("j", MapType(StringType, IntegerType, false), true),
          StructField("k", new TestUDT.MyDenseVectorUDT, true),
          StructField("l", new SimpleTupleUDT, true),
          StructField("m", StructType(
            Seq(StructField("a", IntegerType, false),
              StructField("b", StringType, true))), false),
          StructField("n", BinaryType, true),
          StructField("o", BooleanType, false))))

      // Can't support convert script output data to ArrayType/MapType/StructType now,
      // return these column still as string.
      // For UserDefinedType, if user defined deserialize method to support convert string
      // to UserType like [[SimpleTupleUDT]], we can support convert to this UDT, else we
      // will return null value as column.
      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr,
            df.col("b").expr,
            df.col("c").expr,
            df.col("d").expr,
            df.col("e").expr,
            df.col("f").expr,
            df.col("g").expr,
            df.col("h").expr,
            df.col("i").expr,
            df.col("j").expr,
            df.col("k").expr,
            df.col("l").expr,
            df.col("m").expr,
            df.col("n").expr,
            df.col("o").expr),
          script = "cat",
          output = Seq(
            AttributeReference("a", IntegerType)(),
            AttributeReference("b", StringType)(),
            AttributeReference("c", DoubleType)(),
            AttributeReference("d", ByteType)(),
            AttributeReference("e", DecimalType(38, 18))(),
            AttributeReference("f", TimestampType)(),
            AttributeReference("g", DateType)(),
            AttributeReference("h", CalendarIntervalType)(),
            AttributeReference("i", StringType)(),
            AttributeReference("j", StringType)(),
            AttributeReference("k", StringType)(),
            AttributeReference("l", new SimpleTupleUDT)(),
            AttributeReference("m", StringType)(),
            AttributeReference("n", BinaryType)(),
            AttributeReference("o", BooleanType)()),
          child = child,
          ioschema = defaultIOSchema
        ),
        df.select(
          'a, 'b, 'c, 'd, 'e, 'f, 'g, 'h,
          'i.cast("string"),
          'j.cast("string"),
          'k.cast("string"),
          'l, 'm.cast("string"), 'n, 'o).collect())
    }
  }

  test("SPARK-32106: TRANSFORM shoud return null when return string incompatible(no serde)") {
    checkAnswer(
      sql(
        """
          |SELECT TRANSFORM(a, b, c)
          |USING 'cat' as (a int, b int , c int)
          |FROM (
          |SELECT
          |1 AS a,
          |"a" AS b,
          |CAST(2000 AS timestamp) AS c
          |) tmp
        """.stripMargin),
      identity,
      Row(1, null, null) :: Nil)
  }
}

case class ExceptionInjectingOperator(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().map { x =>
      assert(TaskContext.get() != null) // Make sure that TaskContext is defined.
      Thread.sleep(1000) // This sleep gives the external process time to start.
      throw new IllegalArgumentException("intentional exception")
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}

@SQLUserDefinedType(udt = classOf[SimpleTupleUDT])
private class SimpleTuple(val id: Int, val size: Long) extends Serializable {

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match {
    case v: SimpleTuple => this.id == v.id && this.size == v.size
    case _ => false
  }

  override def toString: String =
    compact(render(
      ("id" -> id) ~
        ("size" -> size)
    ))
}

private class SimpleTupleUDT extends UserDefinedType[SimpleTuple] {

  override def sqlType: DataType = StructType(
    StructField("id", IntegerType, false) ::
      StructField("size", LongType, false) ::
      Nil)

  override def serialize(sql: SimpleTuple): Any = {
    val row = new GenericInternalRow(2)
    row.setInt(0, sql.id)
    row.setLong(1, sql.size)
    row
  }

  override def deserialize(datum: Any): SimpleTuple = {
    datum match {
      case str: String =>
        implicit val format = DefaultFormats
        val json = parse(str)
        new SimpleTuple((json \ "id").extract[Int], (json \ "size").extract[Long])
      case data: InternalRow if data.numFields == 2 =>
        new SimpleTuple(data.getInt(0), data.getLong(1))
      case _ => null
    }
  }

  override def userClass: Class[SimpleTuple] = classOf[SimpleTuple]

  override def asNullable: SimpleTupleUDT = this

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[SimpleTupleUDT]
  }
}

