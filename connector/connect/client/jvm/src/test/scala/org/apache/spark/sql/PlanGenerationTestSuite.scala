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
package org.apache.spark.sql

import java.nio.file.{Files, Path}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.util.Properties.versionNumberString

import com.google.protobuf.util.JsonFormat
import io.grpc.inprocess.InProcessChannelBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.{AnyFunSuite => ConnectFunSuite} // scalastyle:ignore funsuite

import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions => fn}
import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.types._

// scalastyle:off
/**
 * Test the plans generated by the client. This serves two purposes:
 *
 *   1. Make sure the generated plan matches our expectations. The generated JSON file can be used
 *      for this during review.
 *   1. Make sure the generated plans are stable. Changes to the generated plans should be rare.
 *      The generated plan is compared to the (previously) generated proto file; the test fails
 *      when they are different.
 *
 * If you need to re-generate the golden files, you need to set the SPARK_GENERATE_GOLDEN_FILES=1
 * environment variable before running this test, e.g.:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "connect-client-jvm/testOnly org.apache.spark.sql.PlanGenerationTestSuite"
 * }}}
 *
 * Note that the plan protos are used as the input for the `ProtoToParsedPlanTestSuite` in the
 * `connector/connect/server` module
 */
// scalastyle:on
class PlanGenerationTestSuite extends ConnectFunSuite with BeforeAndAfterAll with Logging {

  // Borrowed from SparkFunSuite
  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  private val scala = versionNumberString.substring(0, versionNumberString.indexOf(".", 2))

  // Borrowed from SparkFunSuite
  private def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected val baseResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "common",
      "src",
      "test",
      "resources",
      "query-tests").toAbsolutePath
  }

  protected val queryFilePath: Path = baseResourcePath.resolve("queries")

  // A relative path to /connector/connect/server, used by `ProtoToParsedPlanTestSuite` to run
  // with the datasource.
  protected val testDataPath: Path = java.nio.file.Paths.get(
    "../",
    "common",
    "src",
    "test",
    "resources",
    "query-tests",
    "test-data")

  private val printer = JsonFormat.printer()

  private var session: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val client = new SparkConnectClient(
      proto.UserContext.newBuilder().build(),
      InProcessChannelBuilder.forName("/dev/null").build())
    val builder = SparkSession.builder().client(client)
    session = builder.build()
  }

  override protected def afterAll(): Unit = {
    session.close()
    super.afterAll()
  }

  private def test(name: String)(f: => Dataset[_]): Unit = super.test(name) {
    val actual = f.plan.getRoot
    val goldenFile = queryFilePath.resolve(name.replace(' ', '_') + ".proto.bin")
    Try(readRelation(goldenFile)) match {
      case Success(expected) if expected == actual =>
      // Ok!
      case Success(_) if regenerateGoldenFiles =>
        logInfo("Rewriting Golden File")
        writeGoldenFile(goldenFile, actual)
      case Success(expected) =>
        fail(s"""
             |Expected and actual plans do not match:
             |
             |=== Expected Plan ===
             |$expected
             |
             |=== Actual Plan ===
             |$actual
             |""".stripMargin)
      case Failure(_) if regenerateGoldenFiles =>
        logInfo("Writing Golden File")
        writeGoldenFile(goldenFile, actual)
      case Failure(_) =>
        fail(
          "No golden file found. Please re-run this test with the " +
            "SPARK_GENERATE_GOLDEN_FILES=1 environment variable set")
    }
  }

  private def readRelation(path: Path): proto.Relation = {
    val input = Files.newInputStream(path)
    try proto.Relation.parseFrom(input)
    finally {
      input.close()
    }
  }

  private def writeGoldenFile(path: Path, relation: proto.Relation): Unit = {
    val output = Files.newOutputStream(path)
    try relation.writeTo(output)
    finally {
      output.close()
    }
    // Write the json file for verification.
    val jsonPath =
      path.getParent.resolve(path.getFileName.toString.stripSuffix(".proto.bin") + ".json")
    val writer = Files.newBufferedWriter(jsonPath)
    try writer.write(printer.print(relation))
    finally {
      writer.close()
    }
  }

  private val simpleSchema = new StructType()
    .add("id", "long")
    .add("a", "int")
    .add("b", "double")

  private val simpleSchemaString = simpleSchema.catalogString

  private val otherSchema = new StructType()
    .add("a", "int")
    .add("id", "long")
    .add("payload", "binary")

  private val otherSchemaString = otherSchema.catalogString

  private val complexSchema = simpleSchema
    .add("d", simpleSchema)
    .add("e", "array<int>")
    .add("f", MapType(StringType, simpleSchema))
    .add("g", "string")

  private val complexSchemaString = complexSchema.catalogString

  private def createLocalRelation(schema: String): DataFrame = session.newDataset { builder =>
    // TODO API is not consistent. Now we have two different ways of working with schemas!
    builder.getLocalRelationBuilder.setSchema(schema)
  }

  // A few helper dataframes.
  private def simple: DataFrame = createLocalRelation(simpleSchemaString)
  private def left: DataFrame = simple
  private def right: DataFrame = createLocalRelation(otherSchemaString)
  private def complex = createLocalRelation(complexSchemaString)

  private def select(cs: Column*): DataFrame = simple.select(cs: _*)

  /* Spark Session API */
  test("sql") {
    session.sql("select 1")
  }

  test("range") {
    session.range(1, 10, 1, 2)
  }

  test("read") {
    session.read.format("csv")
      .schema(StructType(
        StructField("name", StringType) ::
          StructField("age", IntegerType) ::
          StructField("job", StringType) :: Nil))
      .option("header", "true")
      .options(Map("delimiter" -> ";"))
      .load(testDataPath.resolve("people.csv").toString)
  }

  test("read json") {
    session.read.json(testDataPath.resolve("people.json").toString)
  }

  test("read csv") {
    session.read.csv(testDataPath.resolve("people.csv").toString)
  }

  test("read parquet") {
    session.read.parquet(testDataPath.resolve("users.parquet").toString)
  }

  test("read orc") {
    session.read.orc(testDataPath.resolve("users.orc").toString)
  }

  test("read table") {
    session.read.table("myTable")
  }

  test("table") {
    session.table("myTable")
  }

  test("read text") {
    session.read.text(testDataPath.resolve("people.txt").toString)
  }

  /* Dataset API */
  test("select") {
    simple.select(fn.col("id"))
  }

  test("limit") {
    simple.limit(10)
  }

  test("filter") {
    simple.filter(fn.col("id") === fn.lit(10L))
  }

  test("toDF") {
    simple.toDF("x1", "x2", "x3")
  }

  test("to") {
    simple.to(
      new StructType()
        .add("b", "double")
        .add("id", "int"))
  }

  test("join inner_no_condition") {
    left.join(right)
  }

  test("join inner_using_single_col") {
    left.join(right, "id")
  }

  test("join inner_using_multiple_col_array") {
    left.join(right, Array("id", "a"))
  }

  test("join inner_using_multiple_col_seq") {
    left.join(right, Seq("id", "a"))
  }

  test("join using_single_col") {
    left.join(right, "id", "left_semi")
  }

  test("join using_multiple_col_array") {
    left.join(right, Array("id", "a"), "full_outer")
  }

  test("join using_multiple_col_seq") {
    left.join(right, Seq("id", "a"), "right_outer")
  }

  test("join inner_condition") {
    left.join(right, fn.col("a") === fn.col("a"))
  }

  test("join condition") {
    left.join(right, fn.col("id") === fn.col("id"), "left_anti")
  }

  test("crossJoin") {
    left.crossJoin(right)
  }

  test("sortWithinPartitions strings") {
    simple.sortWithinPartitions("a", "id")
  }

  test("sortWithinPartitions columns") {
    simple.sortWithinPartitions(fn.col("id"), fn.col("b"))
  }

  test("sort strings") {
    simple.sort("b", "a")
  }

  test("sort columns") {
    simple.sort(fn.col("id"), fn.col("b"))
  }

  test("orderBy strings") {
    simple.sort("b", "id", "a")
  }

  test("orderBy columns") {
    simple.sort(fn.col("id"), fn.col("b"), fn.col("a"))
  }

  test("apply") {
    simple.select(simple.apply("a"))
  }

  test("hint") {
    simple.hint("coalesce", 100)
  }

  test("col") {
    simple.select(simple.col("id"), simple.col("b"))
  }

  test("colRegex") {
    simple.select(simple.colRegex("a|id"))
  }

  test("as string") {
    simple.as("foo")
  }

  test("as symbol") {
    simple.as('bar)
  }
  test("alias string") {
    simple.alias("fooz")
  }

  test("alias symbol") {
    simple.alias("bob")
  }

  test("select strings") {
    simple.select("id", "a")
  }

  test("selectExpr") {
    simple.selectExpr("a + 10 as x", "id % 10 as grp")
  }

  test("filter expr") {
    simple.filter("exp(a) < 10.0")
  }

  test("where column") {
    simple.where(fn.col("id") === fn.lit(1L))
  }

  test("where expr") {
    simple.where("a + id < 1000")
  }

  test("unpivot values") {
    simple.unpivot(
      ids = Array(fn.col("id"), fn.col("a")),
      values = Array(fn.col("b")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("unpivot no_values") {
    simple.unpivot(
      ids = Array(fn.col("id")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("melt values") {
    simple.unpivot(
      ids = Array(fn.col("a")),
      values = Array(fn.col("id")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("melt no_values") {
    simple.melt(
      ids = Array(fn.col("id"), fn.col("a")),
      variableColumnName = "name",
      valueColumnName = "value")
  }

  test("offset") {
    simple.offset(1000)
  }

  test("union") {
    simple.union(simple)
  }

  test("unionAll") {
    simple.union(simple)
  }

  test("unionByName") {
    simple.unionByName(right)
  }

  test("unionByName allowMissingColumns") {
    simple.unionByName(right, allowMissingColumns = true)
  }

  test("intersect") {
    simple.intersect(simple)
  }

  test("intersectAll") {
    simple.intersectAll(simple)
  }

  test("except") {
    simple.except(simple)
  }

  test("exceptAll") {
    simple.exceptAll(simple)
  }

  test("sample fraction_seed") {
    simple.sample(0.43, 9890823L)
  }

  test("sample withReplacement_fraction_seed") {
    simple.sample(withReplacement = true, 0.23, 898L)
  }

  test("withColumn single") {
    simple.withColumn("z", fn.expr("a + 100"))
  }

  test("withColumns scala_map") {
    simple.withColumns(Map(("b", fn.lit("redacted")), ("z", fn.expr("a + 100"))))
  }

  test("withColumns java_map") {
    val map = new java.util.HashMap[String, Column]
    map.put("g", fn.col("id"))
    map.put("a", fn.lit("123"))
    simple.withColumns(map)
  }

  test("withColumnRenamed single") {
    simple.withColumnRenamed("id", "nid")
  }

  test("withColumnRenamed scala_map") {
    simple.withColumnsRenamed(Map(("a", "alpha"), ("b", "beta")))
  }

  test("withColumnRenamed java_map") {
    val map = new java.util.HashMap[String, String]
    map.put("id", "nid")
    map.put("b", "bravo")
    simple.withColumnsRenamed(map)
  }

  test("withMetadata") {
    val builder = new MetadataBuilder
    builder.putString("description", "unique identifier")
    simple.withMetadata("id", builder.build())
  }

  test("drop single string") {
    simple.drop("a")
  }

  test("drop multiple strings") {
    simple.drop("id", "a", "b")
  }

  test("drop single column") {
    simple.drop(fn.col("b"))
  }

  test("drop multiple column") {
    simple.drop(fn.col("b"), fn.col("id"))
  }

  test("dropDuplicates") {
    simple.dropDuplicates()
  }

  test("dropDuplicates names seq") {
    simple.dropDuplicates("a" :: "b" :: Nil)
  }

  test("dropDuplicates names array") {
    simple.dropDuplicates(Array("a", "id"))
  }

  test("dropDuplicates varargs") {
    simple.dropDuplicates("a", "b", "id")
  }

  test("describe") {
    simple.describe("id", "b")
  }

  test("summary") {
    simple.summary("mean", "min")
  }

  test("repartition") {
    simple.repartition(24)
  }

  test("repartition num_partitions_expressions") {
    simple.repartition(22, fn.col("a"), fn.col("id"))
  }

  test("repartition expressions") {
    simple.repartition(fn.col("id"), fn.col("b"))
  }

  test("repartitionByRange num_partitions_expressions") {
    simple.repartitionByRange(33, fn.col("b"), fn.col("id").desc_nulls_first)
  }

  test("repartitionByRange expressions") {
    simple.repartitionByRange(fn.col("a").asc, fn.col("id").desc_nulls_first)
  }

  test("coalesce") {
    simple.coalesce(5)
  }

  test("distinct") {
    simple.distinct()
  }

  /* Column API */
  private def columnTest(name: String)(f: => Column): Unit = {
    test("column " + name) {
      complex.select(f)
    }
  }

  private def orderColumnTest(name: String)(f: => Column): Unit = {
    test("column " + name) {
      complex.orderBy(f)
    }
  }

  columnTest("apply") {
    fn.col("f").apply("super_duper_key")
  }

  columnTest("unary minus") {
    -fn.lit(1)
  }

  columnTest("not") {
    !fn.lit(true)
  }

  columnTest("equals") {
    fn.col("a") === fn.col("b")
  }

  columnTest("not equals") {
    fn.col("a") =!= fn.col("b")
  }

  columnTest("gt") {
    fn.col("a") > fn.col("b")
  }

  columnTest("lt") {
    fn.col("a") < fn.col("b")
  }

  columnTest("geq") {
    fn.col("a") >= fn.col("b")
  }

  columnTest("leq") {
    fn.col("a") <= fn.col("b")
  }

  columnTest("eqNullSafe") {
    fn.col("a") <=> fn.col("b")
  }

  columnTest("when otherwise") {
    val a = fn.col("a")
    fn.when(a < 10, "low").when(a < 20, "medium").otherwise("high")
  }

  columnTest("between") {
    fn.col("a").between(10, 20)
  }

  columnTest("isNaN") {
    fn.col("b").isNaN
  }

  columnTest("isNull") {
    fn.col("g").isNull
  }

  columnTest("isNotNull") {
    fn.col("g").isNotNull
  }

  columnTest("and") {
    fn.col("a") > 10 && fn.col("b") < 0.5d
  }

  columnTest("or") {
    fn.col("a") > 10 || fn.col("b") < 0.5d
  }

  columnTest("add") {
    fn.col("a") + fn.col("b")
  }

  columnTest("subtract") {
    fn.col("a") - fn.col("b")
  }

  columnTest("multiply") {
    fn.col("a") * fn.col("b")
  }

  columnTest("divide") {
    fn.col("a") / fn.col("b")
  }

  columnTest("modulo") {
    fn.col("a") % 10
  }

  columnTest("isin") {
    fn.col("g").isin("hello", "world", "foo")
  }

  columnTest("like") {
    fn.col("g").like("%bob%")
  }

  columnTest("rlike") {
    fn.col("g").like("^[0-9]*$")
  }

  columnTest("ilike") {
    fn.col("g").like("%fOb%")
  }

  columnTest("getItem") {
    fn.col("e").getItem(3)
  }

  columnTest("withField") {
    fn.col("d").withField("x", fn.lit("xq"))
  }

  columnTest("dropFields") {
    fn.col("d").dropFields("a", "c")
  }

  columnTest("getField") {
    fn.col("d").getItem("b")
  }

  columnTest("substr") {
    fn.col("g").substr(8, 3)
  }

  columnTest("contains") {
    fn.col("g").contains("baz")
  }

  columnTest("startsWith") {
    fn.col("g").startsWith("prefix_")
  }

  columnTest("endsWith") {
    fn.col("g").endsWith("suffix_")
  }

  columnTest("alias") {
    fn.col("a").name("b")
  }

  columnTest("as multi") {
    fn.col("d").as(Array("v1", "v2", "v3"))
  }

  columnTest("as with metadata") {
    val builder = new MetadataBuilder
    builder.putString("comment", "modified C field")
    fn.col("c").as("c_mod", builder.build())
  }

  columnTest("cast") {
    fn.col("a").cast("long")
  }

  orderColumnTest("desc") {
    fn.col("b").desc
  }

  orderColumnTest("desc_nulls_first") {
    fn.col("b").desc_nulls_first
  }

  orderColumnTest("desc_nulls_last") {
    fn.col("b").desc_nulls_last
  }

  orderColumnTest("asc") {
    fn.col("a").asc
  }

  orderColumnTest("asc_nulls_first") {
    fn.col("a").asc_nulls_first
  }

  orderColumnTest("asc_nulls_last") {
    fn.col("a").asc_nulls_last
  }

  columnTest("bitwiseOR") {
    fn.col("a").bitwiseOR(7)
  }

  columnTest("bitwiseAND") {
    fn.col("a").bitwiseAND(255)
  }

  columnTest("bitwiseXOR") {
    fn.col("a").bitwiseXOR(78)
  }

  columnTest("star") {
    fn.col("*")
  }

  columnTest("star with target") {
    fn.col("str.*")
  }

  /* Function API */
  test("function col") {
    select(fn.col("id"))
  }

  test("function udf " + scala) {
    // This test might be a bit tricky if different JVM
    // versions are used to generate the golden files.
    val functions = Seq(
      fn.udf(TestUDFs.udf0)
        .asNonNullable()
        .asNondeterministic(),
      fn.udf(TestUDFs.udf1).withName("foo"),
      fn.udf(TestUDFs.udf2).withName("f3"),
      fn.udf(TestUDFs.udf3).withName("bar"),
      fn.udf(TestUDFs.udf4).withName("f_four"))
    val id = fn.col("id")
    val columns = functions.zipWithIndex.map { case (udf, i) =>
      udf(Seq.fill(i)(id): _*)
    }
    select(columns: _*)
  }

  test("function lit") {
    select(
      fn.lit(fn.col("id")),
      fn.lit('id),
      fn.lit(true),
      fn.lit(68.toByte),
      fn.lit(9872.toShort),
      fn.lit(-8726532),
      fn.lit(7834609328726532L),
      fn.lit(Math.E),
      fn.lit(-0.8f),
      fn.lit(BigDecimal(8997620, 5)),
      fn.lit(BigDecimal(898897667231L, 7).bigDecimal),
      fn.lit("connect!"),
      fn.lit('T'),
      fn.lit(Array.tabulate(10)(i => ('A' + i).toChar)),
      fn.lit(Array.tabulate(23)(i => (i + 120).toByte)),
      fn.lit(mutable.WrappedArray.make(Array[Byte](8.toByte, 6.toByte))),
      fn.lit(java.time.LocalDate.of(2020, 10, 10)))
  }

  /* Window API */
}
