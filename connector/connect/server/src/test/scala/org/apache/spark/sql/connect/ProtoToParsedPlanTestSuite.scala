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
package org.apache.spark.sql.connect

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes
import java.util

import scala.util.{Failure, Success, Try}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.{catalog, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.analysis.{caseSensitiveResolution, Analyzer, FunctionRegistry, Resolver, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// scalastyle:off
/**
 * This test uses a corpus of queries ([[proto.Relation]] relations) and transforms each query
 * into its catalyst representation. The resulting catalyst plan is compared with a golden file.
 *
 * The objective of this test is to make sure the JVM client and potentially others produce valid
 * plans, and that these plans are transformed into their expected shape. Additionally this test
 * should capture breaking proto changes to a degree.
 *
 * The corpus of queries is generated by the `PlanGenerationTestSuite` in the connect/client/jvm
 * module.
 *
 * If you need to re-generate the golden files, you need to set the SPARK_GENERATE_GOLDEN_FILES=1
 * environment variable before running this test, e.g.:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "connect/testOnly org.apache.spark.sql.connect.ProtoToParsedPlanTestSuite"
 * }}}
 */
// scalastyle:on
class ProtoToParsedPlanTestSuite extends SparkFunSuite with SharedSparkSession {

  val sessionHolder = SessionHolder("user1", "session1", spark)

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        Connect.CONNECT_EXTENSIONS_RELATION_CLASSES.key,
        "org.apache.spark.sql.connect.plugin.ExampleRelationPlugin")
      .set(
        Connect.CONNECT_EXTENSIONS_EXPRESSION_CLASSES.key,
        "org.apache.spark.sql.connect.plugin.ExampleExpressionPlugin")
      .set(org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED.key, false.toString)
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

  protected val inputFilePath: Path = baseResourcePath.resolve("queries")
  protected val goldenFilePath: Path = baseResourcePath.resolve("explain-results")
  private val emptyProps: util.Map[String, String] = util.Collections.emptyMap()

  private val analyzer = {
    val inMemoryCatalog = new InMemoryCatalog
    inMemoryCatalog.initialize("primary", CaseInsensitiveStringMap.empty())
    inMemoryCatalog.createNamespace(Array("tempdb"), emptyProps)
    inMemoryCatalog.createTable(
      Identifier.of(Array("tempdb"), "myTable"),
      new StructType().add("id", "long"),
      Array.empty[Transform],
      emptyProps)

    val catalogManager = new CatalogManager(
      inMemoryCatalog,
      new SessionCatalog(
        new catalog.InMemoryCatalog(),
        FunctionRegistry.builtin,
        TableFunctionRegistry.builtin))
    catalogManager.setCurrentCatalog("primary")
    catalogManager.setCurrentNamespace(Array("tempdb"))

    new Analyzer(catalogManager) {
      override def resolver: Resolver = caseSensitiveResolution
    }
  }

  // Create the tests.
  Files.walkFileTree(
    inputFilePath,
    new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        createTest(file)
        FileVisitResult.CONTINUE
      }
    })

  private def createTest(file: Path): Unit = {
    val relativePath = inputFilePath.relativize(file)
    val fileName = relativePath.getFileName.toString
    if (!fileName.endsWith(".proto.bin")) {
      logError(s"Skipping $fileName")
      return
    }
    val name = fileName.stripSuffix(".proto.bin")
    test(name) {
      val relation = readRelation(file)
      val planner = new SparkConnectPlanner(sessionHolder)
      val catalystPlan =
        analyzer.executeAndCheck(planner.transformRelation(relation), new QueryPlanningTracker)
      val actual = normalizeExprIds(ReplaceExpressions(catalystPlan)).treeString
      val goldenFile = goldenFilePath.resolve(relativePath).getParent.resolve(name + ".explain")
      Try(readGoldenFile(goldenFile)) match {
        case Success(expected) if expected == actual => // Test passes.
        case Success(_) if regenerateGoldenFiles =>
          logInfo("Overwriting golden file.")
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
          logInfo("Writing golden file.")
          writeGoldenFile(goldenFile, actual)
        case Failure(_) =>
          fail(
            "No golden file found. Please re-run this test with the " +
              "SPARK_GENERATE_GOLDEN_FILES=1 environment variable set")
      }
    }
  }

  private def readRelation(path: Path): proto.Relation = {
    val input = Files.newInputStream(path)
    try proto.Relation.parseFrom(input)
    finally {
      input.close()
    }
  }

  private def readGoldenFile(path: Path): String = {
    new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
  }

  private def writeGoldenFile(path: Path, value: String): Unit = {
    val writer = Files.newBufferedWriter(path)
    try writer.write(value)
    finally {
      writer.close()
    }
  }
}
