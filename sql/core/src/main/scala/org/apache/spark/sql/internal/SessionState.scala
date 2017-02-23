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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.streaming.StreamingQueryManager
import org.apache.spark.sql.util.ExecutionListenerManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 */
private[sql] class SessionState(
    sparkContext: SparkContext,
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val catalog: SessionCatalog,
    val sqlParser: ParserInterface,
    val analyzer: Analyzer,
    val streamingQueryManager: StreamingQueryManager,
    val queryExecutionCreator: LogicalPlan => QueryExecution) {

  /**
   * Interface exposed to the user for registering user-defined functions.
   * Note that the user-defined functions must be deterministic.
   */
  val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  // Logical query plan optimizer.
  val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  def planner: SparkPlanner =
    new SparkPlanner(sparkContext, conf, experimentalMethods.extraStrategies)

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sparkContext.hadoopConfiguration,
    conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  def clone(newSparkSession: SparkSession): SessionState = {
    val sparkContext = newSparkSession.sparkContext
    val confCopy = conf.clone()
    val hadoopConfCopy = SessionState.newHadoopConf(sparkContext.hadoopConfiguration, confCopy)
    val functionRegistryCopy = functionRegistry.clone()
    val sqlParser: ParserInterface = new SparkSqlParser(confCopy)
    val catalogCopy = catalog.clone(confCopy, hadoopConfCopy, functionRegistryCopy, sqlParser)
    val queryExecution = (plan: LogicalPlan) => new QueryExecution(newSparkSession, plan)

    sparkContext.getConf.getAll.foreach { case (k, v) =>
      confCopy.setConfString(k, v)
    }

    new SessionState(
      sparkContext,
      newSparkSession.sharedState,
      confCopy,
      experimentalMethods.clone(),
      functionRegistryCopy,
      catalogCopy,
      sqlParser,
      SessionState.createAnalyzer(newSparkSession, catalogCopy, confCopy),
      new StreamingQueryManager(newSparkSession),
      queryExecution)
  }

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executePlan(plan: LogicalPlan): QueryExecution = queryExecutionCreator(plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }

  def addJar(path: String): Unit = sharedState.addJar(path)
}


object SessionState {

  def apply(sparkSession: SparkSession): SessionState = {
    apply(sparkSession, None)
  }

  def apply(
      sparkSession: SparkSession,
      conf: Option[SQLConf]): SessionState = {

    val sparkContext = sparkSession.sparkContext

    // SQL-specific key-value configurations.
    val sqlConf = conf.getOrElse(new SQLConf)

    // Automatically extract all entries and put them in our SQLConf
    sparkContext.getConf.getAll.foreach { case (k, v) =>
      sqlConf.setConfString(k, v)
    }

    // Internal catalog for managing functions registered by the user.
    val functionRegistry = FunctionRegistry.builtin.clone()

    // A class for loading resources specified by a function.
    val functionResourceLoader: FunctionResourceLoader =
      createFunctionResourceLoader(sparkContext, sparkSession.sharedState)

    // Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
    val sqlParser: ParserInterface = new SparkSqlParser(sqlConf)

    // Internal catalog for managing table and database states.
    val catalog = new SessionCatalog(
      sparkSession.sharedState.externalCatalog,
      sparkSession.sharedState.globalTempViewManager,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      newHadoopConf(sparkContext.hadoopConfiguration, sqlConf),
      sqlParser)

    // Logical query plan analyzer for resolving unresolved attributes and relations.
    val analyzer: Analyzer = createAnalyzer(sparkSession, catalog, sqlConf)

    // Interface to start and stop [[StreamingQuery]]s.
    val streamingQueryManager: StreamingQueryManager = new StreamingQueryManager(sparkSession)

    val queryExecutionCreator = (plan: LogicalPlan) => new QueryExecution(sparkSession, plan)

    new SessionState(
      sparkContext,
      sparkSession.sharedState,
      sqlConf,
      new ExperimentalMethods,
      functionRegistry,
      catalog,
      sqlParser,
      analyzer,
      streamingQueryManager,
      queryExecutionCreator)
  }

  def createFunctionResourceLoader(
      sparkContext: SparkContext,
      sharedState: SharedState): FunctionResourceLoader = {

    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => sharedState.addJar(resource.uri)
          case FileResource => sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
  }

  def newHadoopConf(copyHadoopConf: Configuration, sqlConf: SQLConf): Configuration = {
    val hadoopConf = new Configuration(copyHadoopConf)
    sqlConf.getAllConfs.foreach { case (k, v) => if (v ne null) hadoopConf.set(k, v) }
    hadoopConf
  }

  def createAnalyzer(
      sparkSession: SparkSession,
      catalog: SessionCatalog,
      sqlConf: SQLConf): Analyzer = {

    new Analyzer(catalog, sqlConf) {
      override val extendedResolutionRules =
        new FindDataSourceTable(sparkSession) ::
        new ResolveSQLOnFile(sparkSession) :: Nil

      override val postHocResolutionRules =
        PreprocessTableCreation(sparkSession) ::
        PreprocessTableInsertion(sqlConf) ::
        DataSourceAnalysis(sqlConf) :: Nil

      override val extendedCheckRules = Seq(PreWriteCheck, HiveOnlyCheck)
    }
  }

}
