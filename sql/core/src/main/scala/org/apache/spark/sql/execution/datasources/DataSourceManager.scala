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

package org.apache.spark.sql.execution.datasources

import java.util.Locale
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, V1Scan}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


/**
 * A manager for user-defined data sources. It is used to register and lookup data sources by
 * their short names or fully qualified names.
 */
class DataSourceManager extends Logging {

  private type DataSourceBuilder = (
    SparkSession,  // Spark session
    String,  // provider name
    Option[StructType],  // user specified schema
    CaseInsensitiveMap[String]  // options
  ) => LogicalPlan

  // TODO(SPARK-45917): Statically load Python Data Source so idempotently Python
  //   Data Sources can be loaded even when the Driver is restarted.
  private val dataSourceBuilders = new ConcurrentHashMap[String, DataSourceBuilder]()

  private def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  /**
   * Register a data source builder for the given provider.
   * Note that the provider name is case-insensitive.
   */
  def registerDataSource(name: String, builder: DataSourceBuilder): Unit = {
    val normalizedName = normalize(name)
    val previousValue = dataSourceBuilders.put(normalizedName, builder)
    if (previousValue != null) {
      logWarning(f"The data source $name replaced a previously registered data source.")
    }
  }

  /**
   * Returns a data source builder for the given provider and throw an exception if
   * it does not exist.
   */
  def lookupDataSource(name: String): DataSourceBuilder = {
    if (dataSourceExists(name)) {
      dataSourceBuilders.get(normalize(name))
    } else {
      throw QueryCompilationErrors.dataSourceDoesNotExist(name)
    }
  }

  /**
   * Checks if a data source with the specified name exists (case-insensitive).
   */
  def dataSourceExists(name: String): Boolean = {
    dataSourceBuilders.containsKey(normalize(name))
  }

  override def clone(): DataSourceManager = {
    val manager = new DataSourceManager
    dataSourceBuilders.forEach((k, v) => manager.registerDataSource(k, v))
    manager
  }
}

/**
 * Data Source V2 wrapper for Python Data Source.
 */
class PythonTableProvider(shortName: String) extends TableProvider {
  private var sourceDataFrame: DataFrame = _

  private def getOrCreateSourceDataFrame(
      options: CaseInsensitiveStringMap, maybeSchema: Option[StructType]): DataFrame = {
    if (sourceDataFrame != null) return sourceDataFrame
    // TODO(SPARK-45600): should be session-based.
    val builder = SparkSession.active.sessionState.dataSourceManager.lookupDataSource(shortName)
    val plan = builder(
      SparkSession.active,
      shortName,
      maybeSchema,
      CaseInsensitiveMap(options.asCaseSensitiveMap().asScala.toMap))
    sourceDataFrame = Dataset.ofRows(SparkSession.active, plan)
    sourceDataFrame
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    getOrCreateSourceDataFrame(options, None).schema

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    val givenSchema = schema
    new Table with SupportsRead {
      override def name(): String = shortName

      override def capabilities(): java.util.Set[TableCapability] = java.util.EnumSet.of(BATCH_READ)

      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new ScanBuilder with V1Scan {
          override def build(): Scan = this
          override def toV1TableScan[T <: BaseRelation with TableScan](
              context: SQLContext): T = {
            new BaseRelation with TableScan {
              // Avoid Row <> InternalRow conversion
              override val needConversion: Boolean = false
              override def buildScan(): RDD[Row] =
                getOrCreateSourceDataFrame(options, Some(givenSchema))
                  .queryExecution.toRdd.asInstanceOf[RDD[Row]]
              override def schema: StructType = givenSchema
              override def sqlContext: SQLContext = context
            }.asInstanceOf[T]
          }
          override def readSchema(): StructType = givenSchema
        }
      }

      override def schema(): StructType = givenSchema
    }
  }
}
