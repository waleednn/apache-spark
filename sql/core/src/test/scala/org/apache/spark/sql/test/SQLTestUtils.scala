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

package org.apache.spark.sql.test

import java.io.File
import java.util.UUID

import scala.util.Try
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.Utils

/**
 * Helper trait that should be extended by all SQL test suites involving a
 * [[org.apache.spark.sql.SQLContext]].
 */
private[sql] trait SQLTestUtils extends AbstractSQLTestUtils with SharedSQLContext {
  protected final override def _sqlContext = sqlContext
}

/**
 * Helper trait that should be extended by all SQL test suites.
 *
 * This base trait allows subclasses to plugin a custom [[SQLContext]]. It comes with test
 * data prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the [[SQLContext]].
 */
private[sql] trait AbstractSQLTestUtils extends SparkFunSuite with SQLTestData { self =>
  protected def _sqlContext: SQLContext

  /**
   * A helper object for importing SQL implicits.
   *
   * Note that the alternative of importing `sqlContext.implicits._` is not possible here.
   * This is because we create the [[SQLContext]] immediately before the first test is run,
   * but the implicits import is needed in the constructor.
   */
  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self._sqlContext
  }

  /**
   * The Hadoop configuration used by the active [[SQLContext]].
   */
  protected def configuration: Configuration = {
    _sqlContext.sparkContext.hadoopConfiguration
  }

  /**
   * Sets all SQL configurations specified in `pairs`, calls `f`, and then restore all SQL
   * configurations.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(_sqlContext.conf.getConfString(key)).toOption)
    (keys, values).zipped.foreach(_sqlContext.conf.setConfString)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => _sqlContext.conf.setConfString(key, value)
        case (key, None) => _sqlContext.conf.unsetConf(key)
      }
    }
  }

  /**
   * Generates a temporary path without creating the actual file/directory, then pass it to `f`. If
   * a file/directory is created there by `f`, it will be delete after `f` returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * @todo Probably this method should be moved to a more general place
   */
  protected def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir().getCanonicalFile
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  /**
   * Drops temporary table `tableName` after calling `f`.
   */
  protected def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(_sqlContext.dropTempTable)
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        _sqlContext.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Creates a temporary database and switches current database to it before executing `f`.  This
   * database is dropped after `f` returns.
   */
  protected def withTempDatabase(f: String => Unit): Unit = {
    val dbName = s"db_${UUID.randomUUID().toString.replace('-', '_')}"

    try {
      _sqlContext.sql(s"CREATE DATABASE $dbName")
    } catch { case cause: Throwable =>
      fail("Failed to create temporary database", cause)
    }

    try f(dbName) finally _sqlContext.sql(s"DROP DATABASE $dbName CASCADE")
  }

  /**
   * Activates database `db` before executing `f`, then switches back to `default` database after
   * `f` returns.
   */
  protected def activateDatabase(db: String)(f: => Unit): Unit = {
    _sqlContext.sql(s"USE $db")
    try f finally _sqlContext.sql(s"USE default")
  }

  /**
   * Turn a logical plan into a [[DataFrame]]. This should be removed once we have an easier
   * way to construct [[DataFrame]] directly out of local data without relying on implicits.
   */
  protected implicit def logicalPlanToSparkQuery(plan: LogicalPlan): DataFrame = {
    DataFrame(_sqlContext, plan)
  }
}
