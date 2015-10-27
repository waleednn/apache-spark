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

package org.apache.spark.sql.jdbc

import java.sql.Types

import org.apache.spark.SparkException
import org.apache.spark.sql.SqlIdUtil._
import org.apache.spark.sql.types._
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A database type definition coupled with the jdbc type needed to send null
 * values to the database.
 * @param databaseTypeDefinition The database type definition
 * @param jdbcNullType The jdbc type (as defined in java.sql.Types) used to
 *                     send a null value to the database.
 */
@DeveloperApi
case class JdbcType(databaseTypeDefinition : String, jdbcNullType : Int)

/**
 * :: DeveloperApi ::
 * Encapsulates everything (extensions, workarounds, quirks) to handle the
 * SQL dialect of a certain database or jdbc driver.
 * Lots of databases define types that aren't explicitly supported
 * by the JDBC spec.  Some JDBC drivers also report inaccurate
 * information---for instance, BIT(n>1) being reported as a BIT type is quite
 * common, even though BIT in JDBC is meant for single-bit values.  Also, there
 * does not appear to be a standard name for an unbounded string or binary
 * type; we use BLOB and CLOB by default but override with database-specific
 * alternatives when these are absent or do not behave correctly.
 *
 * Currently, the only thing done by the dialect is type mapping.
 * `getCatalystType` is used when reading from a JDBC table and `getJDBCType`
 * is used when writing to a JDBC table.  If `getCatalystType` returns `null`,
 * the default type handling is used for the given JDBC type.  Similarly,
 * if `getJDBCType` returns `(null, None)`, the default type handling is used
 * for the given Catalyst type.
 */
@DeveloperApi
abstract class JdbcDialect {
  /**
   * Check if this dialect instance can handle a certain jdbc url.
   * @param url the jdbc url.
   * @return True if the dialect can be applied on the given jdbc url.
   * @throws NullPointerException if the url is null.
   */
  def canHandle(url : String): Boolean

  /**
   * Return the character used to frame delimited identifiers in this database.
   * @return The delimited id character (usually ", sometimes `)
   */
  def quoteChar: Char = '"'

  /**
   * Get the custom datatype mapping for the given jdbc meta information.
   * @param sqlType The sql type (see java.sql.Types)
   * @param typeName The sql type name (e.g. "BIGINT UNSIGNED")
   * @param size The size of the type.
   * @param md Result metadata associated with this type.
   * @return The actual DataType (subclasses of [[org.apache.spark.sql.types.DataType]])
   *         or null if the default type mapping should be used.
   */
  def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = None

  /**
   * Retrieve the jdbc / sql type for a given datatype.
   * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return The new JdbcType if there is an override for this DataType
   */
  def getJDBCType(dt: DataType): Option[JdbcType] = None

  /**
   * Quotes the identifier. This is used to put quotes around the identifier in case the column
   * name is a reserved keyword, or in case it contains characters that require quotes (e.g. space).
   */
  def quoteIdentifier(colName: String): String = {
    quoteString(colName, quoteChar)
  }

  /**
   * Get the SQL query that should be used to find if the given table exists.
   * Call this method (and not tableExistsQuery) in order to verify
   * that the table name is properly formed.
   * @param table  The name of the table.
   * @return The SQL query to use for checking the table.
   * @throws org.apache.spark.SparkException On invalid table name.
   */
  final def getTableExistsQuery(table: String): String = {
    vetSqlIdentifier(table)
    tableExistsQuery(table)
  }

  /**
   * Get the SQL query that should be used to find if the given table exists. Dialects can
   * override this method to return a query that works best in a particular database.
   * Don't expose this method outside this class and its subclasses.
   * Other consumers should call getTableExistsQuery instead. That method
   * verifies that the table name is properly formed.
   * @param table  The name of the table.
   * @return The SQL query to use for checking the table.
   */
  protected def tableExistsQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  /** Vet a user-supplied object id of the form
    * [[catalog.]schema.]objectName
    * by parsing it into a (catalog, schema, objectName)
    * triple. The catalog and schema names may be empty. Raises
    * a SparkException if the user-supplied id is malformed,
    * e.g., is a string like "foo; drop database finance;",
    * something intended for a SQL injection attack.
    *
    * @param rawId The user-supplied object id (name).
    * @throws org.apache.spark.SparkException On invalid ids.
    */
  def vetSqlIdentifier(rawId: String) {

    // raises a SparkException if the string doesn't parse
    parseSqlIds(rawId, quoteChar, true)
  }
}

/**
 * :: DeveloperApi ::
 * Registry of dialects that apply to every new jdbc [[org.apache.spark.sql.DataFrame]].
 *
 * If multiple matching dialects are registered then all matching ones will be
 * tried in reverse order. A user-added dialect will thus be applied first,
 * overwriting the defaults.
 *
 * Note that all new dialects are applied to new jdbc DataFrames only. Make
 * sure to register your dialects first.
 */
@DeveloperApi
object JdbcDialects {

  private var dialects = List[JdbcDialect]()

  /**
   * Register a dialect for use on all new matching jdbc [[org.apache.spark.sql.DataFrame]].
   * Readding an existing dialect will cause a move-to-front.
   * @param dialect The new dialect.
   */
  def registerDialect(dialect: JdbcDialect) : Unit = {
    dialects = dialect :: dialects.filterNot(_ == dialect)
  }

  /**
   * Unregister a dialect. Does nothing if the dialect is not registered.
   * @param dialect The jdbc dialect.
   */
  def unregisterDialect(dialect : JdbcDialect) : Unit = {
    dialects = dialects.filterNot(_ == dialect)
  }

  registerDialect(MySQLDialect)
  registerDialect(PostgresDialect)
  registerDialect(DB2Dialect)
  registerDialect(MsSqlServerDialect)
  registerDialect(DerbyDialect)


  /**
   * Fetch the JdbcDialect class corresponding to a given database url.
   */
  private[sql] def get(url: String): JdbcDialect = {
    val matchingDialects = dialects.filter(_.canHandle(url))
    matchingDialects.length match {
      case 0 => NoopDialect
      case 1 => matchingDialects.head
      case _ => new AggregatedDialect(matchingDialects)
    }
  }

  /**
   * Get all dialects (useful for testing purposes).
   */
  private[sql] def getAllDialects(): List[JdbcDialect] = dialects

}

/**
 * :: DeveloperApi ::
 * AggregatedDialect can unify multiple dialects into one virtual Dialect.
 * Dialects are tried in order, and the first dialect that does not return a
 * neutral element will will.
 * @param dialects List of dialects.
 */
@DeveloperApi
class AggregatedDialect(dialects: List[JdbcDialect]) extends JdbcDialect {

  require(dialects.nonEmpty)

  override def canHandle(url : String): Boolean =
    dialects.map(_.canHandle(url)).reduce(_ && _)

  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    dialects.flatMap(_.getCatalystType(sqlType, typeName, size, md)).headOption
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    dialects.flatMap(_.getJDBCType(dt)).headOption
  }
}

/**
 * :: DeveloperApi ::
 * NOOP dialect object, always returning the neutral element.
 */
@DeveloperApi
case object NoopDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = true
}

/**
 * :: DeveloperApi ::
 * Default postgres dialect, mapping bit/cidr/inet on read and string/binary/boolean on write.
 */
@DeveloperApi
case object PostgresDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:postgresql")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.BIT && typeName.equals("bit") && size != 1) {
      Option(BinaryType)
    } else if (sqlType == Types.OTHER && typeName.equals("cidr")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("inet")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("json")) {
      Option(StringType)
    } else if (sqlType == Types.OTHER && typeName.equals("jsonb")) {
      Option(StringType)
    } else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("TEXT", java.sql.Types.CHAR))
    case BinaryType => Some(JdbcType("BYTEA", java.sql.Types.BINARY))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _ => None
  }

  override protected def tableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

}

/**
 * :: DeveloperApi ::
 * Default mysql dialect to read bit/bitsets correctly.
 */
@DeveloperApi
case object MySQLDialect extends JdbcDialect {
  override def canHandle(url : String): Boolean = url.startsWith("jdbc:mysql")
  override def quoteChar: Char = '`'
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else None
  }

  override protected def tableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  // The default implementation of this method allows embedded,
  // escaped quotes inside quoted identifiers. SQL Server does not
  // allow embedded quotes. This means that this method won't catch
  // some illegal table names. Those names will appear to SQL Server as an
  // ungrammatical sequence of quoted identifiers. In order to get
  // a better error message, someone may want to provide an
  // implementation which handles the SQL Server grammar better.
  //
  // override def vetSqlIdentifier(rawId: String)

}

/**
 * :: DeveloperApi ::
 * Default DB2 dialect, mapping string/boolean on write to valid DB2 types.
 * By default string, and boolean gets mapped to db2 invalid types TEXT, and BIT(1).
 */
@DeveloperApi
case object DB2Dialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:db2")

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("CLOB", java.sql.Types.CLOB))
    case BooleanType => Some(JdbcType("CHAR(1)", java.sql.Types.CHAR))
    case _ => None
  }
}

/**
 * :: DeveloperApi ::
 * Default Microsoft SQL Server dialect, mapping the datetimeoffset types to a String on read.
 */
@DeveloperApi
case object MsSqlServerDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlserver")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (typeName.contains("datetimeoffset")) {
      // String is recommend by Microsoft SQL Server for datetimeoffset types in non-MS clients
      Option(StringType)
    } else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case TimestampType => Some(JdbcType("DATETIME", java.sql.Types.TIMESTAMP))
    case _ => None
  }
}

/**
 * :: DeveloperApi ::
 * Default Apache Derby dialect, mapping real on read
 * and string/byte/short/boolean/decimal on write.
 */
@DeveloperApi
case object DerbyDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:derby")
  override def getCatalystType(
      sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.REAL) Option(FloatType) else None
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("CLOB", java.sql.Types.CLOB))
    case ByteType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case ShortType => Some(JdbcType("SMALLINT", java.sql.Types.SMALLINT))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    // 31 is the maximum precision and 5 is the default scale for a Derby DECIMAL
    case (t: DecimalType) if (t.precision > 31) =>
      Some(JdbcType("DECIMAL(31,5)", java.sql.Types.DECIMAL))
    case _ => None
  }

}

