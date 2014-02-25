package org.apache.spark.sql
package catalyst
package analysis

import plans.logical.{LogicalPlan, Subquery}
import scala.collection.mutable

/**
 * An interface for looking up relations by name.  Used by an [[Analyzer]].
 */
trait Catalog {
  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan
}

/**
 * A trait that can be mixed in with other Catalogs allowing specific tables to be overridden with
 * new logical plans.  This can be used to bind query result to virtual tables, or replace tables
 * with in-memory cached versions.  Note that the set of overrides is stored in memory and thus
 * lost when the JVM exits.
 */
trait OverrideCatalog extends Catalog {

  // TODO: This doesn't work when the database changes...
  val overrides = new mutable.HashMap[(Option[String],String), LogicalPlan]()

  abstract override def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None): LogicalPlan = {

    val overriddenTable = overrides.get((databaseName, tableName))

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    val withAlias =
      overriddenTable.map(r => alias.map(a => Subquery(a.toLowerCase, r)).getOrElse(r))

    withAlias.getOrElse(super.lookupRelation(databaseName, tableName, alias))
  }

  def overrideTable(databaseName: Option[String], tableName: String, plan: LogicalPlan) =
    overrides.put((databaseName, tableName), plan)
}

/**
 * A trivial catalog that returns an error when a relation is requested.  Used for testing when all
 * relations are already filled in and the analyser needs only to resolve attribute references.
 */
object EmptyCatalog extends Catalog {
  def lookupRelation(
    databaseName: Option[String],
    tableName: String,
    alias: Option[String] = None) = {
    throw new UnsupportedOperationException
  }
}
