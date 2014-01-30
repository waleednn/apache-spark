package catalyst.execution

import scala.collection.JavaConversions._

import TestShark._

/**
 * A set of test cases that validate partition and column pruning.
 */
class PruningSuite extends HiveComparisonTest {
  createPruningTest("Pruning non-partitioned table",
    "SELECT value from src WHERE key IS NOT NULL",
    Seq("value"),
    Seq("value", "key"),
    Seq.empty)

  createPruningTest("Pruning with predicate on STRING partition key",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2008-04-08'",
    Seq("value", "hr"),
    Seq("value", "hr", "ds"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-08", "12")))

  createPruningTest("Pruning with predicate on INT partition key",
    "SELECT value, hr FROM srcpart1 WHERE hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  createPruningTest("Select only 1 partition",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2008-04-08' AND hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr", "ds"),
    Seq(
      Seq("2008-04-08", "11")))

  createPruningTest("All partitions pruned",
    "SELECT value, hr FROM srcpart1 WHERE ds = '2014-01-27' AND hr = 11",
    Seq("value", "hr"),
    Seq("value", "hr", "ds"),
    Seq.empty)

  createPruningTest("Pruning with both column key and partition key",
    "SELECT value, hr FROM srcpart1 WHERE value IS NOT NULL AND hr < 12",
    Seq("value", "hr"),
    Seq("value", "hr"),
    Seq(
      Seq("2008-04-08", "11"),
      Seq("2008-04-09", "11")))

  def createPruningTest(
      testCaseName: String,
      sql: String,
      expectedOutputColumns: Seq[String],
      expectedScannedColumns: Seq[String],
      expectedPartValues: Seq[Seq[String]]) = {
    test(testCaseName) {
      val plan = sql.q.executedPlan
      val actualOutputColumns = plan.output.map(_.name)
      val (actualScannedColumns, actualPartValues) = plan.collect {
        case p @ HiveTableScan(columns, relation, _) =>
          val columnNames = columns.map(_.name)
          val partValues = p.prunePartitions(relation.hiveQlPartitions).map(_.getValues)
          (columnNames, partValues)
      }.head

      assert(actualOutputColumns sameElements expectedOutputColumns)
      assert(actualScannedColumns sameElements expectedScannedColumns)
      assert(actualPartValues.corresponds(expectedPartValues)(_ sameElements _))
    }
  }
}
