package org.apache.spark.sql
package execution

import catalyst.rules.Rule
import catalyst.errors._
import catalyst.expressions._
import catalyst.plans.physical._

import org.apache.spark.{RangePartitioner, HashPartitioner}
import org.apache.spark.rdd.ShuffledRDD

case class Exchange(newPartitioning: Partitioning, child: SparkPlan) extends UnaryNode {

  override def outputPartitioning = newPartitioning
  def output = child.output

  def execute() = attachTree(this , "execute") {
    newPartitioning match {
      case HashPartitioning(expressions, numPartitions) => {
        // TODO: Eliminate redundant expressions in grouping key and value.
        val rdd = child.execute().map { row =>
          (buildRow(expressions.toSeq.map(Evaluate(_, Vector(row)))), row)
        }
        val part = new HashPartitioner(numPartitions)
        val shuffled = new ShuffledRDD[Row, Row, (Row, Row)](rdd, part)

        shuffled.map(_._2)
      }
      case RangePartitioning(sortingExpressions, numPartitions) => {
        // TODO: ShuffledRDD should take an Ordering.
        implicit val ordering = new RowOrdering(sortingExpressions)

        val rdd = child.execute().map(row => (row, null))
        val part = new RangePartitioner(numPartitions, rdd, ascending = true)
        val shuffled = new ShuffledRDD[Row, Null, (Row, Null)](rdd, part)

        shuffled.map(_._1)
      }
      case SinglePartition => {
        val rdd = child.execute().coalesce(1, true)

        rdd
      }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
      // TODO: Handle BroadcastPartitioning.
    }
  }
}

/**
 * Ensures that the [[catalyst.plans.physical.Partitioning Partitioning]] of input data meets the
 * [[catalyst.plans.physical.Distribution Distribution]] requirements for each operator by inserting
 * [[Exchange]] Operators where required.
 */
object AddExchange extends Rule[SparkPlan] {
  // TODO: Determine the number of partitions.
  val numPartitions = 8

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case operator: SparkPlan =>
      // Check if every child's outputPartitioning satisfies the corresponding
      // required data distribution.
      def meetsRequirements =
        !operator.requiredChildDistribution.zip(operator.children).map {
          case (required, child) =>
            val valid = child.outputPartitioning.satisfies(required)
            logger.debug(
              s"${if (valid) "Valid" else "Invalid"} distribution," +
                s"required: $required current: ${child.outputPartitioning}")
            valid
        }.exists(!_)

      // Check if outputPartitionings of children are compatible with each other.
      // It is possible that every child satisfies its required data distribution
      // but two children have incompatible outputPartitionings. For example,
      // A dataset is range partitioned by "a.asc" (RangePartitioning) and another
      // dataset is hash partitioned by "a" (HashPartitioning). Tuples in these two
      // datasets are both clustered by "a", but these two outputPartitionings are not
      // compatible.
      // TODO: ASSUMES TRANSITIVITY?
      def compatible =
        !operator.children
          .map(_.outputPartitioning)
          .sliding(2)
          .map {
            case Seq(a) => true
            case Seq(a,b) => a compatibleWith b
          }.exists(!_)

      // Check if the partitioning we want to ensure is the same as the child's output
      // partitioning. If so, we do not need to add the Exchange operator.
      def addExchangeIfNecessary(partitioning: Partitioning, child: SparkPlan) =
        if (child.outputPartitioning != partitioning) Exchange(partitioning, child) else child

      if (meetsRequirements && compatible) {
        operator
      } else {
        // At least one child does not satisfies its required data distribution or
        // at least one child's outputPartitioning is not compatible with another child's
        // outputPartitioning. In this case, we need to add Exchange operators.
        val repartitionedChildren = operator.requiredChildDistribution.zip(operator.children).map {
          case (AllTuples, child) =>
            addExchangeIfNecessary(SinglePartition, child)
          case (ClusteredDistribution(clustering), child) =>
            addExchangeIfNecessary(HashPartitioning(clustering, numPartitions), child)
          case (OrderedDistribution(ordering), child) =>
            addExchangeIfNecessary(RangePartitioning(ordering, numPartitions), child)
          case (UnspecifiedDistribution, child) => child
          case (dist, _) => sys.error(s"Don't know how to ensure $dist")
        }
        operator.withNewChildren(repartitionedChildren)
      }
  }
}
