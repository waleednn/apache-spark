package catalyst
package plans
package physical

import org.apache.spark.rdd.RDD

abstract class PhysicalPlan extends QueryPlan[PhysicalPlan] {
  self: Product =>

  /**
   * Runs this query returning the result as an RDD.
   * This fact that this returns an RDD should probably be
   * abstracted away from the rest of the planning code.
   */
  def execute(): RDD[_]
}

abstract trait LeafNode extends PhysicalPlan with trees.LeafNode[PhysicalPlan] {
  self: Product =>
}

abstract trait UnaryNode extends PhysicalPlan with trees.UnaryNode[PhysicalPlan] {
  self: Product =>
}

abstract trait BinaryNode extends PhysicalPlan with trees.BinaryNode[PhysicalPlan] {
  self: Product =>
}