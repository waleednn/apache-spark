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

package org.apache.spark

import java.io.{ObjectInputStream, Serializable}

import scala.collection.generic.Growable
import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.util.Utils


/**
 * A data type that can be accumulated, i.e. has an commutative and associative "add" operation,
 * but where the result type, `R`, may be different from the element type being added, `T`.
 *
 * You must define how to add data, and how to merge two of these together.  For some data types,
 * such as a counter, these might be the same operation. In that case, you can use the simpler
 * [[org.apache.spark.Accumulator]]. They won't always be the same, though -- e.g., imagine you are
 * accumulating a set. You will add items to the set, and you will union two sets together.
 *
 * Operations are not thread-safe.
 *
 * @param id ID of this accumulator; for internal use only.
 * @param initialValue initial value of accumulator
 * @param param helper object defining how to add elements of type `R` and `T`
 * @param name human-readable name for use in Spark's web UI
 * @param countFailedValues whether to accumulate values from failed tasks. This is set to true
 *                          for system and time metrics like serialization time or bytes spilled,
 *                          and false for things with absolute values like number of input rows.
 *                          This should be used for internal metrics only.
 * @param dataProperty if this [[Accumulable]] is for a data property. Data property
 *                     [[Accumulable]]s will only have values added once for each
 *                     RDD/Partition/Shuffle combination. This prevents double counting on
 *                     reevaluation. Partial evaluation of a partition will not increment a data
 *                     property [[Accumulable]]. Data property [[Accumulable]]s are currently
 *                     experimental and the behaviour may change in future versions.
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
class Accumulable[R, T] private[spark] (
    val id: Long,
    // SI-8813: This must explicitly be a private val, or else scala 2.11 doesn't compile
    @transient private val initialValue: R,
    param: AccumulableParam[R, T],
    val name: Option[String],
    private[spark] val countFailedValues: Boolean,
    private[spark] val dataProperty: Boolean)
  extends Serializable {

  private[spark] def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      countFailedValues: Boolean,
      dataProperty: Boolean) = {
    this(Accumulators.newId(), initialValue, param, None, countFailedValues, dataProperty)
  }

  def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String],
      countFailedValues: Boolean) = {
    this(Accumulators.newId(), initialValue, param, name, countFailedValues,
      false /* dataProperty */)
  }

  def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String],
      countFailedValues: Boolean,
      dataProperty: Boolean) = {
    this(Accumulators.newId(), initialValue, param, name, countFailedValues, dataProperty)
  }

  private[spark] def this(
      initialValue: R,
      param: AccumulableParam[R, T],
      name: Option[String]) =
    this(initialValue, param, name, false /* countFailed */)

  def this(
      initialValue: R,
      param: AccumulableParam[R, T]) = {
    this(initialValue, param, None)
  }

  @volatile @transient private var value_ : R = initialValue // Current value on driver

  val zero = param.zero(initialValue) // Zero value to be passed to executors
  private var deserialized = false

  /**
   * The following values are used for data property [[Accumulable]]s.
   * Data property [[Accumulable]]s have only-once semantics. These semantics are implemented
   * by keeping track of which RDD id, shuffle id, and partition id the current function is
   * processing in. If a partition is fully processed the results for that partition/shuffle/rdd
   * combination are sent back to the driver. The driver keeps track of which rdd/shuffle/partitions
   * already have been applied, and only combines values into value_ if the rdd/shuffle/partition
   * has not already been aggregated on the driver program
   */
  // For data property accumulators pending and processed updates.
  // Pending and processed are keyed by (rdd id, shuffle id, partition id)
  @transient private[spark] lazy val pending = new mutable.HashMap[(Int, Int, Int), R]()
  // Completed contains the set of (rdd id, shuffle id, partition id) that have been
  // fully processed on the worker side. This is used to determine if the updates should
  // be sent back to the driver for a particular rdd/shuffle/partition combination.
  @transient private[spark] lazy val completed = new mutable.HashSet[(Int, Int, Int)]()
  // Processed is keyed by (rdd id, shuffle id) and the value is a bitset containing all partitions
  // for the given key which have been merged into the value. This is used on the driver.
  @transient private[spark] lazy val processed = new mutable.HashMap[(Int, Int), mutable.BitSet]()

  Accumulators.register(this)

  /**
   * If this [[Accumulable]] is for a data property.
   * Data property [[Accumulable]]s track pending updates and reconcile them on the driver to avoid
   * applying duplicate updates for the same RDD and partition.
   */
  private[spark] def isDataProperty: Boolean = dataProperty

  /**
   * Return a copy of this [[Accumulable]].
   *
   * The copy will have the same ID as the original and will not be registered with
   * [[Accumulators]] again. This method exists so that the caller can avoid passing the
   * same mutable instance around.
   */
  private[spark] def copy(): Accumulable[R, T] = {
    new Accumulable[R, T](id, initialValue, param, name, countFailedValues, dataProperty)
  }

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def += (term: T) { add(term) }

  /**
   * Add more data to this accumulator / accumulable
   * @param term the data to add
   */
  def add(term: T) {
    value_ = param.addAccumulator(value_, term)
    if (dataProperty) {
      val updateInfo = TaskContext.get().getRDDPartitionInfo()
      val base = pending.getOrElse(updateInfo, zero)
      pending(updateInfo) = param.addAccumulator(base, term)
    }
  }

  /**
   * Mark a specific rdd/shuffle/partition as completely processed. This is a noop for
   * non-data property accumuables.
   */
  private[spark] def markFullyProcessed(rddId: Int, shuffleWriteId: Int, partitionId: Int): Unit = {
    if (dataProperty) {
      completed += ((rddId, shuffleWriteId, partitionId))
    }
  }

  /**
   * Merge two accumulable accumulated values together
   *
   * Normally, a user will not want to use this version, but will instead call `+=`.
   * @param term the other `R` that will get merged with this
   */
  def ++= (term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * Merge two accumulable accumulated values together
   *
   * Normally, a user will not want to use this version, but will instead call `add`.
   * @param term the other `R` that will get merged with this
   */
  def merge(term: R) { value_ = param.addInPlace(value_, term)}

  /**
   * Merge in pending updates for ac data property accumulators or merge accumulated values for
   * regular accumulators. This is only called on the driver when merging task results together.
   */
  private[spark] def internalMerge(term: Any) {
    if (!dataProperty) {
      merge(term.asInstanceOf[R])
    } else {
      mergePending(term.asInstanceOf[mutable.HashMap[(Int, Int, Int), R]])
    }
  }

  /**
   * Merge another Accumulable's pending updates, checks to make sure that each pending update has
   * not already been processed before updating.
   */
  private[spark] def mergePending(term: mutable.HashMap[(Int, Int, Int), R]) = {
    term.foreach { case ((rddId, shuffleWriteId, splitId), v) =>
      val splits = processed.getOrElseUpdate((rddId, shuffleWriteId), new mutable.BitSet())
      if (!splits.contains(splitId)) {
        splits += splitId
        value_ = param.addInPlace(value_, v)
      }
    }
  }

  /**
   * Access the accumulator's current value; only allowed on driver.
   */
  def value: R = {
    if (!deserialized) {
      value_
    } else {
      throw new UnsupportedOperationException("Can't read accumulator value in task")
    }
  }

  /**
   * Get the current value of this accumulator from within a task.
   *
   * This is NOT the global value of the accumulator.  To get the global value after a
   * completed operation on the dataset, call `value`.
   *
   * The typical use of this method is to directly mutate the local value, eg., to add
   * an element to a Set.
   */
  def localValue: R = value_

  /**
   * Get the updates for this accumulator that should be sent to the driver. For data property
   * accumulators this consists of the pending updates, for regular accumulators this is the same as
   * `localValue`.
   */
  private[spark] def updateValue: Any = {
    if (dataProperty) {
      pending.filter{case (k, v) => completed.contains(k)}
    } else {
      localValue
    }
  }

  /**
   * Set the accumulator's value; only allowed on driver and only for non-data property
   * accumulators.
   */
  def value_= (newValue: R) {
    if (!deserialized) {
      if (!dataProperty) {
        value_ = newValue
      } else {
        throw new UnsupportedOperationException("Can't assign value to data property accumulator.")
      }
    } else {
      throw new UnsupportedOperationException("Can't assign accumulator value in task")
    }
  }

  /**
   * Set the accumulator's value. For internal use only.
   */
  def setValue(newValue: R): Unit = { value_ = newValue }

  /**
   * Set the accumulator's value. For internal use only.
   */
  private[spark] def setValueAny(newValue: Any): Unit = { setValue(newValue.asInstanceOf[R]) }

  /**
   * Create an [[AccumulableInfo]] representation of this [[Accumulable]] with the provided values.
   */
  private[spark] def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    val isInternal = name.exists(_.startsWith(InternalAccumulator.METRICS_PREFIX))
    new AccumulableInfo(id, name, update, value, isInternal, countFailedValues, dataProperty)
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    value_ = zero
    deserialized = true

    // Automatically register the accumulator when it is deserialized with the task closure.
    // This is for external accumulators and internal ones that do not represent task level
    // metrics, e.g. internal SQL metrics, which are per-operator.
    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.registerAccumulator(this)
    }
  }

  override def toString: String = if (value_ == null) "null" else value_.toString
}

/**
 * Helper object defining how to accumulate values of a particular type. An implicit
 * AccumulableParam needs to be available when you create [[Accumulable]]s of a specific type.
 *
 * @tparam R the full accumulated data (result type)
 * @tparam T partial data that can be added in
 */
trait AccumulableParam[R, T] extends Serializable {
  /**
   * Add additional data to the accumulator value. Is allowed to modify and return `r`
   * for efficiency (to avoid allocating objects).
   *
   * @param r the current value of the accumulator
   * @param t the data to be added to the accumulator
   * @return the new value of the accumulator
   */
  def addAccumulator(r: R, t: T): R

  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  def addInPlace(r1: R, r2: R): R

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  def zero(initialValue: R): R
}


private[spark] class
GrowableAccumulableParam[R <% Growable[T] with TraversableOnce[T] with Serializable: ClassTag, T]
  extends AccumulableParam[R, T] {

  def addAccumulator(growable: R, elem: T): R = {
    growable += elem
    growable
  }

  def addInPlace(t1: R, t2: R): R = {
    t1 ++= t2
    t1
  }

  def zero(initialValue: R): R = {
    // We need to clone initialValue, but it's hard to specify that R should also be Cloneable.
    // Instead we'll serialize it to a buffer and load it back.
    val ser = new JavaSerializer(new SparkConf(false)).newInstance()
    val copy = ser.deserialize[R](ser.serialize(initialValue))
    copy.clear()   // In case it contained stuff
    copy
  }
}
