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

package org.apache.spark.sql.scripting

import scala.collection.mutable

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.parser.HandlerType
import org.apache.spark.sql.catalyst.parser.HandlerType.HandlerType
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.{Origin, WithOrigin}

/**
 * Trait for all SQL scripting execution nodes used during interpretation phase.
 */
sealed trait CompoundStatementExec extends Logging {

  /**
   * Whether the statement originates from the SQL script or is created during the interpretation.
   * Example: DropVariable statements are automatically created at the end of each compound.
   */
  val isInternal: Boolean = false

  /**
   * Reset execution of the current node.
   */
  def reset(): Unit
}

/**
 * Leaf node in the execution tree.
 */
trait LeafStatementExec extends CompoundStatementExec

/**
 * Non-leaf node in the execution tree. It is an iterator over executable child nodes.
 */
trait NonLeafStatementExec extends CompoundStatementExec {

  /**
   * Construct the iterator to traverse the tree rooted at this node in an in-order traversal.
   * @return
   *   Tree iterator.
   */
  def getTreeIterator: Iterator[CompoundStatementExec]
}

/**
 * Executable node for SingleStatement.
 * @param parsedPlan
 *   Logical plan of the parsed statement.
 * @param origin
 *   Origin descriptor for the statement.
 * @param isInternal
 *   Whether the statement originates from the SQL script or it is created during the
 *   interpretation. Example: DropVariable statements are automatically created at the end of each
 *   compound.
 */
class SingleStatementExec(
    var parsedPlan: LogicalPlan,
    override val origin: Origin,
    override val isInternal: Boolean,
    var collectResult: Boolean = true)  // Whether the statement result should be collected
  extends LeafStatementExec with WithOrigin {

  /**
   * Whether this statement has been executed during the interpretation phase.
   * Example: Statements in conditions of If/Else, While, etc.
   */
  var isExecuted = false

  /**
   * Whether an error was raised during the execution of this statement.
   */
  var raisedError = false

  /**
   * Data returned after execution.
   */
  var data: Option[Array[Row]] = None

  /**
   * Error state of the statement.
   */
  var errorState: Option[String] = None

  /**
   * Get the SQL query text corresponding to this statement.
   * @return
   *   SQL query text.
   */
  def getText: String = {
    assert(origin.sqlText.isDefined && origin.startIndex.isDefined && origin.stopIndex.isDefined)
    origin.sqlText.get.substring(origin.startIndex.get, origin.stopIndex.get + 1)
  }

  override def reset(): Unit = isExecuted = false

  def execute(session: SparkSession): Unit = {
    try {
      isExecuted = true
      val result = Some(Dataset.ofRows(session, parsedPlan).collect())
      if (collectResult) data = result
    } catch {
      case e: SparkThrowable =>
        // TODO: check handlers for error conditions
        raisedError = true
        errorState = Some(e.getSqlState)
      case _: Throwable =>
        raisedError = true
        errorState = Some("UNKNOWN")
    }
  }
}

/**
 * Abstract class for all statements that contain nested statements.
 * Implements recursive iterator logic over all child execution nodes.
 * @param collection
 *   Collection of child execution nodes.
 */
abstract class CompoundNestedStatementIteratorExec(collection: Seq[CompoundStatementExec])
  extends NonLeafStatementExec {

  protected var localIterator: Iterator[CompoundStatementExec] = collection.iterator
  protected var curr: Option[CompoundStatementExec] =
    if (localIterator.hasNext) Some(localIterator.next()) else None

  protected lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = {
        val childHasNext = curr match {
          case Some(body: NonLeafStatementExec) => body.getTreeIterator.hasNext
          case Some(_: LeafStatementExec) => true
          case None => false
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
        localIterator.hasNext || childHasNext
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(statement: SingleStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body.getTreeIterator.next()
            } else {
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }

  override def getTreeIterator: Iterator[CompoundStatementExec] = treeIterator

  override def reset(): Unit = {
    collection.foreach(_.reset())
    localIterator = collection.iterator
    curr = if (localIterator.hasNext) Some(localIterator.next()) else None
  }
}

/**
 * Executable node for CompoundBody.
 * @param statements
 *   Executable nodes for nested statements within the CompoundBody.
 */
class CompoundBodyExec(
      statements: Seq[CompoundStatementExec],
      handlers: Seq[ErrorHandlerExec] = Seq.empty,
      conditionHandlerMap: mutable.HashMap[String, ErrorHandlerExec] = mutable.HashMap(),
      session: SparkSession = null)
  extends CompoundNestedStatementIteratorExec(statements) {

  private def getHandler(condition: String): Option[ErrorHandlerExec] = {
    conditionHandlerMap.get(condition)
  }

  override protected lazy val treeIterator: Iterator[CompoundStatementExec] =
    new Iterator[CompoundStatementExec] {
      override def hasNext: Boolean = {
        val childHasNext = curr match {
          case Some(body: NonLeafStatementExec) => body.getTreeIterator.hasNext
          case Some(_: LeafStatementExec) => true
          case None => false
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
        localIterator.hasNext || childHasNext
      }

      @scala.annotation.tailrec
      override def next(): CompoundStatementExec = {
        curr match {
          case None => throw SparkException.internalError(
            "No more elements to iterate through in the current SQL compound statement.")
          case Some(statement: SingleStatementExec) =>
            curr = if (localIterator.hasNext) Some(localIterator.next()) else None
            statement.execute(session)
            if (statement.raisedError) {
              val handler = getHandler(statement.errorState.get).get
              handler.execute()
              handler.reset()

              if (handler.getHandlerType == HandlerType.EXIT) {
                // TODO: premature exit from the compound ...
                curr = None
              }
            }
            statement
          case Some(body: NonLeafStatementExec) =>
            if (body.getTreeIterator.hasNext) {
              body.getTreeIterator.next()
            } else {
              curr = if (localIterator.hasNext) Some(localIterator.next()) else None
              next()
            }
          case _ => throw SparkException.internalError(
            "Unknown statement type encountered during SQL script interpretation.")
        }
      }
    }

}

class ErrorHandlerExec(
    conditions: Seq[String],
    body: CompoundBodyExec,
    handlerType: HandlerType) extends CompoundStatementExec {

  def getHandlerType: HandlerType = handlerType

  def getHandlerBody: CompoundBodyExec = body

  def execute(): Unit = {
    print("\n\n\nHANDLER\n\n\n")
    val iterator = body.getTreeIterator

    while (iterator.hasNext) iterator.next()
  }

  override def reset(): Unit = body.reset()
}
