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
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{CompoundBody, CompoundPlanStatement, ErrorCondition, ErrorHandler, SingleStatement}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DropVariable, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.Origin

/**
 * SQL scripting interpreter - builds SQL script execution plan.
 */
case class SqlScriptingInterpreter(session: SparkSession) {

  /**
   * Build execution plan and return statements that need to be executed,
   *   wrapped in the execution node.
   *
   * @param compound
   *   CompoundBody for which to build the plan.
   * @return
   *   Iterator through collection of statements to be executed.
   */
  def buildExecutionPlan(compound: CompoundBody): Iterator[CompoundStatementExec] = {
    transformTreeIntoExecutable(compound).asInstanceOf[CompoundBodyExec].getTreeIterator
  }

  /**
   * Fetch the name of the Create Variable plan.
   * @param plan
   *   Plan to fetch the name from.
   * @return
   *   Name of the variable.
   */
  private def getDeclareVarNameFromPlan(plan: LogicalPlan): Option[UnresolvedIdentifier] =
    plan match {
      case CreateVariable(name: UnresolvedIdentifier, _, _) => Some(name)
      case _ => None
    }

  /**
   * Transform the parsed tree to the executable node.
   * @param node
   *   Root node of the parsed tree.
   * @return
   *   Executable statement.
   */
  private def transformTreeIntoExecutable(node: CompoundPlanStatement): CompoundStatementExec =
    node match {
      case body: CompoundBody =>
        // TODO [SPARK-48530]: Current logic doesn't support scoped variables and shadowing.
        val variables = body.collection.flatMap {
          case st: SingleStatement => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVariables = variables
          .map(varName => DropVariable(varName, ifExists = true))
          .map(new SingleStatementExec(_, Origin(), isInternal = true, collectResult = false))
          .reverse

        val conditionHandlerMap = mutable.HashMap[String, ErrorHandlerExec]()
        val handlers = ListBuffer[ErrorHandlerExec]()
        body.handlers.foreach(handler => {
          val handlerBodyExec = transformTreeIntoExecutable(handler.body).
            asInstanceOf[CompoundBodyExec]
          val handlerExec =
            new ErrorHandlerExec(handler.conditions, handlerBodyExec, handler.handlerType)

          handler.conditions.foreach(condition => {
            val conditionValue = body.conditions.getOrElse(condition, condition)
            conditionHandlerMap.put(conditionValue, handlerExec)
          })

          handlers += handlerExec
        })

        new CompoundBodyExec(
          body.collection.
            map(st => transformTreeIntoExecutable(st)) ++ dropVariables,
          handlers.toSeq, conditionHandlerMap, session)
      case sparkStatement: SingleStatement =>
        new SingleStatementExec(
          sparkStatement.parsedPlan,
          sparkStatement.origin,
          isInternal = false)
      case handler: ErrorHandler =>
        val handlerBodyExec = transformTreeIntoExecutable(handler.body).
          asInstanceOf[CompoundBodyExec]
        new ErrorHandlerExec(handler.conditions, handlerBodyExec, handler.handlerType)
      case condition: ErrorCondition =>
        throw new UnsupportedOperationException(
          s"Error condition $condition is not supported in the execution plan.")
    }

  def execute(executionPlan: Iterator[CompoundStatementExec]): Iterator[Array[Row]] = {
    executionPlan.flatMap {
      case statement: SingleStatementExec if statement.collectResult =>
        statement.data
      case _ => None
    }
  }
}
