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

package org.apache.spark.ml.util

import java.util.UUID

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.Param
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
 * A small wrapper that defines a training session for an estimator, and some methods to log
 * useful information during this session.
 *
 * A new instance is expected to be created within fit().
 *
 * @param estimator the estimator that is being fit
 * @param dataset the training dataset
 * @tparam E the type of the estimator
 */
private[spark] class Instrumentation[E <: Estimator[_]] private (
    estimator: E, dataset: RDD[_]) extends Logging {

  private val id = UUID.randomUUID()
  private val prefix = {
    val className = estimator.getClass.getSimpleName
    s"$className-${estimator.uid}-${dataset.hashCode()}-$id: "
  }

  init()

  private def init(): Unit = {
    log(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  /**
   * Logs a warning message with a prefix that uniquely identifies the training session.
   */
  override def logWarning(msg: => String): Unit = {
    super.logWarning(prefix + msg)
  }

  /**
   * Logs a error message with a prefix that uniquely identifies the training session.
   */
  override def logError(msg: => String): Unit = {
    super.logError(prefix + msg)
  }

  /**
   * Logs an info message with a prefix that uniquely identifies the training session.
   */
  override def logInfo(msg: => String): Unit = {
    super.logInfo(prefix + msg)
  }

  /**
   * Alias for logInfo, see above.
   */
  def log(msg: String): Unit = logInfo(msg)

  /**
   * Logs the value of the given parameters for the estimator being used in this session.
   */
  def logParams(params: Param[_]*): Unit = {
    val pairs: Seq[(String, JValue)] = for {
      p <- params
      value <- estimator.get(p)
    } yield {
      val cast = p.asInstanceOf[Param[Any]]
      p.name -> parse(cast.jsonEncode(value))
    }
    log(compact(render(map2jvalue(pairs.toMap))))
  }

  def logNumFeatures(num: Long): Unit = {
    logNamedValue(Instrumentation.loggerTags.numFeatures, num)
  }

  def logNumClasses(num: Long): Unit = {
    logNamedValue(Instrumentation.loggerTags.numClasses, num)
  }

  /**
   * Logs the value with customized name field.
   */
  def logNamedValue(name: String, value: String): Unit = {
    log(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Long): Unit = {
    log(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Double): Unit = {
    log(compact(render(name -> value)))
  }

  /**
   * Logs the successful completion of the training session.
   */
  def logSuccess(model: Model[_]): Unit = {
    log(s"training finished")
  }
}

/**
 * Some common methods for logging information about a training session.
 */
private[spark] object Instrumentation {

  object loggerTags {
    val numFeatures = "numFeatures"
    val numClasses = "numClasses"
    val numExamples = "numExamples"
    val meanOfLabels = "meanOfLabels"
    val varianceOfLabels = "varianceOfLabels"
  }

  /**
   * Creates an instrumentation object for a training session.
   */
  def create[E <: Estimator[_]](
      estimator: E, dataset: Dataset[_]): Instrumentation[E] = {
    create[E](estimator, dataset.rdd)
  }

  /**
   * Creates an instrumentation object for a training session.
   */
  def create[E <: Estimator[_]](
      estimator: E, dataset: RDD[_]): Instrumentation[E] = {
    new Instrumentation[E](estimator, dataset)
  }

}

private[spark] class OptionalInstrument private (
    val instrument: Option[Instrumentation[_ <: Estimator[_]]],
    val className: String) extends Logging {

  def this(instr: Instrumentation[_ <: Estimator[_]]) = this(Some(instr), "")

  def this(clazz: Class[_]) = this(None, clazz.getName.stripSuffix("$"))

  protected override def logName = className

  override def logInfo(msg: => String) {
    instrument match {
      case Some(instr) => instr.logInfo(msg)
      case None => super.logInfo(msg)
    }
  }

  override def logWarning(msg: => String) {
    instrument match {
      case Some(instr) => instr.logWarning(msg)
      case None => super.logWarning(msg)
    }
  }

  override def logError(msg: => String) {
    instrument match {
      case Some(instr) => instr.logError(msg)
      case None => super.logError(msg)
    }
  }
}
