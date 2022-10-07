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

package org.apache.spark.sql.catalyst

/**
 * Interface defines the following methods for a data source:
 *  - register a new option name
 *  - retrieve all registered option names
 *  - valid a given option name
 *  - get alternative option name if any
 */
trait DataSourceOptions {
  // Option -> Alternative Option if any
  private val validOptions = collection.mutable.Map[String, Option[String]]()

  /**
   * Register a new Option.
   * @param name The primary option name
   * @param alternative Alternative option name if any
   */
  protected def newOption(name: String, alternative: Option[String] = None): String = {
    // Register both of the options
    validOptions += (name -> alternative)
    validOptions ++ alternative.map(alterName => alterName -> Some(name))
    name
  }

  /**
   * @return All valid file source options
   */
  def getAllValidOptionNames: scala.collection.Set[String] = validOptions.keySet

  /**
   * @param name Option name to be validated
   * @return if the given Option name is valid
   */
  def isValidOptionName(name: String): Boolean = validOptions.contains(name)

  /**
   * @param name Option name
   * @return Alternative option name if any
   */
  def getAlternativeOptionName(name: String): Option[String] = validOptions.getOrElse(name, None)
}
