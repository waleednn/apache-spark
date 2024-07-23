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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ToPrettyString}
import org.apache.spark.sql.types.StringType

/**
 * A command for `SHOW VARIABLES`.
 *
 * The syntax of this command is:
 * {{{
 *    SHOW VARIABLES (LIKE? pattern)?;
 * }}}
 */
case class ShowVariablesCommand(pattern: Option[String]) extends LeafRunnableCommand {

  override val output: Seq[Attribute] = Seq(
    AttributeReference("name", StringType, nullable = false)(),
    AttributeReference("data_type", StringType, nullable = false)(),
    AttributeReference("default_value_sql", StringType, nullable = false)(),
    AttributeReference("value", StringType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val variableManager = sparkSession.sessionState.catalogManager.tempVariableManager
    variableManager.listViewNames(pattern.getOrElse("*")).map {
      case (name, vd) =>
        Row(name,
          vd.currentValue.dataType.sql,
          vd.defaultValueSQL,
          ToPrettyString(vd.currentValue).eval(null))
    }
  }
}
