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

package org.apache.spark.sql.catalyst.expressions

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "Usage: expr1 [NOT] BETWEEN expr2 AND expr3 - evaluate if `expr1` is [not] in between `expr2` and `expr3`",
  examples = """
    Examples:
      > SELECT 0.5 _FUNC_ 0.1 AND 1.0;
        true
  """,
  arguments = """
    Arguments:
      * input - An expression that is being compared with lower and upper bound.
      * lower - Lower bound of the between check.
      * upper - Upper bound of the between check.
  """,
  since = "4.0.0",
  group = "conditional_funcs")
case class Between private(input: Expression, lower: Expression, upper: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules  {
  def this(input: Expression, lower: Expression, upper: Expression) = {
    this(input, lower, upper, {
      val commonExpr = CommonExpressionDef(input)
      val ref = new CommonExpressionRef(commonExpr)
      val replacement = And(GreaterThanOrEqual(ref, lower), LessThanOrEqual(ref, upper))
      With(replacement, Seq(commonExpr))
    })
  };

  override def parameters: Seq[Expression] = Seq(input, lower, upper)

  override protected def withNewChildInternal(newChild: Expression): Between = {
    copy(replacement = newChild)
  }
}

object Between {
  def apply(input: Expression, lower: Expression, upper: Expression): Between = {
    new Between(input, lower, upper)
  }
}
