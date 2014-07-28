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

import org.apache.spark.sql.catalyst.types.DataType

case class ScalaUdf(function: AnyRef, dataType: DataType, children: Seq[Expression])
  extends Expression {

  type EvaluatedType = Any

  def references = children.flatMap(_.references).toSet
  def nullable = true

  /** This method has been generated by this script

    (1 to 22).map { x =>
      val anys = (1 to x).map(x => "Any").reduce(_ + ", " + _)
      val evals = (0 to x - 1).map(x => s"children($x).eval(input)").reduce(_ + ",\n    " + _)

    s"""
    case $x =>
      function.asInstanceOf[($anys) => Any](
      $evals)
    """
    }

  */

  // scalastyle:off
  override def eval(input: Row): Any = {
    children.size match {
      case 1 => function.asInstanceOf[(Any) => Any](children(0).eval(input))
      case 2 =>
        function.asInstanceOf[(Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input))
      case 3 =>
        function.asInstanceOf[(Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input))
      case 4 =>
        function.asInstanceOf[(Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input))
      case 5 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input))
      case 6 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input))
      case 7 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input))
      case 8 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input))
      case 9 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input))
      case 10 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input))
      case 11 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input))
      case 12 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input))
      case 13 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input))
      case 14 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input))
      case 15 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input))
      case 16 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input))
      case 17 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input))
      case 18 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input),
          children(17).eval(input))
      case 19 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input),
          children(17).eval(input),
          children(18).eval(input))
      case 20 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input),
          children(17).eval(input),
          children(18).eval(input),
          children(19).eval(input))
      case 21 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input),
          children(17).eval(input),
          children(18).eval(input),
          children(19).eval(input),
          children(20).eval(input))
      case 22 =>
        function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any](
          children(0).eval(input),
          children(1).eval(input),
          children(2).eval(input),
          children(3).eval(input),
          children(4).eval(input),
          children(5).eval(input),
          children(6).eval(input),
          children(7).eval(input),
          children(8).eval(input),
          children(9).eval(input),
          children(10).eval(input),
          children(11).eval(input),
          children(12).eval(input),
          children(13).eval(input),
          children(14).eval(input),
          children(15).eval(input),
          children(16).eval(input),
          children(17).eval(input),
          children(18).eval(input),
          children(19).eval(input),
          children(20).eval(input),
          children(21).eval(input))
    }
    // scalastyle:on
  }
}
