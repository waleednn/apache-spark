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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.types.IntegerType

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringKeyHashMap


/** A catalog for looking up user defined functions, used by an [[Analyzer]]. */
trait FunctionRegistry {

  def registerFunction(name: String, builder: FunctionBuilder): Unit

  @throws[AnalysisException]("If function does not exist")
  def lookupFunction(name: String, children: Seq[Expression]): Expression
}

class OverrideFunctionRegistry(underlying: FunctionRegistry) extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    functionBuilders.get(name).map(_(children)).getOrElse(underlying.lookupFunction(name, children))
  }
}

class SimpleFunctionRegistry extends FunctionRegistry {

  private val functionBuilders = StringKeyHashMap[FunctionBuilder](caseSensitive = false)

  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    functionBuilders.put(name, builder)
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    val func = functionBuilders.get(name).getOrElse {
      throw new AnalysisException(s"undefined function $name")
    }
    func(children)
  }
}

/**
 * A trivial catalog that returns an error when a function is requested. Used for testing when all
 * functions are already filled in and the analyzer needs only to resolve attribute references.
 */
object EmptyFunctionRegistry extends FunctionRegistry {
  override def registerFunction(name: String, builder: FunctionBuilder): Unit = {
    throw new UnsupportedOperationException
  }

  override def lookupFunction(name: String, children: Seq[Expression]): Expression = {
    throw new UnsupportedOperationException
  }
}


object FunctionRegistry {

  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    expression[If]("if"),
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    expression[Coalesce]("nvl"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    expression[Sqrt]("sqrt"),

    // math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    expression[Bin]("bin"),
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling"),
    expression[Cos]("cos"),
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    expression[Hypot]("hypot"),
    expression[Logarithm]("log"),
    expression[Log]("ln"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    expression[UnaryMinus]("negative"),
    expression[Pi]("pi"),
    expression[Log2]("log2"),
    expression[Pow]("pow"),
    expression[Pow]("power"),
    expression[UnaryPositive]("positive"),
    expression[Rint]("rint"),
    expression[Signum]("sign"),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[Tan]("tan"),
    expression[Tanh]("tanh"),
    expression[ToDegrees]("degrees"),
    expression[ToRadians]("radians"),

    // misc functions
    expression[Md5]("md5"),

    // aggregate functions
    expression[Average]("avg"),
    expression[Count]("count"),
    expression[First]("first"),
    expression[Last]("last"),
    expression[Max]("max"),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    expression[Lower]("lcase"),
    expression[Lower]("lower"),
    expression[StringLength]("length"),
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    expression[Upper]("ucase"),
    expression[Upper]("upper"),

    // window functions
    leaf("row_number", WindowFunction.rowNumber()),
    leaf("rank", WindowFunction.rank()),
    leaf("dense_rank", WindowFunction.denseRank()),
    leaf("percent_rank", WindowFunction.percentRank()),
    leaf("cume_dist", WindowFunction.cumeDist()),
    ntile,
    lead,
    lag
  )

  val builtin: FunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, builder) => fr.registerFunction(name, builder) }
    fr
  }

  /** See usage above. */
  private def expression[T <: Expression](name: String)
      (implicit tag: ClassTag[T]): (String, FunctionBuilder) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params : _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new AnalysisException(s"Invalid number of arguments for function $name")
        }
        f.newInstance(expressions : _*).asInstanceOf[Expression]
      }
    }
    (name, builder)
  }

  /** Add a leaf expression. */
  private def leaf(name: String, value: => Expression): (String, FunctionBuilder) = {
    val f = (args: Seq[Expression]) => {
      if (!args.isEmpty) {
        throw new AnalysisException(s"Invalid number of arguments for function $name")
      }
      value
    }
    (name, f)
  }

  private def ntile: (String, FunctionBuilder) = {
    val f = (args: Seq[Expression]) => {
      args match {
        case IntegerLiteral(buckets) :: Nil =>
          WindowFunction.ntile(buckets)
        case _ =>
          throw new AnalysisException(s"Invalid arguments for function ntile: $args")
      }
    }
    ("ntile", f)
  }

  private def lead: (String, FunctionBuilder) = {
    val f = (args: Seq[Expression]) => {
      val (e, offset, default) = leadLagParams("lead", args)
      WindowFunction.lead(e, offset, default)
    }
    ("lead", f)
  }

  private def lag: (String, FunctionBuilder) = {
    val f = (args: Seq[Expression]) => {
      val (e, offset, default) = leadLagParams("lag", args)
      WindowFunction.lag(e, offset, default)
    }
    ("lag", f)
  }

  private def leadLagParams(name: String, args: Seq[Expression]): (Expression, Int, Expression) = {
    args match {
      case Seq(e: Expression) =>
        (e, 1, null)
      case Seq(e: Expression, ExtractInteger(offset)) =>
        (e, offset, null)
      case Seq(e: Expression, ExtractInteger(offset), d: Expression) =>
        (e, offset, d)
      case _ =>
        println(args)
        throw new AnalysisException(s"Invalid arguments for function $name: $args")
    }
  }

  object ExtractInteger {
    def unapply(e: Expression): Option[Integer] = {
      if (e.foldable && e.dataType == IntegerType) {
        Some(e.eval(EmptyRow).asInstanceOf[Integer])
      } else None
    }
  }
}
