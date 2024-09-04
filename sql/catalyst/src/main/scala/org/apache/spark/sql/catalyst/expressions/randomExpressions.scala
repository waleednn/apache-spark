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

import scala.util.Random

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedSeed}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.ExpectsInputTypes.{ordinalNumber, toSQLExpr, toSQLType}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.{BinaryLike, TernaryLike}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXPRESSION_WITH_RANDOM_SEED, RUNTIME_REPLACEABLE, TreePattern}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.random.XORShiftRandom

/**
 * A Random distribution generating expression.
 * TODO: This can be made generic to generate any type of random distribution, or any type of
 * StructType.
 *
 * Since this expression is stateful, it cannot be a case object.
 */
abstract class RDG extends UnaryExpression with ExpectsInputTypes with Nondeterministic
  with ExpressionWithRandomSeed {
  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  override def stateful: Boolean = true

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
  }

  override def seedExpression: Expression = child

  @transient protected lazy val seed: Long = seedExpression match {
    case e if e.dataType == IntegerType => e.eval().asInstanceOf[Int]
    case e if e.dataType == LongType => e.eval().asInstanceOf[Long]
  }

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(IntegerType, LongType))
}

/**
 * Represents the behavior of expressions which have a random seed and can renew the seed.
 * Usually the random seed needs to be renewed at each execution under streaming queries.
 */
trait ExpressionWithRandomSeed extends Expression {
  override val nodePatterns: Seq[TreePattern] = Seq(EXPRESSION_WITH_RANDOM_SEED)

  def seedExpression: Expression
  def withNewSeed(seed: Long): Expression
}

private[catalyst] object ExpressionWithRandomSeed {
  def expressionToSeed(e: Expression, source: String): Option[Long] = e match {
    case LongLiteral(seed) => Some(seed)
    case Literal(null, _) => None
    case _ => throw QueryCompilationErrors.invalidRandomSeedParameter(source, e)
  }
}

/** Generate a random column with i.i.d. uniformly distributed values in [0, 1). */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) uniformly distributed values in [0, 1).",
  examples = """
    Examples:
      > SELECT _FUNC_();
       0.9629742951434543
      > SELECT _FUNC_(0);
       0.7604953758285915
      > SELECT _FUNC_(null);
       0.7604953758285915
  """,
  note = """
    The function is non-deterministic in general case.
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Rand(child: Expression, hideSeed: Boolean = false) extends RDG {

  def this() = this(UnresolvedSeed, true)

  def this(child: Expression) = this(child, false)

  override def withNewSeed(seed: Long): Rand = Rand(Literal(seed, LongType), hideSeed)

  override protected def evalInternal(input: InternalRow): Double = rng.nextDouble()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $rngTerm.nextDouble();""",
      isNull = FalseLiteral)
  }

  override def flatArguments: Iterator[Any] = Iterator(child)
  override def sql: String = {
    s"rand(${if (hideSeed) "" else child.sql})"
  }

  override protected def withNewChildInternal(newChild: Expression): Rand = copy(child = newChild)
}

object Rand {
  def apply(seed: Long): Rand = Rand(Literal(seed, LongType))
}

/** Generate a random column with i.i.d. values drawn from the standard normal distribution. */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """_FUNC_([seed]) - Returns a random value with independent and identically distributed (i.i.d.) values drawn from the standard normal distribution.""",
  examples = """
    Examples:
      > SELECT _FUNC_();
       -0.3254147983080288
      > SELECT _FUNC_(0);
       1.6034991609278433
      > SELECT _FUNC_(null);
       1.6034991609278433
  """,
  note = """
    The function is non-deterministic in general case.
  """,
  since = "1.5.0",
  group = "math_funcs")
// scalastyle:on line.size.limit
case class Randn(child: Expression, hideSeed: Boolean = false) extends RDG {

  def this() = this(UnresolvedSeed, true)

  def this(child: Expression) = this(child, false)

  override def withNewSeed(seed: Long): Randn = Randn(Literal(seed, LongType), hideSeed)

  override protected def evalInternal(input: InternalRow): Double = rng.nextGaussian()

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = $rngTerm.nextGaussian();""",
      isNull = FalseLiteral)
  }

  override def flatArguments: Iterator[Any] = Iterator(child)
  override def sql: String = {
    s"randn(${if (hideSeed) "" else child.sql})"
  }

  override protected def withNewChildInternal(newChild: Expression): Randn = copy(child = newChild)
}

object Randn {
  def apply(seed: Long): Randn = Randn(Literal(seed, LongType))
}

@ExpressionDescription(
  usage = """
    _FUNC_(min, max, seed) - Returns a random value with independent and identically
      distributed (i.i.d.) values with the specified range of numbers. The random seed is optional.
      The provided numbers specifying the minimum and maximum values of the range must be constant.
      If both of these numbers are integers, then the result will also be an integer. Otherwise if
      one or both of these are floating-point numbers, then the result will also be a floating-point
      number.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(0, 1);
       -0.3254147983080288
      > SELECT _FUNC_(10, 20, 0);
       26.034991609278433
  """,
  since = "4.0.0",
  group = "math_funcs")
case class Uniform(min: Expression, max: Expression, seed: Expression)
  extends RuntimeReplaceable with TernaryLike[Expression] with ExpressionWithRandomSeed {
  def this(min: Expression, max: Expression) =
    this(min, max, Literal(Uniform.random.nextLong(), LongType))

  final override lazy val deterministic: Boolean = false
  override val nodePatterns: Seq[TreePattern] =
    Seq(RUNTIME_REPLACEABLE, EXPRESSION_WITH_RANDOM_SEED)

  override val dataType: DataType = {
    val first = min.dataType
    val second = max.dataType
    (min.dataType, max.dataType) match {
      case _ if !valid(min) || !valid(max) => NullType
      case (_, LongType) | (LongType, _) if Seq(first, second).forall(integer) => LongType
      case (_, IntegerType) | (IntegerType, _) if Seq(first, second).forall(integer) => IntegerType
      case (_, ShortType) | (ShortType, _) if Seq(first, second).forall(integer) => ShortType
      case (_, DoubleType) | (DoubleType, _) => DoubleType
      case (_, FloatType) | (FloatType, _) => FloatType
      case _ => NullType
    }
  }

  private def valid(e: Expression): Boolean = e.dataType match {
    case _ if !e.foldable => false
    case _: ShortType | _: IntegerType | _: LongType | _: FloatType | _: DoubleType => true
    case _ => false
  }

  private def integer(t: DataType): Boolean = t match {
    case _: ShortType | _: IntegerType | _: LongType => true
    case _ => false
  }

  override def checkInputDataTypes(): TypeCheckResult = {
    var result: TypeCheckResult = TypeCheckResult.TypeCheckSuccess
    Seq(min, max, seed).zipWithIndex.foreach { case (expr: Expression, index: Int) =>
      if (!valid(expr)) {
        result = DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(index),
            "requiredType" -> "constant value of integer or floating-point",
            "inputSql" -> toSQLExpr(expr),
            "inputType" -> toSQLType(expr.dataType)))
      }
    }
    result
  }

  override def first: Expression = min
  override def second: Expression = max
  override def third: Expression = seed

  override def seedExpression: Expression = seed
  override def withNewSeed(newSeed: Long): Expression =
    Uniform(min, max, Literal(newSeed, LongType))

  override def withNewChildrenInternal(
      newFirst: Expression, newSecond: Expression, newThird: Expression): Expression =
    Uniform(newFirst, newSecond, newThird)

  override def replacement: Expression = {
    def cast(e: Expression, to: DataType): Expression = if (e.dataType == to) e else Cast(e, to)
    cast(Add(
      cast(min, DoubleType),
      Multiply(
        Subtract(
          cast(max, DoubleType),
          cast(min, DoubleType)),
        Rand(seed))),
      dataType)
  }
}

object Uniform {
  lazy val random = new Random()
}

@ExpressionDescription(
  usage = """
    _FUNC_(length, seed) - Returns a string of the specified length whose characters are chosen
      uniformly at random from the following pool of characters: 0-9, a-z, A-Z. The random seed is
      optional. The string length must be a constant two-byte or four-byte integer (SMALLINT or INT,
      respectively).
  """,
  examples =
    """
    Examples:
      > SELECT _FUNC_(3, 0);
       abc
  """,
  since = "4.0.0",
  group = "math_funcs")
case class RandStr(length: Expression, override val seedExpression: Expression)
  extends ExpressionWithRandomSeed with BinaryLike[Expression] with Nondeterministic {
  def this(length: Expression) = this(length, Literal(Uniform.random.nextLong(), LongType))

  override def nullable: Boolean = false
  override def dataType: DataType = StringType
  override def stateful: Boolean = true
  override def left: Expression = length
  override def right: Expression = seedExpression

  /**
   * Record ID within each partition. By being transient, the Random Number Generator is
   * reset every time we serialize and deserialize and initialize it.
   */
  @transient protected var rng: XORShiftRandom = _

  @transient protected lazy val seed: Long = seedExpression match {
    case e if e.dataType == IntegerType => e.eval().asInstanceOf[Int]
    case e if e.dataType == LongType => e.eval().asInstanceOf[Long]
  }
  override protected def initializeInternal(partitionIndex: Int): Unit = {
    rng = new XORShiftRandom(seed + partitionIndex)
  }

  override def withNewSeed(newSeed: Long): Expression = RandStr(length, Literal(newSeed, LongType))
  override def withNewChildrenInternal(newFirst: Expression, newSecond: Expression): Expression =
    RandStr(newFirst, newSecond)

  override def checkInputDataTypes(): TypeCheckResult = {
    var result: TypeCheckResult = TypeCheckResult.TypeCheckSuccess
    Seq(length, seedExpression).zipWithIndex.foreach { case (expr: Expression, index: Int) =>
      val valid = expr.dataType match {
        case _ if !expr.foldable => false
        case _: ShortType | _: IntegerType => true
        case _: LongType if index == 1 => true
        case _ => false
      }
      if (!valid) {
        result = DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(index),
            "requiredType" -> "constant value of INT or SMALLINT",
            "inputSql" -> toSQLExpr(expr),
            "inputType" -> toSQLType(expr.dataType)))
      }
    }
    result
  }

  override def evalInternal(input: InternalRow): Any = {
    val numChars: Int = length.eval(input).asInstanceOf[Int]
    val bytes = new Array[Byte](numChars)
    (0 until numChars).foreach { i =>
      val num = (rng.nextInt() % 30).abs
      num match {
        case _ if num < 10 =>
          bytes.update(i, ('0' + num).toByte)
        case _ if num < 20 =>
          bytes.update(i, ('a' + num - 10).toByte)
        case _ =>
          bytes.update(i, ('A' + num - 20).toByte)
      }
    }
    val result: UTF8String = UTF8String.fromBytes(bytes.toArray)
    result
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val className = classOf[XORShiftRandom].getName
    val rngTerm = ctx.addMutableState(className, "rng")
    ctx.addPartitionInitializationStatement(
      s"$rngTerm = new $className(${seed}L + partitionIndex);")
    val eval = length.genCode(ctx)
    ev.copy(code =
      code"""
        |${eval.code}
        |int length = (int)(${eval.value});
        |char[] chars = new char[length];
        |for (int i = 0; i < length; i++) {
        |  int v = Math.abs($rngTerm.nextInt() % 30);
        |  if (v < 10) {
        |    chars[i] = (char)('0' + v);
        |  } else if (v < 20) {
        |    chars[i] = (char)('a' + (v - 10));
        |  } else {
        |    chars[i] = (char)('A' + (v - 20));
        |  }
        |}
        |UTF8String ${ev.value} = UTF8String.fromString(new String(chars));
        |boolean ${ev.isNull} = false;
        |""".stripMargin,
      isNull = FalseLiteral)
  }
}
