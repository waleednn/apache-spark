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

import java.util.Locale
import java.util.regex.{MatchResult, Pattern}

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{GenericArrayData, StringUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


abstract class StringRegexExpression extends BinaryExpression
  with ImplicitCastInputTypes with NullIntolerant {

  def escape(v: String): String
  def matches(regex: Pattern, str: String): Boolean

  override def dataType: DataType = BooleanType
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType)

  // try cache the pattern for Literal
  private lazy val cache: Pattern = right match {
    case x @ Literal(value: String, StringType) => compile(value)
    case _ => null
  }

  protected def compile(str: String): Pattern = if (str == null) {
    null
  } else {
    // Let it raise exception if couldn't compile the regex string
    Pattern.compile(escape(str))
  }

  protected def pattern(str: String) = if (cache == null) compile(str) else cache

  protected override def nullSafeEval(input1: Any, input2: Any): Any = {
    val regex = pattern(input2.asInstanceOf[UTF8String].toString)
    if(regex == null) {
      null
    } else {
      matches(regex, input1.asInstanceOf[UTF8String].toString)
    }
  }

  override def sql: String = s"${left.sql} ${prettyName.toUpperCase(Locale.ROOT)} ${right.sql}"
}

abstract class EscapeRegexExpression(escapeChar: Char) extends StringRegexExpression {

  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).matches()

  override def toString: String = escapeChar match {
    case '\\' => s"$left LIKE $right"
    case c => s"$left LIKE $right ESCAPE '$c'"
  }

  protected def getEscapeFunc: String

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName
    val escapeFunc = getEscapeFunc

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(escape(rVal.asInstanceOf[UTF8String].toString()))
        val pattern = ctx.addMutableState(patternClass, "patternLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).matches();
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val pattern = ctx.freshName("pattern")
      val rightStr = ctx.freshName("rightStr")
      // We need double escape to avoid org.codehaus.commons.compiler.CompileException.
      // '\\' will cause exception 'Single quote must be backslash-escaped in character literal'.
      // '\"' will cause exception 'Line break in literal not allowed'.
      val newEscapeChar = if (escapeChar == '\"' || escapeChar == '\\') {
        s"""\\\\\\$escapeChar"""
      } else {
        escapeChar
      }
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile($escapeFunc($rightStr, '$newEscapeChar'));
          ${ev.value} = $pattern.matcher($eval1.toString()).matches();
        """
      })
    }
  }
}

// scalastyle:off line.contains.tab
/**
 * Simple RegEx pattern matching function
 */
@ExpressionDescription(
  usage = "str _FUNC_ pattern[ ESCAPE escape] - Returns true if str matches `pattern` with " +
    "`escape`, null if any arguments are null, false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * pattern - a string expression. The pattern is a string which is matched literally, with
          exception to the following special symbols:

          _ matches any one character in the input (similar to . in posix regular expressions)

          % matches zero or more characters in the input (similar to .* in posix regular
          expressions)

          Since Spark 2.0, string literals are unescaped in our SQL parser. For example, in order
          to match "\abc", the pattern should be "\\abc".

          When SQL config 'spark.sql.parser.escapedStringLiterals' is enabled, it fallbacks
          to Spark 1.6 behavior regarding string literal parsing. For example, if the config is
          enabled, the pattern to match "\abc" should be "\abc".
      * escape - an character added since Spark 3.0. The default escape character is the '\'.
          If an escape character precedes a special symbol or another escape character, the
          following character is matched literally. It is invalid to escape any other character.
  """,
  examples = """
    Examples:
      > SET spark.sql.parser.escapedStringLiterals=true;
      spark.sql.parser.escapedStringLiterals	true
      > SELECT '%SystemDrive%\Users\John' _FUNC_ '\%SystemDrive\%\\Users%';
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT '%SystemDrive%\\Users\\John' _FUNC_ '\%SystemDrive\%\\\\Users%';
      true
      > SELECT '%SystemDrive%/Users/John' _FUNC_ '/%SystemDrive/%//Users%' ESCAPE '/';
      true
  """,
  note = """
    Use RLIKE to match with standard regular expressions.
  """,
  since = "1.0.0")
// scalastyle:on line.contains.tab
case class Like(left: Expression, right: Expression, escapeChar: Char = '\\')
  extends EscapeRegexExpression(escapeChar) {

  override def escape(v: String): String = StringUtils.escapeLikeRegex(v, escapeChar)

  override def getEscapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeLikeRegex"

}

// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "str _FUNC_ regexp - Returns true if `str` matches `regexp`, or false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * regexp - a string expression. The regex string should be a Java regular expression.

          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".

          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
  """,
  examples = """
    Examples:
      > SET spark.sql.parser.escapedStringLiterals=true;
      spark.sql.parser.escapedStringLiterals	true
      > SELECT '%SystemDrive%\Users\John' _FUNC_ '%SystemDrive%\\Users.*';
      true
      > SET spark.sql.parser.escapedStringLiterals=false;
      spark.sql.parser.escapedStringLiterals	false
      > SELECT '%SystemDrive%\\Users\\John' _FUNC_ '%SystemDrive%\\\\Users.*';
      true
  """,
  note = """
    Use LIKE to match with simple string pattern.
  """,
  since = "1.0.0")
// scalastyle:on line.contains.tab
case class RLike(left: Expression, right: Expression) extends StringRegexExpression {

  override def escape(v: String): String = v
  override def matches(regex: Pattern, str: String): Boolean = regex.matcher(str).find(0)
  override def toString: String = s"$left RLIKE $right"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val patternClass = classOf[Pattern].getName

    if (right.foldable) {
      val rVal = right.eval()
      if (rVal != null) {
        val regexStr =
          StringEscapeUtils.escapeJava(rVal.asInstanceOf[UTF8String].toString())
        val pattern = ctx.addMutableState(patternClass, "patternRLike",
          v => s"""$v = $patternClass.compile("$regexStr");""")

        // We don't use nullSafeCodeGen here because we don't want to re-evaluate right again.
        val eval = left.genCode(ctx)
        ev.copy(code = code"""
          ${eval.code}
          boolean ${ev.isNull} = ${eval.isNull};
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
          if (!${ev.isNull}) {
            ${ev.value} = $pattern.matcher(${eval.value}.toString()).find(0);
          }
        """)
      } else {
        ev.copy(code = code"""
          boolean ${ev.isNull} = true;
          ${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
        """)
      }
    } else {
      val rightStr = ctx.freshName("rightStr")
      val pattern = ctx.freshName("pattern")
      nullSafeCodeGen(ctx, ev, (eval1, eval2) => {
        s"""
          String $rightStr = $eval2.toString();
          $patternClass $pattern = $patternClass.compile($rightStr);
          ${ev.value} = $pattern.matcher($eval1.toString()).find(0);
        """
      })
    }
  }
}

// scalastyle:off line.contains.tab
@ExpressionDescription(
  usage = "str _FUNC_ regexp[ ESCAPE escape] - Returns true if `str` matches `regexp` with " +
    "`escape`, null if any arguments are null, false otherwise.",
  arguments = """
    Arguments:
      * str - a string expression
      * regexp - a string expression. The regex string should be a Java regular expression.
          Since Spark 2.0, string literals (including regex patterns) are unescaped in our SQL
          parser. For example, to match "\abc", a regular expression for `regexp` can be
          "^\\abc$".
          There is a SQL config 'spark.sql.parser.escapedStringLiterals' that can be used to
          fallback to the Spark 1.6 behavior regarding string literal parsing. For example,
          if the config is enabled, the `regexp` that can match "\abc" is "^\abc$".
      * escape - an character added since Spark 3.0. The default escape character is the '\'.
          If an escape character precedes a special symbol or another escape character, the
          following character is matched literally. It is invalid to escape any other character.
  """,
  examples = """
    Examples:
      > SELECT 'abc' SIMILAR TO '%(b|d)%';
      true
      > SELECT 'abc' SIMILAR TO '(b|c)%';
      false
      > SELECT '|bd' SIMILAR TO '\\|(b|d)*' ESCAPE '\\';
      true
      > SELECT '|bd' SIMILAR TO '\\|(b|d)*' ESCAPE '#';
      false
      > SELECT '|bd' SIMILAR TO '#|(b|d)*' ESCAPE '#';
      true
  """,
  note = """
    [[SimilarTo]] is similar to [[RLike]], but with the following differences:
    1. The SIMILAR TO operator returns true only if its pattern matches the entire string,
      unlike [[Rlike]] behavior, where the pattern can match any portion of the string.
    2. The regex string allow uses _ and % as wildcard characters denoting any single character and
      any string, respectively (these are comparable to . and .* in POSIX regular expressions).
    3. The regex string allow uses escape character like [[Like]] behavior.
    4. The period (.) is not a metacharacter for [[SimilarTo]].
  """,
  since = "1.0.0")
// scalastyle:on line.contains.tab
case class SimilarTo(left: Expression, right: Expression, escapeChar: Char = '\\')
  extends escapeRegexExpression(escapeChar) {

  override def escape(v: String): String = StringUtils.escapeSimilarRegex(v, escapeChar)

  override def getEscapeFunc = StringUtils.getClass.getName.stripSuffix("$") + ".escapeSimilarRegex"

}

/**
 * Splits str around matches of the given regex.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regex, limit) - Splits `str` around occurrences that match `regex`" +
    " and returns an array with a length of at most `limit`",
  arguments = """
    Arguments:
      * str - a string expression to split.
      * regex - a string representing a regular expression. The regex string should be a
        Java regular expression.
      * limit - an integer expression which controls the number of times the regex is applied.
          * limit > 0: The resulting array's length will not be more than `limit`,
            and the resulting array's last entry will contain all input
            beyond the last matched regex.
          * limit <= 0: `regex` will be applied as many times as possible, and
            the resulting array can be of any size.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]');
       ["one","two","three",""]
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]', -1);
       ["one","two","three",""]
      > SELECT _FUNC_('oneAtwoBthreeC', '[ABC]', 2);
       ["one","twoBthreeC"]
  """,
  since = "1.5.0")
case class StringSplit(str: Expression, regex: Expression, limit: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  override def dataType: DataType = ArrayType(StringType)
  override def inputTypes: Seq[DataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = str :: regex :: limit :: Nil

  def this(exp: Expression, regex: Expression) = this(exp, regex, Literal(-1));

  override def nullSafeEval(string: Any, regex: Any, limit: Any): Any = {
    val strings = string.asInstanceOf[UTF8String].split(
      regex.asInstanceOf[UTF8String], limit.asInstanceOf[Int])
    new GenericArrayData(strings.asInstanceOf[Array[Any]])
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val arrayClass = classOf[GenericArrayData].getName
    nullSafeCodeGen(ctx, ev, (str, regex, limit) => {
      // Array in java is covariant, so we don't need to cast UTF8String[] to Object[].
      s"""${ev.value} = new $arrayClass($str.split($regex,$limit));""".stripMargin
    })
  }

  override def prettyName: String = "split"
}


/**
 * Replace all substrings of str that match regexp with rep.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(str, regexp, rep) - Replaces all substrings of `str` that match `regexp` with `rep`.",
  examples = """
    Examples:
      > SELECT _FUNC_('100-200', '(\\d+)', 'num');
       num-num
  """,
  since = "1.5.0")
// scalastyle:on line.size.limit
case class RegExpReplace(subject: Expression, regexp: Expression, rep: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _
  // last replacement string, we don't want to convert a UTF8String => java.langString every time.
  @transient private var lastReplacement: String = _
  @transient private var lastReplacementInUTF8: UTF8String = _
  // result buffer write by Matcher
  @transient private lazy val result: StringBuffer = new StringBuffer

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    if (!r.equals(lastReplacementInUTF8)) {
      // replacement string changed
      lastReplacementInUTF8 = r.asInstanceOf[UTF8String].clone()
      lastReplacement = lastReplacementInUTF8.toString
    }
    val m = pattern.matcher(s.toString())
    result.delete(0, result.length())

    while (m.find) {
      m.appendReplacement(result, lastReplacement)
    }
    m.appendTail(result)

    UTF8String.fromString(result.toString)
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, StringType)
  override def children: Seq[Expression] = subject :: regexp :: rep :: Nil
  override def prettyName: String = "regexp_replace"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val termResult = ctx.freshName("termResult")

    val classNamePattern = classOf[Pattern].getCanonicalName
    val classNameStringBuffer = classOf[java.lang.StringBuffer].getCanonicalName

    val matcher = ctx.freshName("matcher")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")
    val termLastReplacement = ctx.addMutableState("String", "lastReplacement")
    val termLastReplacementInUTF8 = ctx.addMutableState("UTF8String", "lastReplacementInUTF8")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, rep) => {
    s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp.clone();
        $termPattern = $classNamePattern.compile($termLastRegex.toString());
      }
      if (!$rep.equals($termLastReplacementInUTF8)) {
        // replacement string changed
        $termLastReplacementInUTF8 = $rep.clone();
        $termLastReplacement = $termLastReplacementInUTF8.toString();
      }
      $classNameStringBuffer $termResult = new $classNameStringBuffer();
      java.util.regex.Matcher $matcher = $termPattern.matcher($subject.toString());

      while ($matcher.find()) {
        $matcher.appendReplacement($termResult, $termLastReplacement);
      }
      $matcher.appendTail($termResult);
      ${ev.value} = UTF8String.fromString($termResult.toString());
      $termResult = null;
      $setEvNotNull
    """
    })
  }
}

/**
 * Extract a specific(idx) group identified by a Java regex.
 *
 * NOTE: this expression is not THREAD-SAFE, as it has some internal mutable status.
 */
@ExpressionDescription(
  usage = "_FUNC_(str, regexp[, idx]) - Extracts a group that matches `regexp`.",
  examples = """
    Examples:
      > SELECT _FUNC_('100-200', '(\\d+)-(\\d+)', 1);
       100
  """,
  since = "1.5.0")
case class RegExpExtract(subject: Expression, regexp: Expression, idx: Expression)
  extends TernaryExpression with ImplicitCastInputTypes {
  def this(s: Expression, r: Expression) = this(s, r, Literal(1))

  // last regex in string, we will update the pattern iff regexp value changed.
  @transient private var lastRegex: UTF8String = _
  // last regex pattern, we cache it for performance concern
  @transient private var pattern: Pattern = _

  override def nullSafeEval(s: Any, p: Any, r: Any): Any = {
    if (!p.equals(lastRegex)) {
      // regex value changed
      lastRegex = p.asInstanceOf[UTF8String].clone()
      pattern = Pattern.compile(lastRegex.toString)
    }
    val m = pattern.matcher(s.toString)
    if (m.find) {
      val mr: MatchResult = m.toMatchResult
      val group = mr.group(r.asInstanceOf[Int])
      if (group == null) { // Pattern matched, but not optional group
        UTF8String.EMPTY_UTF8
      } else {
        UTF8String.fromString(group)
      }
    } else {
      UTF8String.EMPTY_UTF8
    }
  }

  override def dataType: DataType = StringType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType, IntegerType)
  override def children: Seq[Expression] = subject :: regexp :: idx :: Nil
  override def prettyName: String = "regexp_extract"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val classNamePattern = classOf[Pattern].getCanonicalName
    val matcher = ctx.freshName("matcher")
    val matchResult = ctx.freshName("matchResult")

    val termLastRegex = ctx.addMutableState("UTF8String", "lastRegex")
    val termPattern = ctx.addMutableState(classNamePattern, "pattern")

    val setEvNotNull = if (nullable) {
      s"${ev.isNull} = false;"
    } else {
      ""
    }

    nullSafeCodeGen(ctx, ev, (subject, regexp, idx) => {
      s"""
      if (!$regexp.equals($termLastRegex)) {
        // regex value changed
        $termLastRegex = $regexp.clone();
        $termPattern = $classNamePattern.compile($termLastRegex.toString());
      }
      java.util.regex.Matcher $matcher =
        $termPattern.matcher($subject.toString());
      if ($matcher.find()) {
        java.util.regex.MatchResult $matchResult = $matcher.toMatchResult();
        if ($matchResult.group($idx) == null) {
          ${ev.value} = UTF8String.EMPTY_UTF8;
        } else {
          ${ev.value} = UTF8String.fromString($matchResult.group($idx));
        }
        $setEvNotNull
      } else {
        ${ev.value} = UTF8String.EMPTY_UTF8;
        $setEvNotNull
      }"""
    })
  }
}
