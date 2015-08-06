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

package org.apache.spark.ml.feature

import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.mllib.linalg.VectorUDT
import org.apache.spark.sql.types._

/**
 * Represents a parsed R formula.
 */
private[ml] case class ParsedRFormula(label: ColumnRef, terms: Seq[Term]) {
  /**
   * Resolves formula terms into column names. A schema is necessary for inferring the meaning
   * of the special '.' term. Duplicate terms will be removed during resolution.
   */
  def resolve(schema: StructType): ResolvedRFormula = {
    var includedTerms = Seq[Seq[String]]()
    terms.foreach {
      case term: ColumnRef =>
        includedTerms :+= Seq(term.value)
      case ColumnInteraction(terms) =>
        includedTerms ++= expandInteraction(schema, terms)
      case Dot =>
        includedTerms ++= simpleTypes(schema).filter(_ != label.value).map(Seq(_))
      case Deletion(term: Term) =>
        term match {
          case inner: ColumnRef =>
            includedTerms = includedTerms.filter(_ != Seq(inner.value))
          case ColumnInteraction(terms) =>
            val fromInteraction = expandInteraction(schema, terms)
            includedTerms = includedTerms.filter(!fromInteraction.contains(_))
          case Dot =>
            // e.g. "- .", which removes all first-order terms
            val fromSchema = simpleTypes(schema)
            includedTerms = includedTerms.filter {
              case Seq(t) => !fromSchema.contains(t)
              case _ => true
            }
          case _: Deletion =>
            assert(false, "Deletion terms cannot be nested")
          case _: Intercept =>
        }
      case _: Intercept =>
    }
    ResolvedRFormula(label.value, includedTerms.distinct)
  }

  /** Whether this formula specifies fitting with an intercept term. */
  def hasIntercept: Boolean = {
    var intercept = true
    terms.foreach {
      case Intercept(enabled) =>
        intercept = enabled
      case Deletion(Intercept(enabled)) =>
        intercept = !enabled
      case _ =>
    }
    intercept
  }

  private def expandInteraction(
      schema: StructType, terms: Seq[InteractionComponent]): Seq[Seq[String]] = {
    if (terms.isEmpty) {
      Seq(Nil)
    } else {
      terms.head match {
        case Dot =>
          val rest = expandInteraction(schema, terms.tail)
          simpleTypes(schema).filter(_ != label.value).map { t =>
            Seq(t) ++ rest
          }
        case ColumnRef(value) =>
          (Seq(value) ++ expandInteraction(schema, terms.tail))
      }
    }
  }

  // the dot operator excludes complex column types
  private def simpleTypes(schema: StructType): Seq[String] = {
    schema.fields.filter(_.dataType match {
      case _: NumericType | StringType | BooleanType | _: VectorUDT => true
      case _ => false
    }).map(_.name)
  }
}

/**
 * Represents a fully evaluated and simplified R formula.
 */
private[ml] case class ResolvedRFormula(label: String, terms: Seq[Seq[String]])

/**
 * R formula terms. See the R formula docs here for more information:
 * http://stat.ethz.ch/R-manual/R-patched/library/stats/html/formula.html
 */
private[ml] sealed trait Term

/** A term that may be part of an interaction, e.g. 'x' in 'x:y' */
private[ml] sealed trait InteractionComponent extends Term

/* R formula reference to all available columns, e.g. "." in a formula */
private[ml] case object Dot extends InteractionComponent

/* R formula reference to a column, e.g. "+ Species" in a formula */
private[ml] case class ColumnRef(value: String) extends InteractionComponent

/* R formula interaction of several columns, e.g. "Sepal_Length:Species" in a formula */
private[ml] case class ColumnInteraction(terms: Seq[InteractionComponent]) extends Term

/* R formula intercept toggle, e.g. "+ 0" in a formula */
private[ml] case class Intercept(enabled: Boolean) extends Term

/* R formula deletion of a variable, e.g. "- Species" in a formula */
private[ml] case class Deletion(term: Term) extends Term

/**
 * Limited implementation of R formula parsing. Currently supports: '~', '+', '-', '.'.
 */
private[ml] object RFormulaParser extends RegexParsers {
  def intercept: Parser[Intercept] =
    "([01])".r ^^ { case a => Intercept(a == "1") }

  def columnRef: Parser[ColumnRef] =
    "([a-zA-Z]|\\.[a-zA-Z_])[a-zA-Z0-9._]*".r ^^ { case a => ColumnRef(a) }

  def dot: Parser[InteractionComponent] = "\\.".r ^^ { case _ => Dot } 

  def interaction: Parser[List[InteractionComponent]] = repsep(columnRef | dot, ":")

  def term: Parser[Term] = intercept |
    interaction ^^ {
      case Seq(term) => term
      case terms => ColumnInteraction(terms)
    }

  def terms: Parser[List[Term]] = (term ~ rep("+" ~ term | "-" ~ term)) ^^ {
    case op ~ list => list.foldLeft(List(op)) {
      case (left, "+" ~ right) => left ++ Seq(right)
      case (left, "-" ~ right) => left ++ Seq(Deletion(right))
    }
  }

  def formula: Parser[ParsedRFormula] =
    (columnRef ~ "~" ~ terms) ^^ { case r ~ "~" ~ t => ParsedRFormula(r, t) }

  def parse(value: String): ParsedRFormula = parseAll(formula, value) match {
    case Success(result, _) => result
    case failure: NoSuccess => throw new IllegalArgumentException(
      "Could not parse formula: " + value)
  }
}
