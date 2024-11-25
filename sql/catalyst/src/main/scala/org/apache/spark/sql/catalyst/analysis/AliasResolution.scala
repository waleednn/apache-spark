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

import org.apache.spark.sql.catalyst.analysis.MultiAlias
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  Cast,
  Expression,
  ExtractValue,
  Generator,
  GeneratorOuter,
  Literal,
  NamedExpression,
  ScopedExpression
}
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_ALIAS
import org.apache.spark.sql.catalyst.util.{toPrettySQL, AUTO_GENERATED_ALIAS}
import org.apache.spark.sql.types.MetadataBuilder

object AliasResolution {
  def hasUnresolvedAlias(exprs: Seq[NamedExpression]): Boolean = {
    exprs.exists(_.exists(_.isInstanceOf[UnresolvedAlias]))
  }

  def assignAliases(exprs: Seq[NamedExpression]): Seq[NamedExpression] = {
    exprs
      .map(_.transformUpWithPruning(_.containsPattern(UNRESOLVED_ALIAS)) {
        case u: UnresolvedAlias => resolve(u)
      })
      .asInstanceOf[Seq[NamedExpression]]
  }

  def resolve(u: UnresolvedAlias): Expression = {
    val UnresolvedAlias(child, optGenAliasFunc) = u
    child match {
      case ne: NamedExpression => ne
      case go @ GeneratorOuter(g: Generator) if g.resolved => MultiAlias(go, Nil)
      case e if !e.resolved => u
      case g: Generator => MultiAlias(g, Nil)
      case c @ Cast(ne: NamedExpression, _, _, _) => Alias(c, ne.name)()
      case se @ ScopedExpression(e: Expression, _) =>
        resolve(UnresolvedAlias(e, optGenAliasFunc)) match {
          case ne: NamedExpression => Alias(se, ne.name)()
          case _ => se
        }
      case e: ExtractValue if extractOnly(e) => Alias(e, toPrettySQL(e))()
      case e if optGenAliasFunc.isDefined =>
        Alias(child, optGenAliasFunc.get.apply(e))()
      case l: Literal => Alias(l, toPrettySQL(l))()
      case e =>
        val metaForAutoGeneratedAlias = new MetadataBuilder()
          .putString(AUTO_GENERATED_ALIAS, "true")
          .build()
        Alias(e, toPrettySQL(e))(explicitMetadata = Some(metaForAutoGeneratedAlias))
    }
  }

  private def extractOnly(e: Expression): Boolean = e match {
    case _: ExtractValue => e.children.forall(extractOnly)
    case _: Literal => true
    case _: Attribute => true
    case _ => false
  }
}
