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

package org.apache.spark.sql.connector.expressions;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;

/**
 * The general representation of SQL scalar expressions, which contains the upper-cased
 * expression name and all the children expressions.
 * <p>
 * The currently supported expressions:
 * <table border="1">
 *  <tr>
 *   <th>Expression name</th>
 *   <th>SQL scalar expression</th>
 *   <th>Since version</th>
 *  </tr>
 *  <tr>
 *   <td>IS_NULL</td>
 *   <td><pre>expr IS NULL</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>IS_NOT_NULL</td>
 *   <td><pre>expr IS NOT NULL</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>=</td>
 *   <td><pre>expr1 = expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>!=</td>
 *   <td><pre>expr1 != expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td><=></td>
 *   <td><pre>expr1 <=> expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td><</td>
 *   <td><pre>expr1 < expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td><=</td>
 *   <td><pre>expr1 <= expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>></td>
 *   <td><pre>expr1 > expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>>=</td>
 *   <td><pre>expr1 >= expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>+</td>
 *   <td><pre>expr1 + expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>-</td>
 *   <td><pre>expr1 - expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>*</td>
 *   <td><pre>expr1 * expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>/</td>
 *   <td><pre>expr1 / expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>%</td>
 *   <td><pre>expr1 % expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>&&</td>
 *   <td><pre>expr1 && expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>||</td>
 *   <td><pre>expr1 || expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>AND</td>
 *   <td><pre>expr1 AND expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>OR</td>
 *   <td><pre>expr1 OR expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>&</td>
 *   <td><pre>expr1 & expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>|</td>
 *   <td><pre>expr1 | expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>^</td>
 *   <td><pre>expr1 ^ expr2</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>NOT</td>
 *   <td><pre>NOT expr</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 *  <tr>
 *   <td>CASE_WHEN</td>
 *   <td><pre>CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END</pre></td>
 *   <td>3.3.0</td>
 *  </tr>
 * </table>
 *
 * @since 3.3.0
 */
@Evolving
public class GeneralScalarExpression implements Expression, Serializable {
  private String name;
  private Expression[] children;
  private String sql;

  public GeneralScalarExpression(String name, Expression[] children) {
    this.name = name;
    this.children = children;
  }

  public GeneralScalarExpression(Expression[] children) {
    this.name = "UNDEFINED";
    this.children = children;
  }

  public void setSql(String sql) {
        this.sql = sql;
    }

  public String name() { return name; }
  public Expression[] children() { return children; }
  public String sql() { return sql; }

  @Override
  public String toString() {
    if (sql == null) {
      return name + "(" + children.toString() + ")";
    } else {
      return sql;
    }
  }
}
