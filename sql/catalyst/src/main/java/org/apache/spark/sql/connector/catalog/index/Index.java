/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.catalog.index;

import java.util.Collections;
import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.FieldReference;

/**
 * An Index in a table
 *
 * @since 3.3.0
 */
@Evolving
public class Index {
  private String indexName;
  private String indexType;
  private Identifier table;
  private FieldReference[] columns;
  private Map<String, String> properties = Collections.emptyMap();

  public Index(
      String indexName,
      String indexType,
      Identifier table,
      FieldReference[] columns,
      Map<String, String> properties) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.table = table;
    this.columns = columns;
    this.properties = properties;
  }

  /**
   * @return the Index name.
   */
  String indexName() { return indexName; }

  /**
   * @return the indexType of this Index.
   */
  String indexType() { return indexType; }

  /**
   * @return the table this Index is on.
   */
  Identifier table() { return table; }

  /**
   * @return the column(s) this Index is on. Could be multi columns (a multi-column index).
   */
  FieldReference[] columns() { return columns; }

  /**
   * Returns the string map of index properties.
   */
  Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
