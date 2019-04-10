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

package org.apache.spark.sql.catalog.v2.expressions;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.types.DataType;

/**
 * Represents a constant literal value in the public expression API.
 * <p>
 * The JVM type of the value held by a literal must be the type used by Spark's InternalRow API for
 * the literal's {@link DataType SQL data type}.
 *
 * @param <T> the JVM type of a value held by the literal
 */
@Experimental
public interface Literal<T> extends Expression {
  /**
   * Returns the literal value.
   */
  T value();

  /**
   * Returns the SQL data type of the literal.
   */
  DataType dataType();
}
