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
package org.apache.spark.memory;

import org.apache.spark.annotation.Private;

/**
 * SPARK-24294: To bypass scala bug: https://github.com/scala/bug/issues/9554, we catch
 * {@link OutOfMemoryError} in {@link scala.concurrent.Future}'s body, and re-throw
 * SparkOutOfMemoryException instead.
 */
@Private
public final class SparkOutOfMemoryException extends Exception {

  private OutOfMemoryError oe;

  public SparkOutOfMemoryException(OutOfMemoryError e) {
    oe = e;
  }

  public OutOfMemoryError getOOM(){
    return oe;
  }
}
