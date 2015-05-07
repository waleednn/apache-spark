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

package org.apache.spark.ml.util

import java.util.UUID


/**
 * Object with a unique ID that identifies itself and its derivatives.
 */
private[ml] trait Identifiable extends Serializable {

  /**
   * A unique ID for the object and its derivatives. The default implementation concatenates
   * [[simpleClassName]], "_", and 8 random hex chars.
   */
  final def uid: String = _uid

  /**
   * A simple name of the class, which is used as the first part of the generated UID. The default
   * implementation uses [[java.lang.Class#getSimpleName()]].
   */
  protected def simpleClassName: String = this.getClass.getSimpleName

  /**
   * Sets the UID of the instance.
   */
  protected final def setUID(uid: String): this.type = {
    this._uid = uid
    this
  }

  private var _uid = simpleClassName + "_" + UUID.randomUUID().toString.take(8)
}
