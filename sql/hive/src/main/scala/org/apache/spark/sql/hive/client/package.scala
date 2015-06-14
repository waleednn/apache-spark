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

package org.apache.spark.sql.hive

/** Support for interacting with different versions of the HiveMetastoreClient */
package object client {
  private[client] abstract class HiveVersion(val fullVersion: String, val hasBuiltinsJar: Boolean)

  // scalastyle:off
  private[client] object hive {
    case object v10 extends HiveVersion("0.10.0", true)
    case object v11 extends HiveVersion("0.11.0", false)
    case object v12 extends HiveVersion("0.12.0", false)
    case object v13 extends HiveVersion("0.13.1", false)
    case object v120 extends HiveVersion("1.2.0", false)
  }
  // scalastyle:on

}