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
  private[hive] sealed abstract class HiveVersion(
      val fullVersion: String,
      val extraDeps: Seq[String] = Nil,
      val exclusions: Seq[String] = Nil)

  // scalastyle:off
  private[hive] object hive {
    case object v12 extends HiveVersion("0.12.0")

    // Do not need Calcite because we disabled hive.cbo.enable.
    // The commons-httpclient:commons-httpclient:jar:3.0.1 dependency are nowhere to be found.
    // So exclude it and add extra dependency commons-httpclient:commons-httpclient:jar:3.1
    //
    // The other excluded dependencies are nowhere to be found, so exclude them explicitly. If
    // they're needed by the metastore client, users will have to dig them out of somewhere and use
    // configuration to point Spark at the correct jars.
    case object v13 extends HiveVersion("0.13.1",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("commons-httpclient:commons-httpclient"))

    case object v14 extends HiveVersion("0.14.0",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v1_0 extends HiveVersion("1.0.1",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    // The curator dependency was added to the exclusions here because it seems to confuse the ivy
    // library. org.apache.curator:curator is a pom dependency but ivy tries to find the jar for it,
    // and fails.
    case object v1_1 extends HiveVersion("1.1.1",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v1_2 extends HiveVersion("1.2.2",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("eigenbase:eigenbase-properties",
        "org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm",
        "net.hydromatic:linq4j",
        "net.hydromatic:quidem"))

    case object v2_0 extends HiveVersion("2.0.1",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_1 extends HiveVersion("2.1.1",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-avatica",
        "org.apache.curator:*",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    case object v2_2 extends HiveVersion("2.2.0",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("org.apache.calcite:calcite-core",
        "org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since HIVE-14496, Hive materialized view need calcite-core.
    // For spark, only VersionsSuite currently creates a hive materialized view for testing.
    case object v2_3 extends HiveVersion("2.3.4",
      extraDeps = Seq("commons-httpclient:commons-httpclient:3.0.1"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "commons-httpclient:commons-httpclient",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    // Since Hive 3.0, HookUtils uses org.apache.logging.log4j.util.Strings
    // Since HIVE-14496, Hive.java uses calcite-core
    // Since HIVE-19228, Removed commons-httpclient 3.x usage
    case object v3_1 extends HiveVersion("3.1.1",
      extraDeps = Seq("org.apache.logging.log4j:log4j-api:2.10.0",
        "org.apache.derby:derby:10.14.1.0"),
      exclusions = Seq("org.apache.calcite:calcite-druid",
        "org.apache.calcite.avatica:avatica",
        "org.apache.curator:*",
        "org.pentaho:pentaho-aggdesigner-algorithm"))

    val allSupportedHiveVersions = Set(v12, v13, v14, v1_0, v1_1, v1_2, v2_0, v2_1, v2_2, v2_3, v3_1)
  }
  // scalastyle:on

}
