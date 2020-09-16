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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.{TestHive, TestHiveSingleton}
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

class HiveTableScanSuite extends HiveComparisonTest with SQLTestUtils with TestHiveSingleton {

  createQueryTest("partition_based_table_scan_with_different_serde",
    """
      |CREATE TABLE part_scan_test (key STRING, value STRING) PARTITIONED BY (ds STRING)
      |ROW FORMAT SERDE
      |'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe'
      |STORED AS RCFILE;
      |
      |FROM src
      |INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-01')
      |SELECT 100,100 LIMIT 1;
      |
      |ALTER TABLE part_scan_test SET SERDE
      |'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
      |
      |FROM src INSERT INTO TABLE part_scan_test PARTITION (ds='2010-01-02')
      |SELECT 200,200 LIMIT 1;
      |
      |SELECT * from part_scan_test;
    """.stripMargin)

  // In unit test, kv1.txt is a small file and will be loaded as table src
  // Since the small file will be considered as a single split, we assume
  // Hive / SparkSQL HQL has the same output even for SORT BY
  createQueryTest("file_split_for_small_table",
    """
      |SELECT key, value FROM src SORT BY key, value
    """.stripMargin)

  test("Spark-4041: lowercase issue") {
    TestHive.sql("CREATE TABLE tb (KEY INT, VALUE STRING) STORED AS ORC")
    TestHive.sql("insert into table tb select key, value from src")
    TestHive.sql("select KEY from tb where VALUE='just_for_test' limit 5").collect()
    TestHive.sql("drop table tb")
  }

  test("Spark-4077: timestamp query for null value") {
    TestHive.sql("DROP TABLE IF EXISTS timestamp_query_null")
    TestHive.sql(
      """
        CREATE TABLE timestamp_query_null (time TIMESTAMP,id INT)
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
      """.stripMargin)
    val location =
      Utils.getSparkClassLoader.getResource("data/files/issue-4077-data.txt").getFile()

    TestHive.sql(s"LOAD DATA LOCAL INPATH '$location' INTO TABLE timestamp_query_null")
    assert(TestHive.sql("SELECT time from timestamp_query_null limit 2").collect()
      === Array(Row(java.sql.Timestamp.valueOf("2014-12-11 00:00:00")), Row(null)))
    TestHive.sql("DROP TABLE timestamp_query_null")
  }

  test("Spark-4959 Attributes are case sensitive when using a select query from a projection") {
    withTable("spark_4959") {
      sql("create table spark_4959 (col1 string)")
      sql("""insert into table spark_4959 select "hi" from src limit 1""")
      table("spark_4959").select(
        $"col1".as("CaseSensitiveColName"),
        $"col1".as("CaseSensitiveColName2")).createOrReplaceTempView("spark_4959_2")

      assert(sql("select CaseSensitiveColName from spark_4959_2").head() === Row("hi"))
      assert(sql("select casesensitivecolname from spark_4959_2").head() === Row("hi"))
    }
  }

  private def checkNumScannedPartitions(stmt: String, expectedNumParts: Int): Unit = {
    val plan = sql(stmt).queryExecution.sparkPlan
    val numPartitions = plan.collectFirst {
      case p: HiveTableScanExec => p.rawPartitions.length
    }.getOrElse(0)
    assert(numPartitions == expectedNumParts)
  }

  test("Verify SQLConf HIVE_METASTORE_PARTITION_PRUNING") {
    val view = "src"
    withTempView(view) {
      spark.range(1, 5).createOrReplaceTempView(view)
      val table = "table_with_partition"
      withTable(table) {
        sql(
          s"""
             |CREATE TABLE $table(id string)
             |PARTITIONED BY (p1 string,p2 string,p3 string,p4 string,p5 string)
           """.stripMargin)
        sql(
          s"""
             |FROM $view v
             |INSERT INTO TABLE $table
             |PARTITION (p1='a',p2='b',p3='c',p4='d',p5='e')
             |SELECT v.id
             |INSERT INTO TABLE $table
             |PARTITION (p1='a',p2='c',p3='c',p4='d',p5='e')
             |SELECT v.id
           """.stripMargin)

        Seq("true", "false").foreach { hivePruning =>
          withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING.key -> hivePruning) {
            // If the pruning predicate is used, getHiveQlPartitions should only return the
            // qualified partition; Otherwise, it return all the partitions.
            val expectedNumPartitions = if (hivePruning == "true") 1 else 2
            checkNumScannedPartitions(
              stmt = s"SELECT id, p2 FROM $table WHERE p2 <= 'b'", expectedNumPartitions)
          }
        }

        Seq("true", "false").foreach { hivePruning =>
          withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING.key -> hivePruning) {
            // If the pruning predicate does not exist, getHiveQlPartitions should always
            // return all the partitions.
            checkNumScannedPartitions(
              stmt = s"SELECT id, p2 FROM $table WHERE id <= 3", expectedNumParts = 2)
          }
        }
      }
    }
  }

  test("SPARK-16926: number of table and partition columns match for new partitioned table") {
    val view = "src"
    withTempView(view) {
      spark.range(1, 5).createOrReplaceTempView(view)
      val table = "table_with_partition"
      withTable(table) {
        sql(
          s"""
             |CREATE TABLE $table(id string)
             |PARTITIONED BY (p1 string,p2 string,p3 string,p4 string,p5 string)
           """.stripMargin)
        sql(
          s"""
             |FROM $view v
             |INSERT INTO TABLE $table
             |PARTITION (p1='a',p2='b',p3='c',p4='d',p5='e')
             |SELECT v.id
             |INSERT INTO TABLE $table
             |PARTITION (p1='a',p2='c',p3='c',p4='d',p5='e')
             |SELECT v.id
           """.stripMargin)
        val scan = getHiveTableScanExec(s"SELECT * FROM $table")
        val numDataCols = scan.relation.dataCols.length
        scan.rawPartitions.foreach(p => assert(p.getCols.size == numDataCols))
      }
    }
  }

  test("HiveTableScanExec canonicalization for different orders of partition filters") {
    val table = "hive_tbl_part"
    withTable(table) {
      sql(
        s"""
           |CREATE TABLE $table (id int)
           |PARTITIONED BY (a int, b int)
         """.stripMargin)
      val scan1 = getHiveTableScanExec(s"SELECT * FROM $table WHERE a = 1 AND b = 2")
      val scan2 = getHiveTableScanExec(s"SELECT * FROM $table WHERE b = 2 AND a = 1")
      assert(scan1.sameResult(scan2))
    }
  }

  test("SPARK-32867: When explain, HiveTableRelation show limited message") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "false",
      "hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable("df") {
        spark.range(1000)
          .select(col("id"), col("id").as("k"))
          .write
          .partitionBy("k")
          .format("hive")
          .mode("overwrite")
          .saveAsTable("df")

        val scan1 = getHiveTableScanExec("SELECT * FROM df WHERE df.k < 3")
        assert(scan1.simpleString(100).replaceAll("#\\d+L", "") ==
          "Scan hive default.df [id, k]," +
            " HiveTableRelation [" +
            "`default`.`df`," +
            " org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe," +
            " Data Cols: [id]," +
            " Partition Cols: [k]," +
            " Pruned Partitions: [k=0, k=1, k=2]," +
            " Statistic: sizeInBytes=8.0 EiB" +
            "]," +
            " [isnotnull(k), (k < 3)]")
        val scan2 = getHiveTableScanExec("SELECT * FROM df WHERE df.k < 100")
        assert(scan2.simpleString(100).replaceAll("#\\d+L", "") ==
          "Scan hive default.df [id, k]," +
            " HiveTableRelation [" +
            "`default`.`df`," +
            " org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe," +
            " Data Cols: [id]," +
            " Partition Cols: [k]," +
            " Pruned Partitions: [k=0, k=1, k=10, k=11, k=12, k=13, k=14, k=15, k=16," +
            " k=17, k=18, k=19, k=2, k=20, k=21, k=22, k=2...," +
            " Statistic: sizeInBytes=8.0 EiB" +
            "]," +
            " [isnotnull(k), (k < 100)]")
      }
    }
  }

  private def getHiveTableScanExec(query: String): HiveTableScanExec = {
    sql(query).queryExecution.sparkPlan.collectFirst {
      case p: HiveTableScanExec => p
    }.get
  }
}
