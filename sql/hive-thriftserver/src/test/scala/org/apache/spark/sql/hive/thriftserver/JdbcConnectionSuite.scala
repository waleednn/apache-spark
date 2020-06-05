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

package org.apache.spark.sql.hive.thriftserver

import java.sql.{Date, DriverManager, SQLException}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.SparkException
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.HiveTestJars
import org.apache.spark.util.ThreadUtils

trait JdbcConnectionSuite extends SharedThriftServer {

  test("SPARK-17819 Support default database in connection URIs") {
    val JDBC_TEST_DATABASE = "jdbc_test_database"
    withDatabase(JDBC_TEST_DATABASE) { s =>
      s.execute(s"CREATE DATABASE $JDBC_TEST_DATABASE")

      val jdbcUri = if (mode == ServerMode.http) {
        s"""jdbc:hive2://localhost:$serverPort/
           |$JDBC_TEST_DATABASE?
           |hive.server2.transport.mode=http;
           |hive.server2.thrift.http.path=cliservice;
           |$hiveConfList#$hiveVarList
         """.stripMargin.split("\n").mkString.trim
      } else {
        s"""jdbc:hive2://localhost:$serverPort/$JDBC_TEST_DATABASE/?$hiveConfList#$hiveVarList"""
      }
      val connection = DriverManager.getConnection(jdbcUri, user, "")
      val statement = connection.createStatement()
      try {
        val resultSet = statement.executeQuery("select current_database()")
        resultSet.next()
        assert(resultSet.getString(1) === JDBC_TEST_DATABASE)
      } finally {
        statement.close()
        connection.close()
      }
    }
  }

  test("Support beeline --hiveconf and --hivevar") {
    withJdbcStatement() { statement =>
      executeTest(hiveConfList)
      executeTest(hiveVarList)
      def executeTest(hiveList: String): Unit = {
        hiveList.split(";").foreach{ m =>
          val kv = m.split("=")
          val k = kv(0)
          val v = kv(1)
          val modValue = s"${v}_MOD_VALUE"
          // select '${a}'; ---> avalue
          val resultSet = statement.executeQuery(s"select '$${$k}'")
          resultSet.next()
          assert(resultSet.getString(1) === v)
          statement.executeQuery(s"set $k=$modValue")
          val modResultSet = statement.executeQuery(s"select '$${$k}'")
          modResultSet.next()
          assert(modResultSet.getString(1) === s"$modValue")
        }
      }
    }
  }

  test("JDBC query execution") {
    withJdbcStatement("test") { statement =>
      val queries = Seq(
        "SET spark.sql.shuffle.partitions=3",
        "CREATE TABLE test(key INT, val STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test",
        "CACHE TABLE test")

      queries.foreach(statement.execute)

      assertResult(5, "Row count mismatch") {
        val resultSet = statement.executeQuery("SELECT COUNT(*) FROM test")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }

  test("Checks Hive version") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET spark.sql.hive.version")
      resultSet.next()
      assert(resultSet.getString(1) === "spark.sql.hive.version")
      assert(resultSet.getString(2) === HiveUtils.builtinHiveVersion)
    }
  }

  test("SPARK-3004 regression: result set containing NULL") {
    withJdbcStatement("test_null") { statement =>
      val queries = Seq(
        "CREATE TABLE test_null(key INT, val STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKvWithNull}' OVERWRITE INTO TABLE test_null")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT * FROM test_null WHERE key IS NULL")

      (0 until 5).foreach { _ =>
        resultSet.next()
        assert(resultSet.getInt(1) === 0)
        assert(resultSet.wasNull())
      }

      assert(!resultSet.next())
    }
  }

  test("SPARK-4292 regression: result set iterator issue") {
    withJdbcStatement("test_4292") { statement =>
      val queries = Seq(
        "CREATE TABLE test_4292(key INT, val STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_4292")

      queries.foreach(statement.execute)

      val resultSet = statement.executeQuery("SELECT key FROM test_4292")

      Seq(238, 86, 311, 27, 165).foreach { key =>
        resultSet.next()
        assert(resultSet.getInt(1) === key)
      }
    }
  }

  test("SPARK-4309 regression: Date type support") {
    withJdbcStatement("test_date") { statement =>
      val queries = Seq(
        "CREATE TABLE test_date(key INT, value STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_date")

      queries.foreach(statement.execute)

      assertResult(Date.valueOf("2011-01-01")) {
        val resultSet = statement.executeQuery(
          "SELECT CAST('2011-01-01' as date) FROM test_date LIMIT 1")
        resultSet.next()
        resultSet.getDate(1)
      }
    }
  }

  test("SPARK-4407 regression: Complex type support") {
    withJdbcStatement("test_map") { statement =>
      val queries = Seq(
        "CREATE TABLE test_map(key INT, value STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)

      assertResult("""{238:"val_238"}""") {
        val resultSet = statement.executeQuery("SELECT MAP(key, value) FROM test_map LIMIT 1")
        resultSet.next()
        resultSet.getString(1)
      }

      assertResult("""["238","val_238"]""") {
        val resultSet = statement.executeQuery(
          "SELECT ARRAY(CAST(key AS STRING), value) FROM test_map LIMIT 1")
        resultSet.next()
        resultSet.getString(1)
      }
    }
  }

  test("SPARK-12143 regression: Binary type support") {
    withJdbcStatement("test_binary") { statement =>
      val queries = Seq(
        "CREATE TABLE test_binary(key INT, value STRING) USING hive",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_binary")

      queries.foreach(statement.execute)

      val expected: Array[Byte] = "val_238".getBytes
      assertResult(expected) {
        val resultSet = statement.executeQuery(
          "SELECT CAST(value as BINARY) FROM test_binary LIMIT 1")
        resultSet.next()
        resultSet.getObject(1)
      }
    }
  }

  test("test multiple session") {
    import org.apache.spark.sql.internal.SQLConf
    var defaultV1: String = null
    var defaultV2: String = null
    var data: ArrayBuffer[Int] = null

    withMultipleConnectionJdbcStatement("test_map", "db1.test_map2")(
      // create table
      { statement =>

        val queries = Seq(
          "CREATE TABLE test_map(key INT, value STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map",
          "CACHE TABLE test_table AS SELECT key FROM test_map ORDER BY key DESC",
          "CREATE DATABASE db1")

        queries.foreach(statement.execute)

        val plan = statement.executeQuery("explain select * from test_table")
        plan.next()
        plan.next()
        assert(plan.getString(1).contains("Scan In-memory table `test_table`"))

        val rs1 = statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        val buf1 = new collection.mutable.ArrayBuffer[Int]()
        while (rs1.next()) {
          buf1 += rs1.getInt(1)
        }
        rs1.close()

        val rs2 = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf2 = new collection.mutable.ArrayBuffer[Int]()
        while (rs2.next()) {
          buf2 += rs2.getInt(1)
        }
        rs2.close()

        assert(buf1 === buf2)

        data = buf1
      },

      // first session, we get the default value of the session status
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        rs1.next()
        defaultV1 = rs1.getString(1)
        assert(defaultV1 != "200")
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()

        defaultV2 = rs2.getString(1)
        assert(defaultV1 != "true")
        rs2.close()
      },

      // second session, we update the session status
      { statement =>

        val queries = Seq(
          s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}=291",
          "SET hive.cli.print.header=true"
        )

        queries.map(statement.execute)
        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        rs1.next()
        assert("spark.sql.shuffle.partitions" === rs1.getString(1))
        assert("291" === rs1.getString(2))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert("hive.cli.print.header" === rs2.getString(1))
        assert("true" === rs2.getString(2))
        rs2.close()
      },

      // third session, we get the latest session status, supposed to be the
      // default value
      { statement =>

        val rs1 = statement.executeQuery(s"SET ${SQLConf.SHUFFLE_PARTITIONS.key}")
        rs1.next()
        assert(defaultV1 === rs1.getString(1))
        rs1.close()

        val rs2 = statement.executeQuery("SET hive.cli.print.header")
        rs2.next()
        assert(defaultV2 === rs2.getString(1))
        rs2.close()
      },

      // try to access the cached data in another session
      { statement =>

        // Cached temporary table can't be accessed by other sessions
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_table ORDER BY KEY DESC")
        }

        val plan = statement.executeQuery("explain select key from test_map ORDER BY key DESC")
        plan.next()
        plan.next()
        assert(plan.getString(1).contains("Scan In-memory table `test_table`"))

        val rs = statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        val buf = new collection.mutable.ArrayBuffer[Int]()
        while (rs.next()) {
          buf += rs.getInt(1)
        }
        rs.close()
        assert(buf === data)
      },

      // switch another database
      { statement =>
        statement.execute("USE db1")

        // there is no test_map table in db1
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map ORDER BY KEY DESC")
        }

        statement.execute("CREATE TABLE test_map2(key INT, value STRING)")
      },

      // access default database
      { statement =>

        // current database should still be `default`
        intercept[SQLException] {
          statement.executeQuery("SELECT key FROM test_map2")
        }

        statement.execute("USE db1")
        // access test_map2
        statement.executeQuery("SELECT key from test_map2")
      }
    )
  }

  // This test often hangs and then times out, leaving the hanging processes.
  // Let's ignore it and improve the test.
  ignore("test jdbc cancel") {
    withJdbcStatement("test_map") { statement =>
      val queries = Seq(
        "CREATE TABLE test_map(key INT, value STRING)",
        s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE test_map")

      queries.foreach(statement.execute)
      implicit val ec = ExecutionContext.fromExecutorService(
        ThreadUtils.newDaemonSingleThreadExecutor("test-jdbc-cancel"))
      try {
        // Start a very-long-running query that will take hours to finish, then cancel it in order
        // to demonstrate that cancellation works.
        val f = Future {
          statement.executeQuery(
            "SELECT COUNT(*) FROM test_map " +
              List.fill(10)("join test_map").mkString(" "))
        }
        // Note that this is slightly race-prone: if the cancel is issued before the statement
        // begins executing then we'll fail with a timeout. As a result, this fixed delay is set
        // slightly more conservatively than may be strictly necessary.
        Thread.sleep(1000)
        statement.cancel()
        val e = intercept[SparkException] {
          ThreadUtils.awaitResult(f, 3.minute)
        }.getCause
        assert(e.isInstanceOf[SQLException])
        assert(e.getMessage.contains("cancelled"))

        // Cancellation is a no-op if spark.sql.hive.thriftServer.async=false
        statement.executeQuery("SET spark.sql.hive.thriftServer.async=false")
        try {
          val sf = Future {
            statement.executeQuery(
              "SELECT COUNT(*) FROM test_map " +
                List.fill(4)("join test_map").mkString(" ")
            )
          }
          // Similarly, this is also slightly race-prone on fast machines where the query above
          // might race and complete before we issue the cancel.
          Thread.sleep(1000)
          statement.cancel()
          val rs1 = ThreadUtils.awaitResult(sf, 3.minute)
          rs1.next()
          assert(rs1.getInt(1) === math.pow(5, 5))
          rs1.close()

          val rs2 = statement.executeQuery("SELECT COUNT(*) FROM test_map")
          rs2.next()
          assert(rs2.getInt(1) === 5)
          rs2.close()
        } finally {
          statement.executeQuery("SET spark.sql.hive.thriftServer.async=true")
        }
      } finally {
        ec.shutdownNow()
      }
    }
  }

  test("test add jar") {
    withMultipleConnectionJdbcStatement("smallKV", "addJar")(
      {
        statement =>
          val jarFile = HiveTestJars.getHiveHcatalogCoreJar().getCanonicalPath

          statement.executeQuery(s"ADD JAR $jarFile")
      },

      {
        statement =>
          val queries = Seq(
            "CREATE TABLE smallKV(key INT, val STRING) USING hive",
            s"LOAD DATA LOCAL INPATH '${TestData.smallKv}' OVERWRITE INTO TABLE smallKV",
            """CREATE TABLE addJar(key string)
              |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            """.stripMargin)

          queries.foreach(statement.execute)

          statement.executeQuery(
            """
              |INSERT INTO TABLE addJar SELECT 'k1' as key FROM smallKV limit 1
            """.stripMargin)

          val actualResult =
            statement.executeQuery("SELECT key FROM addJar")
          val actualResultBuffer = new collection.mutable.ArrayBuffer[String]()
          while (actualResult.next()) {
            actualResultBuffer += actualResult.getString(1)
          }
          actualResult.close()

          val expectedResult =
            statement.executeQuery("SELECT 'k1'")
          val expectedResultBuffer = new collection.mutable.ArrayBuffer[String]()
          while (expectedResult.next()) {
            expectedResultBuffer += expectedResult.getString(1)
          }
          expectedResult.close()

          assert(expectedResultBuffer === actualResultBuffer)
      }
    )
  }

  test("Checks Hive version via SET -v") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET -v")

      val conf = mutable.Map.empty[String, String]
      while (resultSet.next()) {
        conf += resultSet.getString(1) -> resultSet.getString(2)
      }

      if (HiveUtils.isHive23) {
        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("2.3.7"))
      } else {
        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("1.2.1"))
      }
    }
  }

  test("Checks Hive version via SET") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SET")

      val conf = mutable.Map.empty[String, String]
      while (resultSet.next()) {
        conf += resultSet.getString(1) -> resultSet.getString(2)
      }

      if (HiveUtils.isHive23) {
        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("2.3.7"))
      } else {
        assert(conf.get(HiveUtils.FAKE_HIVE_VERSION.key) === Some("1.2.1"))
      }
    }
  }

  test("SPARK-11595 ADD JAR with input path having URL scheme") {
    withJdbcStatement("test_udtf") { statement =>
      try {
        val jarPath = "../hive/src/test/resources/TestUDTF.jar"
        val jarURL = s"file://${System.getProperty("user.dir")}/$jarPath"

        Seq(
          s"ADD JAR $jarURL",
          s"""CREATE TEMPORARY FUNCTION udtf_count2
             |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           """.stripMargin
        ).foreach(statement.execute)

        val rs1 = statement.executeQuery("DESCRIBE FUNCTION udtf_count2")

        assert(rs1.next())
        assert(rs1.getString(1) === "Function: udtf_count2")

        assert(rs1.next())
        assertResult("Class: org.apache.spark.sql.hive.execution.GenericUDTFCount2") {
          rs1.getString(1)
        }

        assert(rs1.next())
        assert(rs1.getString(1) === "Usage: N/A.")

        val dataPath = "../hive/src/test/resources/data/files/kv1.txt"

        Seq(
          "CREATE TABLE test_udtf(key INT, value STRING) USING hive",
          s"LOAD DATA LOCAL INPATH '$dataPath' OVERWRITE INTO TABLE test_udtf"
        ).foreach(statement.execute)

        val rs2 = statement.executeQuery(
          "SELECT key, cc FROM test_udtf LATERAL VIEW udtf_count2(value) dd AS cc")

        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)

        assert(rs2.next())
        assert(rs2.getInt(1) === 97)
        assert(rs2.getInt(2) === 500)
      } finally {
        statement.executeQuery("DROP TEMPORARY FUNCTION udtf_count2")
      }
    }
  }

  test("SPARK-24829 Checks cast as float") {
    withJdbcStatement() { statement =>
      val resultSet = statement.executeQuery("SELECT CAST('4.56' AS FLOAT)")
      resultSet.next()
      assert(resultSet.getString(1) === "4.56")
    }
  }

  test("SPARK-28463: Thriftserver throws BigDecimal incompatible with HiveDecimal") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT CAST(1 AS decimal(38, 18))")
      assert(rs.next())
      assert(rs.getBigDecimal(1) === new java.math.BigDecimal("1.000000000000000000"))
    }
  }

  test("Support interval type") {
    withJdbcStatement() { statement =>
      val rs = statement.executeQuery("SELECT interval 3 months 1 hours")
      assert(rs.next())
      assert(rs.getString(1) === "3 months 1 hours")
    }
    // Invalid interval value
    withJdbcStatement() { statement =>
      val e = intercept[SQLException] {
        statement.executeQuery("SELECT interval 3 months 1 hou")
      }
      assert(e.getMessage.contains("org.apache.spark.sql.catalyst.parser.ParseException"))
    }
  }

  test("SPARK-31859 Thriftserver works with spark.sql.datetime.java8API.enabled=true") {
    withJdbcStatement() { statement =>
      withJdbcStatement() { st =>
        st.execute("set spark.sql.datetime.java8API.enabled=true")
        val rs = st.executeQuery("select date '2020-05-28', timestamp '2020-05-28 00:00:00'")
        rs.next()
        assert(rs.getDate(1).toString() == "2020-05-28")
        assert(rs.getTimestamp(2).toString() == "2020-05-28 00:00:00.0")
      }
    }
  }

  test("SPARK-31861 Thriftserver respects spark.sql.session.timeZone") {
    withJdbcStatement() { statement =>
      withJdbcStatement() { st =>
        st.execute("set spark.sql.session.timeZone=+03:15") // different than Thriftserver's JVM tz
        val rs = st.executeQuery("select timestamp '2020-05-28 10:00:00'")
        rs.next()
        // The timestamp as string is the same as the literal
        assert(rs.getString(1) == "2020-05-28 10:00:00.0")
        // Parsing it to java.sql.Timestamp in the client will always result in a timestamp
        // in client default JVM timezone. The string value of the Timestamp will match the literal,
        // but if the JDBC application cares about the internal timezone and UTC offset of the
        // Timestamp object, it should set spark.sql.session.timeZone to match its client JVM tz.
        assert(rs.getTimestamp(1).toString() == "2020-05-28 10:00:00.0")
      }
    }
  }

  test("SPARK-31863 Session conf should persist between Thriftserver worker threads") {
    val iter = 20
    withJdbcStatement() { statement =>
      // date 'now' is resolved during parsing, and relies on SQLConf.get to
      // obtain the current set timezone. We exploit this to run this test.
      // If the timezones are set correctly to 25 hours apart across threads,
      // the dates should reflect this.

      // iterate a few times for the odd chance the same thread is selected
      for (_ <- 0 until iter) {
        statement.execute("SET spark.sql.session.timeZone=GMT-12")
        val firstResult = statement.executeQuery("SELECT date 'now'")
        firstResult.next()
        val beyondDateLineWest = firstResult.getDate(1)

        statement.execute("SET spark.sql.session.timeZone=GMT+13")
        val secondResult = statement.executeQuery("SELECT date 'now'")
        secondResult.next()
        val dateLineEast = secondResult.getDate(1)
        assert(
          dateLineEast after beyondDateLineWest,
          "SQLConf changes should persist across execution threads")
      }
    }
  }
}
