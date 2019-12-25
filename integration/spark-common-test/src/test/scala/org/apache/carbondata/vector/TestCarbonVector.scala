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

package org.apache.carbondata.vector

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll

class TestCarbonVector extends QueryTest with BeforeAndAfterAll {

  val dbName = "vector_db"

  override protected def beforeAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    sql(s"CREATE DATABASE $dbName")
    sql(s"USE $dbName")
    prepareTable("base_table")
  }

  private def prepareTable(tableName: String): Unit = {
    val rdd = sqlContext
      .sparkSession
      .sparkContext
      .parallelize(Seq(
        Record(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
        Record(1.asInstanceOf[Short], 2, 3L, 4.1f, 5.2d, BigDecimal.decimal(6.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "a", "ab", "abc", true, Array(1.asInstanceOf[Byte], 2.asInstanceOf[Byte]), Array("a", "b", "c"), SubRecord1("c1", 11, null, null), Map("k1"-> 1, "k2"-> 2)),
        Record(2.asInstanceOf[Short], 3, 4L, 5.1f, 6.2d, BigDecimal.decimal(7.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "b", "bc", "bcd", false, Array(11.asInstanceOf[Byte], 12.asInstanceOf[Byte]), null, SubRecord1("c11", 22, Array("c53"), SubRecord2("a")), Map("k11"-> null, "k22"-> 22)),
        Record(33.asInstanceOf[Short], 33, 34L, 35.1f, 36.2d, BigDecimal.decimal(37.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "3b", "3bc", "3bcd", false, Array(31.asInstanceOf[Byte], 32.asInstanceOf[Byte]), Array("3b", "33c", "33d"), SubRecord1("c113", 332, Array("c73", "c83"), SubRecord2("b")), Map("k113"-> 11, "k23"-> 22)),
        Record(null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
      ))
    val df = sqlContext.createDataFrame(rdd)
    df.createOrReplaceTempView(tableName)
  }

  override protected def afterAll(): Unit = {
    sql(s"use default")
    // sql(s"DROP DATABASE $dbName CASCADE")
  }

  test("Test insert column with primitive data type") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "vector_table"
      sql(s"drop table if exists $tableName")
      sql(
        s"""create table $tableName(
           | smallIntField smallInt,
           | intField int,
           | bigIntField bigint,
           | floatField float,
           | doubleField double,
           | decimalField decimal(25, 4),
           | timestampField timestamp,
           | dateField date,
           | stringField string,
           | varcharField varchar(10),
           | charField char(10),
           | booleanField boolean,
           | binaryFiled binary
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from base_table")

      sql(s"insert into $tableName select * from base_table")

      sql(s"show segments for table $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)

      sql(s"select smallIntField, stringField from $tableName").show(100, false)

      sql(s"select count(*) from $tableName").show(100, false)

      sql(s"select smallIntField, stringField from $tableName where smallIntField = 1").show(100, false)

      sql(s"""insert columns(newcol1, newcol2 int, newcol3)
             | into table $tableName
             | select smallIntField + 100,
             | case when smallIntField > 1 then smallIntField + 100 end,
             | stringField
             | from $tableName""".stripMargin
      ).show(100, false)

      sql(s"""insert columns(newcol4, newcol5, newcol6)
             | into table $tableName
             | select smallIntField + 100,
             | stringField,
             | timestampField
             | from $tableName
             | where smallIntField > 1""".stripMargin
      ).show(100, false)

      sql(s"describe formatted $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)
    }
  }

  ignore("Test insert column with complex data type") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "vector_table_complex"
      sql(s"drop table if exists $tableName")
      sql(
        s"""create table $tableName(
           | smallIntField smallInt,
           | intField int,
           | bigIntField bigint,
           | floatField float,
           | doubleField double,
           | decimalField decimal(25, 4),
           | timestampField timestamp,
           | dateField date,
           | stringField string,
           | varcharField varchar(10),
           | charField char(10),
           | booleanField boolean,
           | binaryFiled binary,
           | arrayField array<string>,
           | structField struct<col1:string, col2:int, col3:array<string>>,
           | mapField map<string, int>
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from base_table")

      sql(s"insert into $tableName select * from base_table")

      sql(s"show segments for table $tableName").show(100, false)

      sql(s"select arrayField from $tableName").show(100, false)

      sql(s"select structField.col1 from $tableName").show(100, false)

      sql(s"select mapField from $tableName").show(100, false)

      sql(s"select count(*) from $tableName").show(100, false)

      sql(
        s""" insert columns(
           |   newCol1,
           |   newArrayField,
           |   newCol2,
           |   newStructField struct<col1:string, col2:int, col3:array<string>>,
           |   newCol3,
           |   newMapField)
           | into table $tableName
           | select
           |   bigIntField,
           |   arrayField,
           |   doubleField,
           |   structField,
           |   smallIntField,
           |   mapField
           | from $tableName""".stripMargin
      ).show(100, false)

      sql(s"describe formatted $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)
    }
  }

  ignore("Test insert column with infer data type") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "vector_table_infer"
      sql(s"drop table if exists $tableName")
      sql(
        s"""create table $tableName(
           | smallIntField smallInt,
           | intField int,
           | bigIntField bigint,
           | floatField float,
           | doubleField double,
           | decimalField decimal(25, 4),
           | timestampField timestamp,
           | dateField date,
           | stringField string,
           | varcharField varchar(10),
           | charField char(10),
           | booleanField boolean,
           | binaryFiled binary,
           | arrayField array<string>,
           | structField struct<col1:string, col2:int, col3:array<string>>,
           | mapField map<string, int>
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from base_table")

      sql(s"insert into $tableName select * from base_table")

      sql(s"show segments for table $tableName").show(100, false)

      sql(s"select arrayField from $tableName").show(100, false)

      sql(s"select structField.col1 from $tableName").show(100, false)

      sql(s"select mapField from $tableName").show(100, false)

      sql(s"select count(*) from $tableName").show(100, false)

      sql(s"select smallIntField, structField from $tableName where structfield.col1 = 'c1'").show(100, false)

      sql(s"select smallIntField, structField from $tableName where mapField['k1'] = 1").show(100, false)

      sql(s"insert columns(newArrayField) into table $tableName select arrayField from $tableName").show(100, false)

      sql(s"select newArrayField, arrayField from $tableName").show(100, false)

      sql(s"insert columns(newStructField) into $tableName select structField from $tableName").show(100, false)

      sql(s"select newStructField, structField from $tableName").show(100, false)

      sql(s"insert columns(newMapField) into $tableName select mapField from $tableName").show(100, false)

      sql(s"select newMapField, mapField from $tableName").show(100, false)

      sql(s"insert columns(newcol1 bigint) into table $tableName select smallIntField + 100 from $tableName").show(100, false)

      sql(s"select smallIntField, newcol1 from $tableName").show(100, false)

      sql(s"insert columns(newcol2) into $tableName select case when smallIntField > 1 then smallIntField + 100 end from $tableName").show(100, false)

      sql(s"select smallIntField, newcol2 from $tableName").show(100, false)

      sql(s"insert columns(newcol3 string) into $tableName select smallIntField + 100 from $tableName where smallIntField > 1").show(100, false)

      sql(s"select smallIntField, newcol3 from $tableName").show(100, false)

      sql(s"describe formatted $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)
    }
  }

  test("Test insert column with json schema infer") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "vector_table_json"
      sql(s"drop table if exists $tableName")
      val rdd = sqlContext
        .sparkSession
        .sparkContext
        .parallelize(Seq(
          JsonRecord("{\"name\":\"abc\", \"age\":1}"),
          JsonRecord("{\"name\":\"bcd\", \"age\":2}"),
          JsonRecord("{\"name\":\"ddd\", \"age\":32}")
        ))
      val df = sqlContext.createDataFrame(rdd)
      df.createOrReplaceTempView("json_table")

      sql(
        s"""create table $tableName(
           | jsonContent string
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from json_table")

      sql(s"insert into $tableName select * from json_table")

      sql(s"insert columns(jsonStringField) into $tableName select jsonContent from $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)

      sql(s"insert columns(jsonObjectField json) into $tableName select jsonContent from $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)

      sql(s"describe formatted $tableName").show(100, false)
    }

  }

  test("query with complex") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "query_complex"
      sql(s"drop table if exists $tableName")
      val rdd = sqlContext
        .sparkSession
        .sparkContext
        .parallelize(Seq(
          NewRecord(SubRecord2("aa1"), SubRecord3(Array("bb1", "cc3"), "dd1")),
          NewRecord(SubRecord2("aa2"), SubRecord3(Array("bb2", "cc3"), "dd2")),
          NewRecord(SubRecord2("aa3"), SubRecord3(Array("bb3", "cc3"), "dd3"))
        ))
      val df = sqlContext.createDataFrame(rdd)
      df.createOrReplaceTempView("base_query_complex")

      sql(
        s"""create table $tableName(
           |  col1 struct<col5: string>,
           |  col2 struct<col3: array<string>, col4: string>
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from base_query_complex")

      sql(s"describe formatted $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)

      sql(s"select col1.col5, col2.col4 from $tableName").show(100, false)
    }
  }

  ignore("Test insert multiple columns") {
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      val tableName = "insert_multiple_columns"
      sql(s"drop table if exists $tableName")
      sql(
        s"""create table $tableName(
           | smallIntField smallInt,
           | intField int,
           | bigIntField bigint,
           | floatField float,
           | doubleField double,
           | decimalField decimal(25, 4),
           | timestampField timestamp,
           | dateField date,
           | stringField string,
           | varcharField varchar(10),
           | charField char(10),
           | booleanField boolean,
           | binaryFiled binary,
           | arrayField array<string>,
           | structField struct<col1:string, col2:int, col3:array<string>>,
           | mapField map<string, int>
           | )
           | STORED AS carbondata
           | tblproperties('vector'='true')
      """.stripMargin)

      sql(s"insert into $tableName select * from base_table")

      sql(s"insert into $tableName select * from base_table")

      sql(s"show segments for table $tableName").show(100, false)

      sql(s"select count(*) from $tableName").show(100, false)

      sql(s"insert columns(newcol1, newcol2) into table $tableName select smallIntField + 100, case when smallIntField > 1 then smallIntField + 100 end from $tableName").show(100, false)

      sql(s"describe formatted $tableName").show(100, false)

      sql(s"select * from $tableName").show(100, false)
    }
  }
}

case class SubRecord1(
    col1: String,
    col2: java.lang.Integer,
    col3: Array[String],
    col4: SubRecord2
)

case class SubRecord2(
    col5: String
)

case class SubRecord3(
    col3: Array[String],
    col4: String
)

case class Record(
    smallIntField: java.lang.Short,
    intField: java.lang.Integer,
    bigIntField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    decimalField: BigDecimal,
    timestampField: Timestamp,
    dateField: Date,
    stringField: String,
    varcharField: String,
    charField: String,
    booleanFiled: java.lang.Boolean,
    binaryFiled: Array[Byte],
    arrayField: Array[String],
    structField: SubRecord1,
    mapField: Map[String, java.lang.Integer]
)

case class NewRecord(
    col1: SubRecord2,
    col2: SubRecord3
)

case class JsonRecord(
  jsonContent: String
)
