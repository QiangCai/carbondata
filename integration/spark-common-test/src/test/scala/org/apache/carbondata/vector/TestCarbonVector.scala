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
        Record(1.asInstanceOf[Short], 2, 3L, 4.1f, 5.2d, BigDecimal.decimal(6.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "a", "ab", "abc", true, Array(1.asInstanceOf[Byte], 2.asInstanceOf[Byte]), Array("a", "b", "c"), SubRecord("c1", "c2", "c3"), Map("k1"-> "v1", "k2"-> "v2")),
        Record(2.asInstanceOf[Short], 3, 4L, 5.1f, 6.2d, BigDecimal.decimal(7.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "b", "bc", "bcd", false, Array(11.asInstanceOf[Byte], 12.asInstanceOf[Byte]), Array("b", "c", "d"), SubRecord("c11", "c22", "c33"), Map("k11"-> "v11", "k22"-> "v22")),
        Record(33.asInstanceOf[Short], 33, 34L, 35.1f, 36.2d, BigDecimal.decimal(37.3), new Timestamp(System.currentTimeMillis()), new Date(System.currentTimeMillis()), "3b", "3bc", "3bcd", false, Array(31.asInstanceOf[Byte], 32.asInstanceOf[Byte]), Array("3b", "33c", "33d"), SubRecord("c113", "c2233", "c3333"), Map("k113"-> "v11", "k23"-> "v22"))
      ))
    val df = sqlContext.createDataFrame(rdd)
    df.createOrReplaceTempView("base_table")
  }

  override protected def afterAll(): Unit = {
    sql(s"use default")
    // sql(s"DROP DATABASE $dbName CASCADE")
  }

  test("Test insert column with primitive data type") {
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
         | stored by 'carbondata'
         | tblproperties('vector'='true')
      """.stripMargin)

    sql(s"insert into $tableName select * from base_table")

    sql(s"insert into $tableName select * from base_table")

    sql(s"show segments for table $tableName").show(100, false)

    sql(s"select * from $tableName").show(100, false)

    sql(s"select smallIntField, stringField from $tableName").show(100, false)

    sql(s"select count(*) from $tableName").show(100, false)

    sql(s"select smallIntField, stringField from $tableName where smallIntField = 1").show(100, false)

    //sql(s"insert column(newcol1 int) into $tableName select smallIntField + 100 from $tableName").show(100, false)

    //sql(s"insert column(newcol2 int) into $tableName select smallIntField + 100 from $tableName where smallIntField > 1").show(100, false)

  }

  test("Test insert column with complex data type") {
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
         | structField struct<col1:string, col2:string, col3:string>,
         | mapField map<string, string>
         | )
         | stored by 'carbondata'
         | tblproperties('vector'='true')
      """.stripMargin)

    sql(s"insert into $tableName select * from base_table")

    sql(s"insert into $tableName select * from base_table")

    sql(s"show segments for table $tableName").show(100, false)

    sql(s"select * from $tableName").show(100, false)

    sql(s"select smallIntField, stringField from $tableName").show(100, false)

    sql(s"select count(*) from $tableName").show(100, false)

    sql(s"select smallIntField, stringField from $tableName where smallIntField = 1").show(100, false)

    // sql(s"insert column(newcol1 int) into $tableName select smallIntField + 100 from $tableName").show(100, false)

    // sql(s"insert column(newcol2 int) into $tableName select smallIntField + 100 from $tableName where smallIntField > 1").show(100, false)

  }

}

case class SubRecord(
    col1: String,
    col2: String,
    col3: String
)

case class Record(
    smallIntField: Short,
    intField: Int,
    bigIntField: Long,
    floatField: Float,
    doubleField: Double,
    decimalField: BigDecimal,
    timestampField: Timestamp,
    dateField: Date,
    stringField: String,
    varcharField: String,
    charField: String,
    booleanFiled: Boolean,
    binaryFiled: Array[Byte],
    arrayField: Array[String],
    structField: SubRecord,
    mapField: Map[String, String]
)
