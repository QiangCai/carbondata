/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, Ignore}

import org.apache.carbondata.core.util.CarbonProperties


class DropTableTest extends QueryTest with BeforeAndAfterAll {

  test("test to drop parent table with all indexes") {
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("show tables in cd").show()
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) AS 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) AS 'carbondata'")
    sql("show tables in cd").show()
    sql("drop table cd.t1")
    assert(sql("show tables in cd").collect()
      .forall(row => row.getString(1) != "i2" && row != Row("cd", "i1", "false") && row != Row("cd", "t1", "false")))
  }


  /*test("test to drop one index table out of two"){
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("show tables in cd").show()
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) as 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) as 'carbondata'")
    sql("show tables in cd").show()
    sql("drop index i1 on cd.t1")
    sql("show tables in cd").show()
    sql("select * from i2").show()
  }*/

  test("test to drop index tables") {
    sql("drop database if exists cd cascade")
    sql("create database cd")
    sql("create table cd.t1 (a string, b string, c string) STORED AS carbondata")
    sql("create index i1 on table cd.t1(c) AS 'carbondata'")
    sql("create index i2 on table cd.t1(c,b) AS 'carbondata'")
    sql("show tables in cd").show()
    sql("drop index i1 on cd.t1")
    sql("drop index i2 on cd.t1")
    assert(sql("show tables in cd").collect()
      .forall(row => !row.getString(1).equals("i1") && !row.getString(1).equals("i2") && row.getString(1).equals("t1")))
    assert(sql("show indexes on t1 in cd").collect().isEmpty)
  }

  test("test drop index command") {
    sql("drop table if exists testDrop")
    sql("create table testDrop (a string, b string, c string) STORED AS carbondata")
    val exception = intercept[RuntimeException] {
      sql("drop index indTestDrop on testDrop")
    }
    assert(exception.getMessage().contains("Table or view 'indtestdrop' not found"))
    sql("drop table if exists testDrop")
  }
}
