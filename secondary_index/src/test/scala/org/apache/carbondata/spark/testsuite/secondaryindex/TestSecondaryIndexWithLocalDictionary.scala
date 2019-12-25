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

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestSecondaryIndexWithLocalDictionary extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists local_sec")
  }

  test("test invalid properties in secondary index creation"){
    sql("drop table if exists local_sec")
    sql("create table local_sec (a string,b string) STORED AS carbondata tblproperties('local_dictionary_enable'='true', 'local_dictionary_exclude'='b','local_dictionary_threshold'='20000')")
    val exception = intercept[Exception] {
      sql(
        "create index index1 on table local_sec(b) AS 'carbondata' tblproperties('local_dictionary_enable'='true')")
    }
    exception.getMessage.contains("Unsupported Table property in index creation: local_dictionary_enable")
  }

  test("test local dictionary for index when main table is disable"){
    sql("drop table if exists local_sec")
    sql("create table local_sec (a string,b string) STORED AS carbondata tblproperties('local_dictionary_enable'='false')")
    sql("create index index1 on table local_sec(b) AS 'carbondata'")
    checkExistence(sql("DESC FORMATTED index1"), false,
      "Local Dictionary Include")
  }

  test("test local dictionary for index with default properties when enabled") {
    sql("drop table if exists local_sec")
    sql("create table local_sec (a string,b string) STORED AS carbondata tblproperties('local_dictionary_enable'='true')")
    sql("create index index1 on table local_sec(b) AS 'carbondata'")
    val descLoc = sql("describe formatted index1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("10000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("b,positionReference"))
      case None => assert(false)
    }
  }

  test("test local dictionary for index when index column is dictionary excluded") {
    sql("drop table if exists local_sec")
    sql("create table local_sec (a string,b string) STORED AS carbondata tblproperties('local_dictionary_enable'='true','local_dictionary_exclude'='b','local_dictionary_threshold'='20000')")
    sql("create index index1 on table local_sec(b) AS 'carbondata'")
    val descLoc = sql("describe formatted index1").collect
    descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
      case Some(row) => assert(row.get(1).toString.contains("true"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
      case Some(row) => assert(row.get(1).toString.contains("20000"))
      case None => assert(false)
    }
    descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
      case Some(row) => assert(row.get(1).toString.contains("positionReference"))
      case None => assert(false)
    }
  }

  test("test local dictionary for index when index column is dictionary excluded, but dictionary is disabled") {
    sql("drop table if exists local_sec")
    sql("create table local_sec (a string,b string) STORED AS carbondata tblproperties('local_dictionary_exclude'='b','local_dictionary_enable'='false')")
    sql("create index index1 on table local_sec(b) AS 'carbondata'")
    checkExistence(sql("DESC FORMATTED index1"), false,
      "Local Dictionary Include")
  }

  override def afterAll {
    sql("drop table if exists local_sec")
  }
}
