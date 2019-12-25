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

import org.apache.spark.sql.CarbonSession._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll

/**
 * test cases for testing carbonJoin.scala
 */
class TestCarbonJoin extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
  }

  test("test broadcast FilterPushDown with alias") {
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS ptable")
    sql("DROP TABLE IF EXISTS result")
    sql("create table if not exists table1 (ID string) STORED AS carbondata")
    sql("insert into table1 select 'animal'")
    sql("insert into table1 select 'person'")
    sql("create table ptable(pid string) stored as parquet")
    sql("insert into table ptable values('person')")

    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), "table1")(sqlContext.sparkSession)
    val df2 = sql("select id as f91 from table1")
    df2.createOrReplaceTempView("tempTable_2")
    sql("select t1.f91 from tempTable_2 t1, ptable t2 where t1.f91 = t2.pid ").write.saveAsTable("result")
    checkAnswer(sql("select count(*) from result"), Seq(Row(1)))
    checkAnswer(sql("select * from result"), Seq(Row("person")))

    sql("DROP TABLE IF EXISTS result")
    sql("DROP TABLE IF EXISTS table1")
    sql("DROP TABLE IF EXISTS patble")
  }

  test("test broadcast FilterPushDown with alias with SI") {
    sql("drop index if exists cindex on ctable")
    sql("DROP TABLE IF EXISTS ctable")
    sql("DROP TABLE IF EXISTS ptable")
    sql("DROP TABLE IF EXISTS result")
    sql("create table if not exists ctable (type int, id1 string, id string) stored as " +
        "carbondata")
    sql("create index cindex on table ctable (id) AS 'carbondata'")
    sql("insert into ctable select 0, 'animal1', 'animal'")
    sql("insert into ctable select 1, 'person1', 'person'")
    sql("create table ptable(pid string) stored as parquet")
    sql("insert into table ptable values('person')")
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), "ctable")(sqlContext.sparkSession)
    val df2 = sql("select id as f91 from ctable")
    df2.createOrReplaceTempView("tempTable_2")
    sql("select t1.f91 from tempTable_2 t1, ptable t2 where t1.f91 = t2.pid ").write
      .saveAsTable("result")
    checkAnswer(sql("select count(*) from result"), Seq(Row(1)))
    checkAnswer(sql("select * from result"), Seq(Row("person")))
    sql("DROP TABLE IF EXISTS result")
    sql("drop index if exists cindex on ctable")
    sql("DROP TABLE IF EXISTS ctable")
    sql("DROP TABLE IF EXISTS patble")
  }
}
