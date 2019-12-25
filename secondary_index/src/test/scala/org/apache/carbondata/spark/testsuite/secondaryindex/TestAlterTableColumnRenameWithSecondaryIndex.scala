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

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.exception.ProcessMetaDataException
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.BroadCastSIFilterPushJoin
import org.scalatest.BeforeAndAfterAll

class TestAlterTableColumnRenameWithSecondaryIndex extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    dropTable()
  }

  test("test direct rename on SI table") {
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    val ex = intercept[ProcessMetaDataException] {
      sql("alter table index1 change c test string")
    }
    assert(ex.getMessage.contains("Alter table column rename is not allowed on index table"))
  }

  test("test column rename with SI table") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    sql("alter table si_rename change c test string")
    val carbonTable = CarbonMetadata.getInstance().getCarbonTable("default", "index1")
    assert(null != carbonTable.getColumnByName("index1", "test"))
    assert(null == carbonTable.getColumnByName("index1", "c"))
  }

  test("test column rename with multiple SI table table") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata' ")
    sql("create index index2 on table si_rename(c,d) AS 'carbondata' ")
    sql("alter table si_rename change c test string")
    sql("alter table si_rename change d testSI string")
    val carbonTable1 = CarbonMetadata.getInstance().getCarbonTable("default", "index1")
    assert(null != carbonTable1.getColumnByName("index1", "test"))
    assert(null == carbonTable1.getColumnByName("index1", "c"))
    val carbonTable2 = CarbonMetadata.getInstance().getCarbonTable("default", "index2")
    assert(null != carbonTable2.getColumnByName("index2", "testSI"))
    assert(null == carbonTable2.getColumnByName("index2", "d"))
  }

  test("test column rename with SI tables load and query") {
    dropTable()
    createTable()
    sql("create index index1 on table si_rename(c) AS 'carbondata'")
    sql("create index index2 on table si_rename(c,d) AS 'carbondata'")
    sql("insert into si_rename select 'abc',3,'def','mno'")
    sql("insert into si_rename select 'def',4,'xyz','pqr'")
    val query1 = sql("select c,d from si_rename where d = 'pqr' or c = 'def'").count()
    sql("alter table si_rename change c test string")
    sql("alter table si_rename change d testSI string")
    sql("show indexes on si_rename").collect
    val query2 = sql("select test,testsi from si_rename where testsi = 'pqr' or test = 'def'").count()
    assert(query1 == query2)
    val df = sql("select test,testsi from si_rename where testsi = 'pqr' or test = 'def'").queryExecution.sparkPlan
    if (!isFilterPushedDownToSI(df)) {
      assert(false)
    } else {
      assert(true)
    }
  }

  override protected def afterAll(): Unit = {
    dropTable()
  }

  private def dropTable(): Unit = {
    sql("drop table if exists si_rename")
  }

  private def createTable(): Unit = {
    sql("create table si_rename (a string,b int, c string, d string) STORED AS carbondata")
  }

  /**
    * Method to check whether the filter is push down to SI table or not
    *
    * @param sparkPlan
    * @return
    */
  private def isFilterPushedDownToSI(sparkPlan: SparkPlan): Boolean = {
    var isValidPlan = false
    sparkPlan.transform {
      case broadCastSIFilterPushDown: BroadCastSIFilterPushJoin =>
        isValidPlan = true
        broadCastSIFilterPushDown
    }
    isValidPlan
  }
}
