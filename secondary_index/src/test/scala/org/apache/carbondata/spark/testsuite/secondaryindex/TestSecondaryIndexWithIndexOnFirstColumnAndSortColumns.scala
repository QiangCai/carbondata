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
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Created by sWX420738 on 05/9/2017.
 */
class TestSecondaryIndexWithIndexOnFirstColumnAndSortColumns extends QueryTest with BeforeAndAfterAll {

  var count1BeforeIndex : Array[Row] = null

  override def beforeAll {

    sql("drop table if exists seccust")
    sql("create table seccust (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment string, c_comment string) " +
        "STORED AS carbondata TBLPROPERTIES ('table_blocksize'='128','SORT_COLUMNS'='c_custkey,c_name','NO_INVERTED_INDEX'='c_nationkey')")
    sql(s"""load data  inpath '${pluginResourcesPath}/secindex/firstunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    sql(s"""load data  inpath '${pluginResourcesPath}/secindex/secondunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    count1BeforeIndex = sql("select * from seccust where id = '1' limit 1").collect()
    sql("create index sc_indx1 on table seccust(id) AS 'carbondata'")
  }

  test("Test secondry index on 1st column and with sort columns") {
    checkAnswer(sql("select count(*) from seccust where id = '1'"),Row(2))
  }

  override def afterAll {
    sql("drop table if exists orders")
  }
}