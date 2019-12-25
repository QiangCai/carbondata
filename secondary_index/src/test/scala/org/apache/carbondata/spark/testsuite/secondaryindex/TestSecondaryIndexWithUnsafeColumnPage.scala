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
 * Created by K00900841 on 2017/8/22.
 */
class TestSecondaryIndexWithUnsafeColumnPage extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("drop table if exists testSecondryIndex")
    sql("create table testSecondryIndex( a string,b string,c string) STORED AS carbondata")
    sql("insert into testSecondryIndex select 'babu','a','6'")
    sql("create index testSecondryIndex_IndexTable on table testSecondryIndex(b) AS 'carbondata'")
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from testSecondryIndex_IndexTable")
    ,Seq(Row(1)))
  }

  override def afterAll {
    sql("drop table if exists testIndexTable")
  }

}
