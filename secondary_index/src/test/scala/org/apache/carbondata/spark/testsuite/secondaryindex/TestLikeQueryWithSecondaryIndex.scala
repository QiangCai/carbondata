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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, DataFrame, Row}
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * Test Class for filter expression query on String datatypes
  */
class TestLikeQueryWithSecondaryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists TCarbon")

    sql("CREATE TABLE IF NOT EXISTS TCarbon(ID Int, country String, "+
          "name String, phonetype String, serialname String) "+
        "STORED AS carbondata"
    )
    var csvFilePath = s"$pluginResourcesPath/secondaryIndexLikeTest.csv"

    sql(
      s"LOAD DATA LOCAL INPATH '" + csvFilePath + "' INTO TABLE " +
      s"TCarbon " +
      s"OPTIONS('DELIMITER'= ',')"

    )

    sql("create index insert_index on table TCarbon (name) AS 'carbondata'"
    )
  }

  test("select secondary index like query Contains") {
    val df = sql("select * from TCarbon where name like '%aaa1%'")
    secondaryIndexTableCheck(df,_.equalsIgnoreCase("TCarbon"))

    checkAnswer(
      sql("select * from TCarbon where name like '%aaa1%'"),
      Seq(Row(1, "china", "aaa1", "phone197", "A234"),
        Row(9, "china", "aaa1", "phone756", "A455"))
    )
  }

    test("select secondary index like query ends with") {
      val df = sql("select * from TCarbon where name like '%aaa1'")
      secondaryIndexTableCheck(df,_.equalsIgnoreCase("TCarbon"))

      checkAnswer(
        sql("select * from TCarbon where name like '%aaa1'"),
        Seq(Row(1, "china", "aaa1", "phone197", "A234"),
          Row(9, "china", "aaa1", "phone756", "A455"))
      )
    }

      test("select secondary index like query starts with") {
        val df = sql("select * from TCarbon where name like 'aaa1%'")
        secondaryIndexTableCheck(df, Set("insert_index","TCarbon").contains(_))

        checkAnswer(
          sql("select * from TCarbon where name like 'aaa1%'"),
          Seq(Row(1, "china", "aaa1", "phone197", "A234"),
            Row(9, "china", "aaa1", "phone756", "A455"))
        )
      }

  def secondaryIndexTableCheck(dataFrame:DataFrame,
      tableNameMatchCondition :(String) => Boolean): Unit ={
    dataFrame.queryExecution.sparkPlan.collect {
      case bcf: CarbonDatasourceHadoopRelation =>
        if(!tableNameMatchCondition(bcf.carbonTable.getTableUniqueName)){
          assert(true)
        }
    }
  }

  override def afterAll {
    sql("DROP INDEX if exists insert_index ON TCarbon")
    sql("drop table if exists TCarbon")
  }
}