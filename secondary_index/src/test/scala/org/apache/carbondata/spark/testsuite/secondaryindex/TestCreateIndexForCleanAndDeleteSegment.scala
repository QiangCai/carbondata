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

/**
 * test cases for testing clean and delete segment functionality for index tables
 */
class TestCreateIndexForCleanAndDeleteSegment extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists carbon")
    sql("drop table if exists delete_segment_by_id")
    sql("drop table if exists clean_files_test")
  }

  test("test secondary index for delete segment by id") {
    sql("drop index if exists index_no_dictionary on delete_segment_by_id")
    sql("drop table if exists delete_segment_by_id")

    sql("CREATE table delete_segment_by_id (empno int, empname String, " +
        "designation String, doj Timestamp, workgroupcategory int, " +
        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
        "utilization int,salary int) STORED AS carbondata " +
        "TBLPROPERTIES('DICTIONARY_INCLUDE'='empno,workgroupcategory,deptno,projectcode'," +
        "'DICTIONARY_EXCLUDE'='empname')")

    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/data.csv' INTO " +
    "TABLE delete_segment_by_id OPTIONS('DELIMITER'=',', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")


    sql("create index index_no_dictionary on table delete_segment_by_id (empname) AS 'carbondata'")

    sql("delete from table delete_segment_by_id where segment.id IN(0)")

    checkAnswer(sql("select count(*) from delete_segment_by_id"),
      sql("select count(*) from index_no_dictionary"))

    sql("drop table if exists delete_segment_by_id")
  }

//  test("test secondary index for clean files") {
//    sql("drop table if exists clean_files_test")
//
//    sql("CREATE table clean_files_test (empno int, empname String, " +
//        "designation String, doj Timestamp, workgroupcategory int, " +
//        "workgroupcategoryname String, deptno int, deptname String, projectcode int, " +
//        "projectjoindate Timestamp, projectenddate Timestamp, attendance int, " +
//        "utilization int,salary int) STORED AS carbondata " +
//        "TBLPROPERTIES('DICTIONARY_INCLUDE'='empno,workgroupcategory,deptno,projectcode'," +
//        "'DICTIONARY_EXCLUDE'='empname')")
//
//    sql("LOAD DATA LOCAL INPATH './src/test/resources/data.csv' INTO " +
//        "TABLE clean_files_test OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'BAD_RECORDS_LOGGER_ENABLE'='FALSE', 'BAD_RECORDS_ACTION'='FORCE')")
//
//    sql("drop index if exists index_no_dictionary on clean_files_test")
//
//    sql("create index index_no_dictionary on table clean_files_test (empname) AS 'carbondata'")
//
//    sql("delete from table clean_files_test where segment.id IN(0)")
//
//    sql("clean files for table clean_files_test")
//
//    val indexTable = CarbonMetadata.getInstance().getCarbonTable("default_index_no_dictionary")
//    val carbonTablePath: CarbonTablePath = CarbonStorePath.getCarbonTablePath(indexTable.getStorePath, indexTable.getCarbonTableIdentifier)
//    val dataDirectoryPath: String = carbonTablePath.getCarbonDataDirectoryPath("0", "0")
//    if (CarbonUtil.isFileExists(dataDirectoryPath)) {
//      assert(false)
//    }
//    assert(true)
//
//    sql("drop table if exists clean_files_test")
//  }

  override def afterAll: Unit = {
    sql("drop table if exists carbon")
//    sql("drop table if exists delete_segment_by_id")
    sql("drop table if exists clean_files_test")
  }

}
