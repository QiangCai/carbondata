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

import java.io.{File, IOException}

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * Secondary index refresh and registration to the main table
 */
class TestRegisterIndexCarbonTable extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop database if exists carbon cascade")
  }

  def restoreData(dblocation: String, tableName: String) = {
    val destination = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val source = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
      FileUtils.deleteDirectory(new File(source))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data restore failed.")
    } finally {

    }
  }
  def backUpData(dblocation: String, tableName: String) = {
    val source = dblocation + CarbonCommonConstants.FILE_SEPARATOR + tableName
    val destination = dblocation+ "_back" + CarbonCommonConstants.FILE_SEPARATOR + tableName
    try {
      FileUtils.copyDirectory(new File(source), new File(destination))
    } catch {
      case e : Exception =>
        throw new IOException("carbon table data backup failed.")
    }
  }
  test("register tables test") {
    sql("drop database if exists carbon cascade")
    sql(s"create database carbon location '${location}'")
    sql("use carbon")
    sql("""create table carbon.carbontable (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata""")
    sql("insert into carbontable select 'a',1,'aa','aaa'")
    sql("create index index_on_c3 on table carbontable (c3, c5) AS 'carbondata'")
    backUpData(location, "carbontable")
    backUpData(location, "index_on_c3")
    sql("drop table carbontable")
    restoreData(location, "carbontable")
    restoreData(location, "index_on_c3")
    sql("refresh table carbontable")
    sql("refresh table index_on_c3")
    checkAnswer(sql("select count(*) from carbontable"), Row(1))
    checkAnswer(sql("select c1 from carbontable"), Seq(Row("a")))
    sql("REGISTER INDEX TABLE index_on_c3 ON carbontable")
    assert(sql("show indexes on carbontable in carbon").collect().nonEmpty)
  }
  override def afterAll {
    sql("drop database if exists carbon cascade")
    sql("use default")
  }
}
