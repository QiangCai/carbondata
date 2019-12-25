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
package org.apache.spark.carbondata.commands

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
 * Class to test carbon.load.datamaps.parallel. configuration
 */
class LoadDataMapsParallelSETCommandTestCase extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop database if exists set_command cascade")
    sql("create database set_command")
    sql("use set_command")
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table_par")
    sql("DROP TABLE IF EXISTS carbon_table_par1")
    sql("CREATE TABLE IF NOT EXISTS carbon_table (empno String, doj String, salary Int) STORED AS " +
        "carbondata")
    val csvFilePath = s"$resourcesPath/datasample.csv"
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    sql("CREATE TABLE IF NOT EXISTS carbon_table1 (empno String, doj String, salary Int) STORED AS " +
        "carbondata")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table1 OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");

    sql("CREATE TABLE IF NOT EXISTS carbon_table_par (doj String, salary Int) PARTITIONED BY (empno String) STORED AS " +
        "carbondata")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table_par OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    sql("CREATE TABLE IF NOT EXISTS carbon_table_par1 (doj String, salary Int) PARTITIONED BY (empno String) STORED AS " +
        "carbondata")
    sql("LOAD DATA local inpath '" + csvFilePath + "' INTO TABLE carbon_table_par1 OPTIONS"
        + "('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
  }

  ignore("test SET carbon.load.datamaps.parallel. after load") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=true")
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table")
    sql("select * from carbon_table").show(100)
    checkAnswer(sql("select * from carbon_table"), sql("select * from carbon_table1"))
  }

  ignore("test SET carbon.load.datamaps.parallel. after update") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=false")
    sql("update carbon_table set(empno)=('21') where empno='11'").show()
    sql("update carbon_table1 set(empno)=('21') where empno='11'").show()
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table=true")
    checkAnswer(sql("select * from carbon_table"), sql("select * from carbon_table1"))
  }

  ignore("test SET carbon.load.datamaps.parallel. for partition table after load") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=true")
    checkAnswer(sql("select * from carbon_table_par"), sql("select * from carbon_table_par1"))
  }

  ignore("test SET carbon.load.datamaps.parallel. for partition table after update") {
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=false")
    sql("update carbon_table_par set(salary)=('20000') where salary is null").show()
    sql("update carbon_table_par1 set(salary)=('20000') where salary is null").show()
    sql("SET carbon.load.datamaps.parallel.set_command.carbon_table_par=true")
    checkAnswer(sql("select * from carbon_table_par"), sql("select * from carbon_table_par1"))
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS carbon_table")
    sql("DROP TABLE IF EXISTS carbon_table1")
    sql("DROP TABLE IF EXISTS carbon_table_par")
    sql("DROP TABLE IF EXISTS carbon_table_par1")
    sql("drop database if exists set_command cascade")
  }
}
