package org.apache.carbondata.spark.testsuite.standardpartition

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class StandardPartitionAlterTableTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    dropTable()
  }

  override def afterAll(): Unit = {
    // dropTable()
  }

  def dropTable(): Unit = {
    sql("drop table if exists sp_hive_t1")
    sql("drop table if exists sp_hive_t2")
    sql("drop table if exists sp_ds_t1")
    sql("drop table if exists sp_ds_t2")
  }

  test("alter hive partitioned table: partition column without comment") {
    sql("""create table sp_hive_t1(
        | col1 string,
        | col2 int)
        | stored as carbondata
        | partitioned by (col3 string)""".stripMargin)
    sql("insert into table sp_hive_t1 values('col1_1', 2, 'col3_1')")
    sql("alter table sp_hive_t1 add columns(col4 string)")
    sql("insert into table sp_hive_t1 values('col1_2',2,'col4_2','col3_2')")
    sql("describe table sp_hive_t1").show(100, false)
    sql("select * from sp_hive_t1").show(100, false)
  }

  test("alter hive partitioned table: partition column with comment") {
    sql("""create table sp_hive_t2(
          | col1 string comment 'this is col1',
          | col2 int comment 'this is col2')
          | stored as carbondata
          | partitioned by (col3 string comment 'this is col3')""".stripMargin)
    sql("insert into table sp_hive_t2 values('col1_1', 2, 'col3_1')")
    sql("alter table sp_hive_t2 add columns(col4 string)")
    sql("insert into table sp_hive_t2 values('col1_2',2,'col4_2','col3_2')")
    sql("describe table sp_hive_t2").show(100, false)
    sql("select * from sp_hive_t2").show(100, false)
  }

  test("alter datasource partitioned table: partition column without comment") {
    sql("""create table sp_ds_t1(
          | col1 string,
          | col2 int,
          | col3 string)
          | using carbondata
          | partitioned by (col3)""".stripMargin)
    sql("insert into table sp_ds_t1 values('col1_1', 2, 'col3_1')")
    sql("alter table sp_ds_t1 add columns(col4 string)")
    sql("insert into table sp_ds_t1 values('col1_2',2,'col4_2','col3_2')")
    sql("describe table sp_ds_t1").show(100, false)
    sql("select * from sp_ds_t1").show(100, false)
  }

  test("alter datasource partitioned table: partition column with comment") {
    sql("""create table sp_ds_t2(
          | col1 string comment 'this is col1',
          | col2 int comment 'this is col2',
          | col3 string comment 'this is col3')
          | using carbondata
          | partitioned by (col3)""".stripMargin)
    sql("insert into table sp_ds_t2 values('col1_1', 2, 'col3_1')")
    sql("alter table sp_ds_t2 add columns(col4 string)")
    sql("insert into table sp_ds_t2 values('col1_2',2,'col4_2','col3_2')")
    sql("describe table sp_ds_t2").show(100, false)
    sql("select * from sp_ds_t2").show(100, false)
  }

}
