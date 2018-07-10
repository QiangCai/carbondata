package org.apache.carbondata.spark.testsuite.secondaryindex

import scala.collection.JavaConverters._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.spark.sql.{CarbonEnv, Row}
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Created by K00900841 on 2017/9/1.
 */
class TestSIWithSecondryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop index if exists si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("create table table_WithSIAndAlter(c1 string, c2 date,c3 timestamp) stored by 'carbondata'")
    sql("insert into table_WithSIAndAlter select 'xx',current_date, current_timestamp")
    sql("alter table table_WithSIAndAlter add columns(date1 date, time timestamp)")
    sql("update table_WithSIAndAlter set(date1) = (c2)").show
    sql("update table_WithSIAndAlter set(time) = (c3)").show
    sql("create index si_altercolumn on table table_WithSIAndAlter(date1,time) as 'carbondata'")
  }

  private def isExpectedValueValid(dbName: String,
      tableName: String,
      key: String,
      expectedValue: String): Boolean = {
    val carbonTable = CarbonEnv.getCarbonTable(Option(dbName), tableName)(sqlContext.sparkSession)
    if (key.equalsIgnoreCase(CarbonCommonConstants.COLUMN_META_CACHE)) {
      val value = carbonTable.getMinMaxCachedColumnsInCreateOrder.asScala.mkString(",")
      expectedValue.equals(value)
    } else {
      val value = carbonTable.getTableInfo.getFactTable.getTableProperties.get(key)
      expectedValue.equals(value)
    }
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from si_altercolumn")
      ,Seq(Row(1)))
  }

  test("test create secondary index when all records are deleted from table") {
    sql("drop table if exists delete_records")
    sql("create table delete_records (a string,b string) stored by 'carbondata'")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("delete from delete_records where a='k'").show()
    sql("alter table delete_records compact 'minor'")
    sql("create index index1 on table delete_records(b) as 'carbondata'")
    checkAnswer(sql("select count(*) from index1"), Row(0))
    sql("drop table if exists delete_records")
  }

  test("test secondary index data after parent table rename") {
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
    sql("create table maintable (a string,b string, c int) stored by 'carbondata'")
    sql("insert into maintable values('k','x',2)")
    sql("insert into maintable values('k','r',1)")
    sql("create index index21 on table maintable(b) as 'carbondata'")
    checkAnswer(sql("select * from maintable where c>1"), Seq(Row("k","x",2)))
    sql("ALTER TABLE maintable RENAME TO maintableeee")
    checkAnswer(sql("select * from maintableeee where c>1"), Seq(Row("k","x",2)))
  }

  test("validate column_meta_cache and cache_level on SI table") {
    sql("drop table if exists column_meta_cache")
    sql("create table column_meta_cache(c1 String, c2 String, c3 int, c4 double) stored by 'carbondata'")
    sql("create index indexCache on table column_meta_cache(c2,c1) as 'carbondata' TBLPROPERTIES('COLUMN_meta_CachE'='c2','cache_level'='BLOCK')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", "c2"))
    assert(isExpectedValueValid("default", "indexCache", "cache_level", "BLOCK"))
    // set invalid values for SI table for column_meta_cache and cache_level and verify
    intercept[MalformedCarbonCommandException] {
      sql("create index indexCache1 on table column_meta_cache(c2) as 'carbondata' TBLPROPERTIES('COLUMN_meta_CachE'='abc')")
    }
    intercept[MalformedCarbonCommandException] {
      sql("create index indexCache1 on table column_meta_cache(c2) as 'carbondata' TBLPROPERTIES('cache_level'='abc')")
    }
    intercept[Exception] {
      sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='abc')")
    }
    intercept[Exception] {
      sql("Alter table indexCache SET TBLPROPERTIES('CACHE_LEVEL'='abc')")
    }
    // alter table to unset these properties on SI table
    sql("Alter table indexCache UNSET TBLPROPERTIES('column_meta_cache')")
    var descResult = sql("describe formatted indexCache")
    checkExistence(descResult, false, "COLUMN_META_CACHE")
    sql("Alter table indexCache UNSET TBLPROPERTIES('cache_level')")
    descResult = sql("describe formatted indexCache")
    checkExistence(descResult, true, "CACHE_LEVEL")
    //alter SI table to set the properties again
    sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='c1')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", "c1"))
    // set empty value for column_meta_cache
    sql("Alter table indexCache SET TBLPROPERTIES('column_meta_cache'='')")
    assert(isExpectedValueValid("default", "indexCache", "column_meta_cache", ""))
    // set cache_level to blocklet
    sql("Alter table indexCache SET TBLPROPERTIES('cache_level'='BLOCKLET')")
    assert(isExpectedValueValid("default", "indexCache", "cache_level", "BLOCKLET"))
  }

  override def afterAll {
    sql("drop index si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
    sql("drop table if exists column_meta_cache")
  }

}
