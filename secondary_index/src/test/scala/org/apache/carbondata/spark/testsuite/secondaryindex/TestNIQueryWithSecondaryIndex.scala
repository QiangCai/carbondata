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
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.spark._
/**
  * Created by sWX420738 on 16-08-2017.
  */
class TestNIQueryWithSecondaryIndex extends QueryTest with BeforeAndAfterAll{

  var count1BeforeIndex : Array[Row] = null
  var count2BeforeIndex : Array[Row] = null

  override def beforeAll: Unit = {
    sql("drop table if exists seccust")
    sql("create table seccust (id string, c_custkey string, c_name string, c_address string, c_nationkey string, c_phone string,c_acctbal decimal, c_mktsegment string, c_comment string) STORED AS carbondata")
    sql(s"""load data  inpath '${pluginResourcesPath}/secindex/firstunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")
    sql(s"""load data  inpath '${pluginResourcesPath}/secindex/secondunique.csv' into table seccust options('DELIMITER'='|','QUOTECHAR'='\"','FILEHEADER'='id,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment')""")

    count1BeforeIndex = sql("select count(*) from seccust where c_phone = '25-989-741-2988'").collect()
    count2BeforeIndex = sql("select count(*) from seccust where (c_mktsegment ='BUILDING' and c_phone ='25-989-741-2989')").collect()

    sql("drop index if exists sc_indx5 on seccust")
    sql("drop index if exists sc_indx6 on seccust")

    sql("create index sc_indx6 on table seccust(c_phone,c_mktsegment) AS 'carbondata'")
    sql("create index sc_indx5 on table seccust(c_phone) AS 'carbondata'")
    sql("set carbon.si.lookup.partialstring=true")
  }

  test("Test NI UDF") {

    val df = sql("select count(*) from seccust where c_phone = '25-989-741-2988'")
    assert(isIndexTablePresent(df)) // Index Table should present

    val df1 = sql("select count(*) from seccust where NI(c_phone = '25-989-741-2988')")
    assert(!isIndexTablePresent(df1)) // Index Table should't present

    checkAnswer(sql("select count(*) from seccust where NI(c_phone = '25-989-741-2988')"), count1BeforeIndex)
    checkAnswer(sql("select count(*) from seccust where NI(c_mktsegment ='BUILDING' and c_phone ='25-989-741-2989')"), count2BeforeIndex)
  }

  test("With partialstring=true for starts with") {
  try {
    sql("set carbon.lookup.partialstring=true")

    val dfT = sql("select count(*) from seccust where c_phone like '25-989-741-2988%'")
    assert(isIndexTablePresent(dfT)) // Index Table should present

    val dfT2 = sql("select count(*) from seccust where NI(c_phone like '25-989-741-2988%')")
    assert(!isIndexTablePresent(dfT2)) // Index Table should't present
  } finally {
      sql(s"set carbon.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("With partialstring=true for Ends With") {
  try {
    sql("set carbon.lookup.partialstring=true")

    val dfET1 = sql("select count(*) from seccust where c_phone like '%25-989-741-2988'")
    assert(isIndexTablePresent(dfET1)) // Index Table should present

    val dfET2 = sql("select count(*) from seccust where NI(c_phone like '%25-989-741-2988')")
    assert(!isIndexTablePresent(dfET2)) // Index Table should't present
  } finally {
    sql(s"set carbon.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
  }
  }

  test("With partialstring=true for Contains") {
    try {
      sql("set carbon.lookup.partialstring=true")

      val dfCT1 = sql("select count(*) from seccust where c_phone like '%25-989-741-2988%'")
      assert(isIndexTablePresent(dfCT1)) // Index Table should present

      val dfCT2 = sql("select count(*) from seccust where NI(c_phone like '%25-989-741-2988%')")
      assert(!isIndexTablePresent(dfCT2)) // Index Table should't present
    } finally {
      sql(s"set carbon.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("With partialstring=false for starts with") {
    try {
      sql("set carbon.si.lookup.partialstring=false")

      val dfSF1 = sql("select count(*) from seccust where c_phone like '25-989-741-2988%'")
      assert(isIndexTablePresent(dfSF1)) // Index Table should present

      val dfSF2 = sql("select count(*) from seccust where NI(c_phone like '25-989-741-2988%')")
      assert(!isIndexTablePresent(dfSF2)) // Index Table should't present
    } finally {
      sql(s"set carbon.si.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("With partialstring=false for Ends With") {
    try {
      sql("set carbon.si.lookup.partialstring=false")

      val dfEF1 = sql("select count(*) from seccust where c_phone like '%25-989-741-2988'")
      assert(!isIndexTablePresent(dfEF1)) // Index Table should't present

      val dfEF2 = sql("select count(*) from seccust where NI(c_phone like '%25-989-741-2988')")
      assert(!isIndexTablePresent(dfEF2)) // Index Table should't present
    } finally {
      sql(s"set carbon.si.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("With partialstring=false for Contains") {
    try {
      sql("set carbon.si.lookup.partialstring=false")

      val dfCF1 = sql("select count(*) from seccust where c_phone like '%25-989-741-2988%'")
      assert(!isIndexTablePresent(dfCF1)) // Index Table should't present

      val dfCF2 = sql("select count(*) from seccust where NI(c_phone like '%25-989-741-2988%')")
      assert(!isIndexTablePresent(dfCF2)) // Index Table should't present
    } finally {
      sql(s"set carbon.si.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("Check SI Pushing or Not when partialstring=True") {
    try {
      sql("set carbon.si.lookup.partialstring=true")
      val ch21 = sql("select * from seccust where c_phone like '25%989-741-2988'")
      //startsWith & endsWith so SI -yes
      assert(checkSIColumnsSize(ch21, 3)) // size = length, startsWith and EndsWith

      val ch22 = sql("select count(*) from seccust where c_phone like '%989-741-2988'")
      // endsWith so, SI - Yes
      assert(checkSIColumnsSize(ch22, 1)) // size = EndsWith

      val ch23 = sql("select count(*) from seccust where c_phone like '25%989-741%2988'")
      // Query startsWith & Contains & endsWith so SI - Yes (his is combined with Like, hence SI
      // - YES)
      assert(checkSIColumnsSize(ch23, 1))

      val ch24 = sql("select * from seccust where c_phone='25-989-741-2988'")
      // Query has EqualTo - So SI = Yes
      assert(checkSIColumnsSize(ch24, 1)) // EqualTo

    }finally{
      sql(s"set carbon.si.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  test("Check SI Pushing or Not when partialstring=False") {
    try {
      sql("set carbon.si.lookup.partialstring=false")
      val ch11 = sql("select count(*) from seccust where c_phone like '25%989-741-2988'")
      assert(checkSIColumnsSize(ch11, 2)) // pushed filter = length, startsWith

      val ch12 = sql("select count(*) from seccust where c_phone like '%989-741-2988'")
      // endsWith so SI - No
      assert(!isIndexTablePresent(ch12))

      val ch13 = sql("select count(*) from seccust where c_phone like '25%989-741%2988'") //
      // startsWith & Contains & endsWith so SI - Yes But this is combined with Like So--NO
      assert(!isIndexTablePresent(ch13))

      val ch14 = sql("select count(*) from seccust where c_phone='25-989-741-2988' and c_mktsegment like '%BUILDING'")
      // equals & endswith so SI - Yes
      assert(isIndexTablePresent(ch14))

      val ch15 = sql("select count(*) from seccust where c_phone='25-989-741-2988' and c_mktsegment like 'BUI%LDING'")
      // equals on c_phone of I1, I2 & (length & startsWith & endswith) on c_mktsegment of I2 so SI - Yes
      assert(checkSIColumnsSize(ch15, 3)) //size = EqualTo on c_phone, length, StartsWith

      val ch16 = sql("select * from seccust where c_phone='25-989-741-2988'")
      // Query has EqualTo so SI - Yes
      assert(checkSIColumnsSize(ch16, 1)) // size = EqualTo

    } finally{
      sql(s"set carbon.si.lookup.partialstring=${CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING_DEFAULT}")
    }
  }

  // DTS2018112703907  Jinling Issue
  test("creation of parquet table with filter on SI column") {
    sql("drop table if exists testParq")
    sql(
      "create table testParq stored as parquet select * from seccust where " +
      "c_phone='25-989-741-2988'")
    sql("drop table if exists testParq")
  }

  // DTS2019030809425
  test("order by with SI filter") {
    sql("drop table if exists testOrderBy")
    sql(s"CREATE TABLE testOrderBy(empno int, empname String, designation String, " +
        s"doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, " +
        s"deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp," +
        s"attendance int, utilization int, salary int) STORED AS carbondata")
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE testOrderBy")
    sql("CREATE INDEX index_orderBy ON TABLE testOrderBy (workgroupcategoryname) AS 'carbondata'")
    sql("CREATE INDEX index1_orderBy ON TABLE testOrderBy (deptname) AS 'carbondata'")
    sql(
      "select designation from testOrderBy where deptname IN ('network', " +
      "'protocol','security') OR workgroupcategoryname IN ('developer','tester','manager') " +
      "order by designation desc limit 1").show(false)
    sql("drop table if exists testOrderBy")
  }

  def isIndexTablePresent(plan: DataFrame): Boolean = {
    plan.queryExecution.optimizedPlan.find {
      case PhysicalOperation(projects, filters, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        CarbonInternalScalaUtil.isIndexTable(relation.carbonTable)
      case _ => false
    }.isDefined
  }

  // Checks the Number of pushed columns inside the Index Table
  def checkSIColumnsSize(plan: DataFrame, size: Integer): Boolean = {
    plan.queryExecution.optimizedPlan.find {
      case PhysicalOperation(projects, filters, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        lazy val ll = filters.map( _.collect {
          case atr:AttributeReference => atr }
        ).foldLeft(Seq[Expression]())((cs, s) => cs ++ s)
        CarbonInternalScalaUtil.isIndexTable(relation.carbonTable) && ll.size == size
      case _ => false
    }.isDefined
  }

  override def afterAll: Unit = {
    sql("drop table if exists seccust")
  }
}
