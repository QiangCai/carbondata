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

package org.apache.spark.sql.test

import java.io.File

import org.apache.spark.sql.SparkSession
import org.junit.Ignore

/**
 * test cases for creating carbon session with different parameters
 */
class TestSessionParameter {

  @Ignore
  //test getorCreateCarbonSession with spark-conf 'spark.carbon.sessionstate.classname'
  def test_Sparkconf_Classname() {
    val sparkSession = TestSessionParameter
      .createSession("spark.carbon.sessionstate.classname")
    sparkSession.sql("drop table if exists dest")
    sparkSession.sql(
      "create table dest (c1 string,c2 int) STORED AS carbondata")
    sparkSession.sql("insert into dest values('ab',1)")
    val result = sparkSession.sql("select count(*) from dest").collect()
    assert(result.length.equals(1))
    sparkSession.close()
  }

  @Ignore
  //test getorCreateCarbonSession with spark-conf 'spark.sql.session.state.builder'
  def test_Sparkconf_Builder() {
    val sparkSession = TestSessionParameter
      .createSession("spark.sql.session.state.builder")
    sparkSession.sql("drop table if exists dest")
    sparkSession.sql(
      "create table dest (c1 string,c2 int) STORED AS carbondata")
    sparkSession.sql("insert into dest values('ab',1)")
    val result = sparkSession.sql("select count(*) from dest").collect()
    assert(result.length.equals(1))
    sparkSession.close()
  }

}

object TestSessionParameter {

  def createSession(config: String): SparkSession = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target/metastore_db"
    import org.apache.spark.sql.CarbonSession._
    //create carbon session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("TestGetOrCreateCarbonSession")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.sql.crossJoin.enabled", "true")
      .config(config, "org.apache.spark.sql.hive.CarbonACLInternalSessionStateBuilder")
      .config("spark.sql.catalog.class", "org.apache.spark.sql.hive.HiveACLExternalCatalog")
      .config("spark.sql.hive.implementation", "org.apache.spark.sql.hive.HiveACLClientImpl")
      .config("spark.sql.hiveClient.isolation.enabled", "false")
      .getOrCreateCarbonSession(storeLocation, metastoredb)
    spark
  }
}
