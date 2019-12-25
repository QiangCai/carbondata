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

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.test.TestQueryExecutor.{hdfsUrl, integrationPath, warehouse}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class is a sql executor of unit test case for spark version 2.x.
 */

class Spark2TestQueryExecutor extends TestQueryExecutorRegister {

  override def sql(sqlText: String): DataFrame = Spark2TestQueryExecutor.spark.sql(sqlText)

  override def sqlContext: SQLContext = Spark2TestQueryExecutor.spark.sqlContext

  override def stop(): Unit = Spark2TestQueryExecutor.spark.stop()
}

object Spark2TestQueryExecutor {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  LOGGER.info("use TestQueryExecutorImplV2")
  CarbonProperties.getInstance()
//    .addProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
//      System.getProperty("java.io.tmpdir"))
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")


  import org.apache.spark.sql.CarbonSession._

  val conf = new SparkConf()
  if (!TestQueryExecutor.masterUrl.startsWith("local")) {
    conf.setJars(TestQueryExecutor.jars).
      set("spark.driver.memory", "6g").
      set("spark.executor.memory", "4g").
      set("spark.executor.cores", "2").
      set("spark.executor.instances", "2").
      set("spark.cores.max", "4")
    FileFactory.getConfiguration.
      set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  }
  val metastoredb = s"$integrationPath/spark-common-cluster-test/target"
  val spark = SparkSession
    .builder().config(conf)
    .master(TestQueryExecutor.masterUrl)
    .appName("Spark2TestQueryExecutor")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir", warehouse)
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.extensions", "org.apache.spark.sql.CarbonInternalExtensions")
    .getOrCreate()
  if (warehouse.startsWith("hdfs://")) {
    System.setProperty(CarbonCommonConstants.HDFS_TEMP_LOCATION, warehouse)
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    ResourceRegisterAndCopier.
      copyResourcesifNotExists(hdfsUrl, s"$integrationPath/spark-common-test/src/test/resources",
        s"$integrationPath//spark-common-cluster-test/src/test/resources/testdatafileslist.txt")
  }
  FileFactory.getConfiguration.
    set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  spark.sparkContext.setLogLevel("ERROR")
}
