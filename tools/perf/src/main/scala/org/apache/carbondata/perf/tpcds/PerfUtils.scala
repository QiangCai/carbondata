/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.perf.tpcds

import java.io.File

import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * PerfUtils
 */
object PerfUtils {

  def main(args: Array[String]): Unit = {
    val spark = PerfUtils.createSparkSession("Tpc-ds Query", 4)
    CreateTable.loadAllData(spark)
    executeQuery(Query9.sql9)(spark)
    Thread.sleep(1000000)
  }

  def executeQuery(currentSql: String)(spark: SparkSession): Unit = {
    spark.sql("use parquet")
    System.out.println("start parquet query...")
    val t1 = System.currentTimeMillis()
    spark.sql(currentSql).collect()
    val t2 = System.currentTimeMillis()
    val parquetTime = t2 - t1
    System.out.println(s"finish parquet query, taken time: ${ parquetTime } ms")
    System.out.println("start carbon query...")
    spark.sql("use carbon")
    // spark.sql(Query14.sql14a).collect()
    val t3 = System.currentTimeMillis()
    spark.sql(currentSql).show(false).collect()
    val t4 = System.currentTimeMillis()
    val carbonTime = t4 - t3
    System.out.println(s"finish carbon query, taken time: ${ carbonTime } ms")
    System.out.println(
      s"query taken time: carbon vs parquet = ${ carbonTime.toFloat / parquetTime } : 1")
  }

  def createSparkSession(appName: String, workThreadNum: Int = 1): SparkSession = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val warehouse = s"$rootPath/examples/spark/target/warehouse"
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")
    val masterUrl = if (workThreadNum <= 1) {
      "local"
    } else {
      "local[" + workThreadNum.toString() + "]"
    }
    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .enableHiveSupport()
      .getOrCreate()
    CarbonEnv.getInstance(spark)
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }


}
