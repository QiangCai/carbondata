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

package org.apache.carbondata.perf.util

import scala.io.Source

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, DataFrame, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * SqlHelper
 */
class SqlHelper {
  // env
  val csvFolder = "/opt/bigdata/data/tpcds"
  val codeWorkSpace = "/home/david/Documents/code/carbondata"
  val warehouse = "/opt/bigdata/warehouse"
  val metadata = "/opt/bigdata/metadata/metastore_db"

  val spark: SparkSession = createSparkSession("Sql Helper", 2)

  def sql(sqlText: String): DataFrame = spark.sql(sqlText)

  def loadSql(filePath: String, variableMap: Map[String, String]): Array[String] = {
    val source = Source.fromFile(filePath)
    var sqls =
      source
        .getLines()
        .filter(!StringUtils.isEmpty(_))
        .filter(!_.startsWith("--"))
        .mkString("\n")
    source.close()
    variableMap.foreach { case (key, value) =>
      sqls = sqls.replaceAll("\\$\\{" + key + "\\}", value)
    }
    sqls
      .split(";", -1)
      .map(_.stripPrefix("\n"))
      .map(_.stripSuffix("\n"))
      .filter(!StringUtils.isEmpty(_))
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
    spark.sql(currentSql).show(false)
    val t4 = System.currentTimeMillis()
    val carbonTime = t4 - t3
    println(s"finish carbon query, taken time: ${ carbonTime } ms")
    println(s"query taken time: carbon vs parquet = ${ carbonTime.toFloat / parquetTime } : 1")
  }

  def createSparkSession(appName: String, workThreadNum: Int = 1): SparkSession = {
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
      .config("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$metadata;create=true")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .config("hive.exec.max.dynamic.partitions", "20000")
      .config("spark.carbon.pushdown.join.as.filter", "false")
      .enableHiveSupport()
      .getOrCreate()
    CarbonEnv.getInstance(spark)
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def println(x: String): Unit = {
    // scalastyle:off println
    System.out.println(x)
    // scalastyle:on println
  }
}
