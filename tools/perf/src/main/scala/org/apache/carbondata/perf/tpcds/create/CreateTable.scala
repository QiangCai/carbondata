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

package org.apache.carbondata.perf.tpcds.create

import org.apache.carbondata.perf.util.SqlHelper

/**
 * CreateTable
 */
object CreateTable extends SqlHelper {

  val sqlFolder =
    s"$codeWorkSpace/tools/perf/src/main/resources/tpcds/create";

  def main(args: Array[String]): Unit = {
    loadCSVIntoDB(csvFolder, "csvdb")
    val carbonTime = insertIntoCarbon("csvdb", "carbondb")
    val parquetTime = insertIntoParquet("csvdb", "parquetdb")
    println(s"load taken time: carbon vs parquet = ${ carbonTime.toFloat / parquetTime } : 1 ")
  }

  def loadCSVIntoDB(csvFolder: String, dbName: String): Unit = {
    val sqlTexts = loadSql(
      s"$sqlFolder/create_csv.sql", Map("DB" -> dbName, "LOCATION" -> csvFolder))._2
    sqlTexts.foreach { sqlText =>
      println("================================")
      println(sqlText)
      sql(sqlText)
    }
    sql("show tables").show(100, false)
  }

  def insertIntoParquet(source: String, target: String): Long = {
    println("start to load parquet...")
    val sqlTexts = loadSql(s"$sqlFolder/create_parquet_with_partition.sql",
      Map("DB" -> target, "SOURCE" -> source, "FILE" -> "parquet"))._2
    val t1 = System.currentTimeMillis()
    sqlTexts.foreach { sqlText =>
      println("================================")
      println(sqlText)
      sql(sqlText)
    }
    sql("show tables").show(100, false)
    val t2 = System.currentTimeMillis()
    val time = t2 - t1
    println(s"finish to load parquet, taken time: ${ time } ms")
    time
  }

  def insertIntoCarbon(source: String, target: String): Long = {
    println("start to load carbon...")
    val sqlTexts = loadSql(s"$sqlFolder/create_carbon_with_partition.sql",
      Map("DB" -> target, "SOURCE" -> source, "FILE" -> "carbondata"))._2
    val t1 = System.currentTimeMillis()
    sqlTexts.foreach { sqlText =>
      println("================================")
      println(sqlText)
      sql(sqlText)
    }
    sql("show tables").show(100, false)
    val t2 = System.currentTimeMillis()
    val time = t2 - t1
    println(s"finish to load carbon, taken ${ time } ms")
    time
  }
}
