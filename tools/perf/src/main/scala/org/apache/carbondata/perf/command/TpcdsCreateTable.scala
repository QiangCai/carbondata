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

package org.apache.carbondata.perf.command

import org.apache.spark.sql.SqlHelper._

import org.apache.carbondata.perf.Constant._
import org.apache.carbondata.perf.PerfHelper._

/**
 * tpcds create and load table
 */
case class TpcdsCreateTable() extends Command {

  def run(): Unit = {
    loadCsv(csvFolder, csvdb)
    val parquetTimes = insertInto(csvdb, parquetdb, "parquet")
    val carbonTimes = insertInto(csvdb, carbondb, "carbondata")
    showSummary(parquetdb, parquetTimes, carbondb, carbonTimes)
  }

  def loadCsv(csvFolder: String, dbName: String): Unit = {
    runFile(s"$sqlFolder/text/alltables.sql",
      Some(Map("DB" -> dbName, "LOCATION" -> csvFolder)))
  }

  def insertInto(source: String, target: String, fileFormat: String): Seq[(String, Long)] = {
    myPrintln(s"start to load $target...")
    runDir(s"$sqlFolder/partitioned",
      Some(Map("DB" -> target, "SOURCE" -> source, "FILE" -> fileFormat)))
  }
}
