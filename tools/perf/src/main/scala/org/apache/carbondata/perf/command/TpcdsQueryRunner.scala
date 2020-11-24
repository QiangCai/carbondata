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

import org.apache.spark.sql.SqlHelper.runQueriesWithOption

import org.apache.carbondata.perf.Constant.{carbondb, parquetdb, sqlFolder}
import org.apache.carbondata.perf.PerfHelper.{loadSqlDir, loadSqlFile, showDetails, showSummary}

/**
 * tpcds query runner
 */
case class TpcdsQueryRunner(database: Option[String] = None,
    queryFiles: Option[Seq[String]] = None) extends Command {

  override def run(): Unit = {
    if (database.isDefined && queryFiles.isDefined) {
      runFilesOnOneDatabase(database.get, queryFiles.get)
    } else if (database.isDefined) {
      runOneDatabase(database.get)
    } else if (queryFiles.isDefined) {
      runFilesOnAllDatabases(queryFiles.get)
    } else {
      runAllDatabases()
    }
  }

  def runFilesOnOneDatabase(database: String, queryFileIndexes: Seq[String]): Unit = {
    val sqlFiles = loadSpecifiedFiles(queryFileIndexes)
    runSpecifiedFilesOnOneDatabases(database, sqlFiles)
  }

  private def runOneDatabase(database: String): Unit = {
    val sqlFiles = loadSqlDir(s"$sqlFolder/query")
    runSpecifiedFilesOnOneDatabases(database, sqlFiles)
  }

  def runFilesOnAllDatabases(queryFileIndexes: Seq[String]): Unit = {
    val sqlFiles = loadSpecifiedFiles(queryFileIndexes)
    runSpecifiedFilesOnAllDatabases(sqlFiles)
  }

  private def runAllDatabases(): Unit = {
    val sqlFiles = loadSqlDir(s"$sqlFolder/query")
    runSpecifiedFilesOnAllDatabases(sqlFiles)
  }

  private def loadSpecifiedFiles(queryFileIndexes: Seq[String]): Seq[(String, Array[String])] = {
    queryFileIndexes
      .map(index => s"$sqlFolder/query/q" + index + ".sql")
      .map(loadSqlFile(_))
  }

  private def runSpecifiedFilesOnAllDatabases(fileList: Seq[(String, Array[String])]): Unit = {
    val parquetTime = runQueriesWithOption(fileList, dbOption = Some(parquetdb))
    val carbonTime = runQueriesWithOption(fileList, dbOption = Some(carbondb))
    showDetails(parquetdb, parquetTime, carbondb, carbonTime)
    showSummary(parquetdb, parquetTime, carbondb, carbonTime)
  }

  private def runSpecifiedFilesOnOneDatabases(database: String,
      fileList: Seq[(String, Array[String])]): Unit = {
    val times = runQueriesWithOption(fileList, dbOption = Some(database))
    showDetails(database, times)
    showSummary(database, times)
  }
}
