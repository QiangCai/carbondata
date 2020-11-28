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

import org.apache.carbondata.perf.Constant.{allFormats, sqlDir}
import org.apache.carbondata.perf.PerfHelper.{loadSqlDir, loadSqlFile, showDetail, showDetailContest, showSummary, showSummaryContest}

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
    val sqlFiles = loadSqlDir(s"$sqlDir/query")
    runSpecifiedFilesOnOneDatabases(database, sqlFiles)
  }

  def runFilesOnAllDatabases(queryFileIndexes: Seq[String]): Unit = {
    val sqlFiles = loadSpecifiedFiles(queryFileIndexes)
    runSpecifiedFilesOnAllDatabases(sqlFiles)
  }

  private def runAllDatabases(): Unit = {
    val sqlFiles = loadSqlDir(s"$sqlDir/query")
    runSpecifiedFilesOnAllDatabases(sqlFiles)
  }

  private def loadSpecifiedFiles(queryFileIndexes: Seq[String]): Seq[(String, Array[String])] = {
    queryFileIndexes
      .map(index => s"$sqlDir/query/q" + index + ".sql")
      .map(loadSqlFile(_))
  }

  private def runSpecifiedFilesOnAllDatabases(fileList: Seq[(String, Array[String])]): Unit = {
    val resultDetails = allFormats.map { case (_, db) =>
      (db, runQueriesWithOption(fileList, dbOption = Some(db)))
    }
    showDetailContest(resultDetails)
    showSummaryContest(resultDetails)
  }

  private def runSpecifiedFilesOnOneDatabases(database: String,
      fileList: Seq[(String, Array[String])]): Unit = {
    val times = runQueriesWithOption(fileList, dbOption = Some(database))
    showDetail(database, times)
    showSummary(database, times)
  }
}
