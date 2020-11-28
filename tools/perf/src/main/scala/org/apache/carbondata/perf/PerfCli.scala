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

package org.apache.carbondata.perf

import java.io.{BufferedReader, InputStreamReader}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SqlHelper

import org.apache.carbondata.perf.Constant.{carbondb, orcdb, parquetdb}
import org.apache.carbondata.perf.PerfHelper.{myPrint, myPrintln}
import org.apache.carbondata.perf.command.{TpcdsCleaner, TpcdsCreateTable, TpcdsExplain, TpcdsQueryRunner}
import org.apache.carbondata.perf.MatchTpcdsQuery.MatchTpcdsExplian

object PerfCli {

  // need to set these for your env
  val dataDir = "/opt/bigdata/data"
  val tpcds_scale = 10

  def main(args: Array[String]): Unit = {
    val input = new BufferedReader(new InputStreamReader(System.in))
    var newLine = ""
    val builder = new java.lang.StringBuilder()
    var continue = true
    SqlHelper.initConfiguration()
    while(continue) {
      myPrint(sqlPrompt)
      newCommandExecuted = false
      newLine = input.readLine()
      val trimLine = newLine.trim
      if (StringUtils.isNotEmpty(trimLine)) {
        if (trimLine.startsWith("!") && builder.length() == 0) {
          if (trimLine.equalsIgnoreCase("!q")) {
            continue = false
          } else {
            catchException(newLine)(executeCommand)
          }
        } else if (trimLine.endsWith(";")) {
          builder.append(newLine)
          val statement = builder.toString
          builder.setLength(0)
          catchException(statement)(executeSql)
        } else {
          builder.append(newLine).append("\n")
        }
      }
    }
  }

  private lazy val user = System.getProperty("user.name")
  private var currentDatabase = "default"
  private var newCommandExecuted = true

  private def sqlPrompt: String = {
    if (newCommandExecuted) {
      currentDatabase = SqlHelper.getCurrentDatabase()
    }
    s"$user's sql in $currentDatabase/>"
  }

  private def executeCommand(command: String): Unit = {
    val normalizeCommand = normalize(command)
    normalizeCommand.toLowerCase match {
      case "tpcds load" => TpcdsCreateTable().run()
      case "tpcds clean" => TpcdsCleaner().run()
      case MatchTpcdsQuery(queryRunner) => queryRunner.run()
      case MatchTpcdsExplian(explain) => explain.run()
      case _ =>
        myPrintln(s"Unknown command: $normalizeCommand")
    }
  }

  private def executeSql(sqlText: String): Unit = {
    SqlHelper.runQuery(normalize(sqlText))
  }

  private def normalize(statement: String): String = {
    var trimStatement = statement.trim
    if (trimStatement.endsWith(";")) {
      trimStatement = trimStatement.substring(0, trimStatement.length - 1)
    }
    if (trimStatement.startsWith("!")) {
      trimStatement = trimStatement.substring(1)
    }
    trimStatement
  }

  private def catchException(sqlText: String)(func: String => Unit): Unit = {
    try {
      newCommandExecuted = true
      func(sqlText)
    } catch {
      case e: Throwable =>
        myPrintln(e.getMessage)
        e.printStackTrace(System.err)
    }
  }

}

object MatchTpcdsQuery {
  // return (db, file list)
  def unapply(command: String): Option[TpcdsQueryRunner] = {

    def collectFileList(offset: Int): Option[Seq[String]] = {
      Some(command.substring(offset).split(",", -1).toSeq.map(_.trim))
    }

    command match {
      case "tpcds query" => Some(TpcdsQueryRunner())
      case "tpcds carbon query" => Some(TpcdsQueryRunner(Some(carbondb)))
      case "tpcds parquet query" => Some(TpcdsQueryRunner(Some(parquetdb)))
      case "tpcds orc query" => Some(TpcdsQueryRunner(Some(orcdb)))
      case _ =>
        if (command.startsWith("tpcds query ")) {
          Some(TpcdsQueryRunner(None, collectFileList("tpcds query ".length)))
        } else if (command.startsWith("tpcds carbon query ")) {
          Some(TpcdsQueryRunner(Some(carbondb), collectFileList("tpcds carbon query ".length)))
        } else if (command.startsWith("tpcds parquet query ")) {
          Some(TpcdsQueryRunner(Some(parquetdb), collectFileList("tpcds parquet query ".length)))
        } else if (command.startsWith("tpcds orc query ")) {
          Some(TpcdsQueryRunner(Some(parquetdb), collectFileList("tpcds orc query ".length)))
        } else {
          None
        }
    }
  }

  object MatchTpcdsExplian {
    def unapply(command: String): Option[TpcdsExplain] = {
      command match {
        case "tpcds carbon explain" | "tpcds parquet explain" | "tpcds orc explain" =>
          Some(TpcdsExplain(carbondb))
        case _ => None
      }
    }
  }
}
