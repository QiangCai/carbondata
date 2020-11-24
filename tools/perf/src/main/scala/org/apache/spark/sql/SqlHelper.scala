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

package org.apache.spark.sql

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.perf.Constant
import org.apache.carbondata.perf.PerfHelper._

/**
 * SqlHelper
 */
object SqlHelper {

  private val spark: SparkSession = createSparkSession("Sql Helper", 2)

  private def sql(sqlText: String): DataFrame = spark.sql(sqlText)

  def initConfiguration(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
      .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "")
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_AUDIT, "false")
      .addProperty(CarbonCommonConstants.CARBON_ENABLE_MV, "false")
      .addProperty(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION, "blocklet")
      .addProperty(CarbonCommonConstants.CARBON_PUSH_ROW_FILTERS_FOR_VECTOR, "false")
      .addProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE, "false")
  }

  def getCurrentDatabase(): String = {
    spark.sessionState.catalog.getCurrentDatabase
  }

  def runDir(dirPath: String,
      variableMap: Option[Map[String, String]] = None,
      confOption: Option[() => Unit] = None,
      dbOption: Option[String] = None): Seq[(String, Long)] = {
    val sqlFiles = loadSqlDir(dirPath, variableMap)
    runQueriesWithOption(sqlFiles, confOption, dbOption)
  }

  def runFile(filePath: String,
      variableMap: Option[Map[String, String]],
      confOption: Option[() => Unit] = None,
      dbOption: Option[String] = None): (String, Long) = {
    val sqlTexts = loadSqlFile(filePath, variableMap)
    runQueriesWithOption(Seq(sqlTexts), confOption, dbOption).head
  }

  def runQueriesWithOption(sqlFiles: Seq[(String, Array[String])],
      confOption: Option[() => Unit] = None,
      dbOption: Option[String] = None): Seq[(String, Long)] = {
    if (confOption.isDefined) {
      confOption.get.apply()
    }
    if (dbOption.isDefined) {
      runQuery(s"use ${ dbOption.get }", true, false)
    }
    runQueries(sqlFiles)
  }

  def runQueries(sqlFiles: Seq[(String, Array[String])],
      silentPrint: Boolean = false): Seq[(String, Long)] = {
    val (printSql, printResult) = if (silentPrint) {
      (false, false)
    } else {
      (true, true)
    }

    sqlFiles.map { case (file, sqlTexts) =>
      myPrintln(s"start to run file: $file")
      val takenTime = sqlTexts.zipWithIndex.map { case (sqlText, index) =>
        myPrintln(s"running sql[${ index }] in file $file")
        val period = runQuery(sqlText, printSql, printResult)
        period
      }.sum
      (file, takenTime)
    }
  }

  def runQuery(sqlText: String, printSql: Boolean = false, printResult: Boolean = true): Long = {
    if (printSql) {
      myPrintln(sqlText)
    }
    val t1 = System.currentTimeMillis()
    val df = sql(sqlText)
    val rows = df.collect()
    val t2 = System.currentTimeMillis()
    val period = t2 - t1
    if (printResult) {
      val schema = SparkUtils.logicalPlan(df).output.map(_.name)
      val showRows = new ArrayBuffer[Seq[String]](rows.length + 1)
      showRows += schema
      rows.foreach { row =>
        showRows += row.toSeq.map {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case cell => cell.toString
        }
      }
      show(showRows.toArray, schema.size)
    }
    myPrintln(s"get ${rows.length} ${if (rows.length > 1) "rows" else "row"}, " +
              s"taken ${formatMillis(period)}\n")
    period
  }

  private def createSparkSession(appName: String, workThreadNum: Int = 1): SparkSession = {
    val masterUrl = if (workThreadNum <= 1) {
      "local"
    } else {
      "local[" + workThreadNum.toString + "]"
    }
    val spark = SparkSession
      .builder()
      .master(masterUrl)
      .appName(appName)
      .config("spark.sql.warehouse.dir", Constant.warehouse)
      .config("javax.jdo.option.ConnectionURL",
        s"jdbc:derby:;databaseName=${Constant.metadata};create=true")
      .config("spark.driver.host", "localhost")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "20000")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .enableHiveSupport()
      .getOrCreate()
    CarbonEnv.getInstance(spark)
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def show(rows: Array[Seq[String]], numCols: Int): Unit = {
    val sb = new StringBuilder
    val minimumColWidth = 3
    val colWidths = Array.fill(numCols)(minimumColWidth)

    for(row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), SparkUtils.stringHalfWidth(cell))
      }
    }

    val paddedRows = rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        StringUtils.rightPad(cell, colWidths(i) - SparkUtils.stringHalfWidth(cell) + cell.length)
      }
    }

    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)
    if (paddedRows.length > 1) {
      paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
      sb.append(sep)
    }
    myPrintln(sb.toString())
  }
}
