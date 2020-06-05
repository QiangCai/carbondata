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

import java.io.File

import scala.collection.mutable
import scala.io.Source

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, DataFrame, SparkPerfUtil, SparkSession}

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

  val spark: SparkSession = createSparkSession("Sql Helper", 4)

  def sql(sqlText: String): DataFrame = spark.sql(sqlText)

  def loadSql(filePath: String, variableMap: Map[String, String]): (String, Array[String]) = {
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
    val finalSqls = sqls
      .split(";", -1)
      .map(_.stripPrefix("\n"))
      .map(_.stripSuffix("\n"))
      .filter(!StringUtils.isEmpty(_))
    (filePath.substring(filePath.lastIndexOf("/") + 1), finalSqls)
  }

  def compareFile(filePath: String, db1: String, db2: String, silent: Boolean = false): (Long, Long) = {
    val sqlTexts = loadSql(filePath, Map.empty)
    compareQuerys(Seq(sqlTexts), db1, db2, silent)
  }

  def compareFolder(folderPath: String, db1: String, db2: String, silent: Boolean = false): (Long, Long) = {
    val folder = new File(folderPath)
    val files = folder.listFiles()
    val sqlTexts = files.map(_.getCanonicalPath).sorted.map { filePath =>
      loadSql(filePath, Map.empty)
    }
    compareQuerys(sqlTexts, db1, db2, silent)
  }

  case class Detail(var t1: Long, var t2: Long, var t2_r: Long, var r: Float, var r_r: Float)

  def formatFloat(f: Float): String = {
    var bd = new java.math.BigDecimal(java.lang.Float.toString(f))
    bd = bd.setScale(3, java.math.BigDecimal.ROUND_HALF_UP)
    bd.toString
  }

  def compareQuerys(sqlTexts: Seq[(String, Array[String])], db1: String, db2: String, silent: Boolean = false): (Long, Long) = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_RUNTIME_FILTER_ENABLE, "false")
    val db1Times = runQuerys(sqlTexts, db1, silent)
    val db1Time = db1Times.map(_._2).sum
    val db2Times = runQuerys(sqlTexts, db2, silent)
    val db2Time = db2Times.map(_._2).sum
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_RUNTIME_FILTER_ENABLE, "true")
    val db3Times = runQuerys(sqlTexts, db2, silent)
    val db3Time = db3Times.map(_._2).sum
    val maxLen = Math.max(db1.length, db2.length)
    println(s"${rightPad(db1, maxLen)} taken time: $db1Time ms")
    println(s"${rightPad(db2, maxLen)} taken time: $db2Time ms")
    println(s"${rightPad(db2, maxLen)} taken time: $db3Time ms (RunFilter)")
    println(s"$db2 vs $db1 = ${formatFloat(db2Time.toFloat / db1Time)} : 1\n")
    println(s"$db2 vs $db1 = ${formatFloat(db3Time.toFloat / db1Time)} : 1 (RunFilter)\n")
    val map = new mutable.HashMap[String, Detail]
    db1Times.foreach { time =>
      val detailOpt = map.get(time._1)
      if (detailOpt.isEmpty) {
        map(time._1) = Detail(time._2, 0L, 0L, 0f, 0f)
      } else {
        detailOpt.get.t1 = time._2
      }
    }
    db2Times.foreach { time =>
      val detailOpt = map.get(time._1)
      if (detailOpt.isEmpty) {
        map(time._1) = Detail(0L, time._2, 0L, 0f, 0f)
      } else {
        val detail = detailOpt.get
        detail.t2 = time._2
        if (detail.t1 != 0) {
          detail.r = detail.t2.toFloat / detail.t1
        }
      }
    }

    db3Times.foreach { time =>
      val detailOpt = map.get(time._1)
      if (detailOpt.isEmpty) {
        map(time._1) = Detail(0L, 0L, time._2, 0f, 0f)
      } else {
        val detail = detailOpt.get
        detail.t2_r = time._2
        if (detail.t1 != 0) {
          detail.r_r = detail.t2_r.toFloat / detail.t1
        }
      }
    }
    val head = Seq(Seq("fileName", s"$db1(ms)", s"$db2(ms)", s"${db2}_RF(ms)",  s"$db2/$db1", s"${db2}_RF/$db1"))
    val body = map.map { entry =>
      Seq(entry._1, entry._2.t1.toString, entry._2.t2.toString, entry._2.t2_r.toString, formatFloat(entry._2.r), formatFloat(entry._2.r_r))
    }.toSeq.sortBy(_.head)
    show((head ++ body).toArray, 6)
    (db1Time, db2Time)
  }

  def runFolder(folderPath: String, db: String, silent: Boolean = false): Long = {
    val folder = new File(folderPath)
    val files = folder.listFiles()
    val sqlTexts = files.map(_.getCanonicalPath).sorted.map { filePath =>
      loadSql(filePath, Map.empty)
    }
    val fileTimes = runQuerys(sqlTexts, db, silent)
    val showRows = Seq(Seq("fileName", "time taken(ms)")) ++ fileTimes.map( x => Seq(x._1, x._2.toString))
    show(showRows.toArray, 2)
    fileTimes.map(_._2).sum
  }

  def runFile(filePath: String, db: String, silent: Boolean = false): Long = {
    val sqlTexts = loadSql(filePath, Map.empty)
    val fileTimes = runQuerys(Seq(sqlTexts), db, silent)
    val showRows = Seq(Seq("file", s"$db(ms)")) ++ fileTimes.map( x => Seq(x._1, x._2.toString))
    show(showRows.toArray, 2)
    fileTimes.map(_._2).sum
  }

  def runQuerys(sqlTexts: Seq[(String, Array[String])], db: String, silent: Boolean = false): Seq[(String, Long)] = {
    println(s"start query on database: $db ...")
    spark.sql(s"use $db")
    val fileTimes = sqlTexts.map { fileSql =>
      val fileTime = fileSql._2.zipWithIndex.map { sqlText =>
        println(s"running sql[${sqlText._2 + 1}] in file ${fileSql._1}")
        if (!silent) {
          println(sqlText._1)
        }
        runQuery(sqlText._1, silent)
      }.sum
      (fileSql._1, fileTime)
    }
    val dbTime = fileTimes.map(_._2).sum
    println(s"$db taken time: $dbTime ms\n")
    fileTimes
  }

  def runQuery(sqlText: String, silent: Boolean = false): Long = {
    val t1 = System.currentTimeMillis()
    val df = spark.sql(sqlText)
    val rows = df.collect()
    val t2 = System.currentTimeMillis()
    val time = t2 - t1

    if (!silent) {
      val schema = SparkPerfUtil.getLogicalPlan(df).output
      val showRows = schema.map(_.name) +: rows.map { row =>
        row.toSeq.map {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case cell => cell.toString
        }: Seq[String]
      }
      show(showRows, schema.size)
    }
    println(s"get ${rows.length} ${if (rows.length > 1) "rows" else "row" }, taken ${time} ms\n")
    time
  }

  def show(rows: Array[Seq[String]], numCols: Int): Unit = {
    val sb = new StringBuilder
    val minimumColWidth = 3
    val colWidths = Array.fill(numCols)(minimumColWidth)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex) {
        colWidths(i) = math.max(colWidths(i), SparkPerfUtil.stringHalfWidth(cell))
      }
    }

    val paddedRows = rows.map { row =>
      row.zipWithIndex.map { case (cell, i) =>
        StringUtils.rightPad(cell, colWidths(i) - SparkPerfUtil.stringHalfWidth(cell) + cell.length)
      }
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

    // column names
    paddedRows.head.addString(sb, "|", "|", "|\n")
    sb.append(sep)

    // data
    paddedRows.tail.foreach(_.addString(sb, "|", "|", "|\n"))
    sb.append(sep)
    println(sb.toString())
  }

  def rightPad(text: String, len: Int): String = {
    StringUtils.rightPad(text, len, " ")
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
