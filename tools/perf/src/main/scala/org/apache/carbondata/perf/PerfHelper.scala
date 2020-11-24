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

import java.io.File

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SqlHelper.show
import org.joda.time.Duration
import org.joda.time.format.PeriodFormatterBuilder

object PerfHelper {
  def loadSqlDir(dirPath: String,
      variableMap: Option[Map[String, String]] = None): Seq[(String, Array[String])] = {
    val files = new File(dirPath).list().sorted
    val sqlListWithFileName = new ArrayBuffer[(String, Array[String])]()
    for (file <- files) {
      sqlListWithFileName += loadSqlFile(dirPath + "/" + file, variableMap)
    }
    sqlListWithFileName.toSeq
  }

  def loadSqlFile(filePath: String,
      variableMap: Option[Map[String, String]] = None): (String, Array[String]) = {
    val source = Source.fromFile(filePath)
    var sqlList =
      source
        .getLines()
        .filter(!StringUtils.isEmpty(_))
        .filter(!_.trim.startsWith("--"))
        .mkString("\n")
    source.close()
    variableMap.foreach { map =>
      map.foreach { case (key, value) =>
        sqlList = sqlList.replaceAll("\\$\\{" + key + "\\}", value)
      }
    }
    val finalSqlList = sqlList
      .split(";", -1)
      .map(_.stripPrefix("\n"))
      .map(_.stripSuffix("\n"))
      .filter(!StringUtils.isEmpty(_))
    (filePath.substring(filePath.lastIndexOf("/") + 1), finalSqlList)
  }

  def myPrintln(x: String): Unit = {
    // scalastyle:off println
    System.out.println(x)
    // scalastyle:on println
  }

  def myPrint(x: String): Unit = {
    // scalastyle:off println
    System.out.print(x)
    // scalastyle:on println
  }

  def rightPad(text: String, len: Int): String = {
    StringUtils.rightPad(text, len, " ")
  }

  def formatFloat(f: Float): String = {
    var db = new java.math.BigDecimal(java.lang.Float.toString(f))
    db = db.setScale(3, java.math.BigDecimal.ROUND_HALF_UP)
    db.toString
  }

  private val formatter = new PeriodFormatterBuilder()
    .appendHours
    .appendSuffix("h ")
    .appendMinutes
    .appendSuffix("m ")
    .appendSeconds
    .appendSuffix("s ")
    .appendMillis
    .appendSuffix("ms")
    .toFormatter

  def formatMillis(millis: Long): String = {
    formatter.print(new Duration(millis).toPeriod)
  }

  def showSummary(
      db1: String,
      db1Times: Seq[(String, Long)],
      db2: String,
      db2Times: Seq[(String, Long)]): Unit = {
    val db1Time = db1Times.map(_._2).sum
    val db2Time = db2Times.map(_._2).sum
    val maxLen = Math.max(db1.length, db2.length)
    myPrintln(s"${rightPad(db1, maxLen)} taken time: ${formatMillis(db1Time)}")
    myPrintln(s"${rightPad(db2, maxLen)} taken time: ${formatMillis(db2Time)}")
    myPrintln(s"$db2 vs $db1 = ${ formatFloat(db2Time.toFloat / db1Time) } : 1\n")
  }

  def showDetails(db1: String,
      db1Times: Seq[(String, Long)],
      db2: String,
      db2Times: Seq[(String, Long)]): Unit = {
    val map = new mutable.HashMap[String, (Long, Long, Float)]()
    db1Times.foreach{ case (file, time) =>
      val info = map.get(file)
      if (info.isEmpty) {
        map(file) = (time, 0L, 0f)
      } else {
        map(file) = info.get.copy(_1 = time)
      }
    }
    db2Times.foreach { case (file, time) =>
      val info = map.get(file)
      if (info.isEmpty) {
        map(file) = (0L, time, 0f)
      } else {
        val (v1, _, v3) = info.get
        val newV3 = if (v1 != 0) {
          time.toFloat / v1
        } else {
          v3
        }
        map(file) = info.get.copy(_2 = time, _3 = newV3)
      }
    }
    val head = Seq(Seq("file name", db1, db2, s"$db2/$db1"))
    val body = map.map { case (key, value) =>
      Seq(key, formatMillis(value._1), formatMillis(value._2), formatFloat(value._3))
    }.toSeq.sortBy(_.head)
    show((head ++ body).toArray, 4)
  }

  def showSummary(
      db: String,
      dbTimes: Seq[(String, Long)]): Unit = {
    val dbTime = dbTimes.map(_._2).sum
    myPrintln(s"$db taken time: ${ formatMillis(dbTime) }")
  }

  def showDetails(db: String, dbTimes: Seq[(String, Long)]): Unit = {
    val head = Seq(Seq("file name", db))
    val body = dbTimes.map { case (key, value) =>
      Seq(key, formatMillis(value))
    }.sortBy(_.head)
    show((head ++ body).toArray, 2)
  }
}
