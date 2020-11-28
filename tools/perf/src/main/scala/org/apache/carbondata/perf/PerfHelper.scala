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
    sqlListWithFileName
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

  // resultDetails: Seq((database, Seq((fileName, time))))
  def showSummaryContest(resultDetails: Seq[(String, Seq[(String, Long)])]): Unit = {
    val dbTimes = resultDetails.map { databaseDetail =>
      (databaseDetail._1, databaseDetail._2.map(_._2).sum)
    }
    val maxLen = resultDetails.map(_._1.length).max
    val timePercent = dbTimes.zipWithIndex.map { case (dbTime, index) =>
      myPrintln(s"${ rightPad(dbTime._1, maxLen) } taken time: ${ formatMillis(dbTime._2) }")
      if (index == 0) {
        "1"
      } else {
        formatFloat(dbTime._2.toFloat / dbTimes.head._2)
      }
    }
    myPrintln(dbTimes.map(_._1).mkString(" vs ") + " = "  + timePercent.mkString(" : "))
    myPrintln("")
  }

  // resultDetails: Seq((database, Seq((fileName, time))))
  def showDetailContest(resultDetails: Seq[(String, Seq[(String, Long)])]): Unit = {

    val numDatabases = resultDetails.size
    val map = new mutable.HashMap[String, (Array[Long], Array[Float])]()

    def initValues(file: String, time: Long, index: Int): Unit = {
      val times = new Array[Long](numDatabases)
      val percent = new Array[Float](numDatabases - 1)
      times(index) = time
      map(file) = (times, percent)
    }

    resultDetails.head._2.foreach{ case (file, time) =>
      val info = map.get(file)
      if (info.isEmpty) {
        initValues(file, time, 0)
      } else {
        info.get._1(0) = time
      }
    }

    resultDetails.zipWithIndex.tail.foreach { case ((_, files), index) =>
      files.foreach { case (file, time) =>
        val info = map.get(file)
        if (info.isEmpty) {
          initValues(file, time, 1)
        } else {
          val (times, percents) = info.get
          times(index) = time
          val baseTime = times(0)
          if (baseTime != 0) {
            percents(index - 1) = time.toFloat / baseTime
          }
        }
      }
    }

    val numColumns = resultDetails.length + 2
    val headItems = new ArrayBuffer[String](numColumns)
    headItems += "file name"
    val result = resultDetails.map { case (db, _) =>
      headItems += db
      db
    }.mkString(" : ")
    headItems += result

    val head: Seq[Seq[String]] = Seq(headItems)
    val body: Seq[Seq[String]] = map.map { case (file, (times, percents)) =>
      val bodyItems = new ArrayBuffer[String](numColumns)
      bodyItems += file
      times.map { time =>
        bodyItems += formatMillis(time)
      }
      bodyItems += "1 : " + percents.map(value => formatFloat(value)).mkString(" : ")
      bodyItems
    }.toSeq.sortBy(_.head)
    show((head ++ body).toArray, numColumns)
  }

  def showSummary(
      db: String,
      dbTimes: Seq[(String, Long)]): Unit = {
    val dbTime = dbTimes.map(_._2).sum
    myPrintln(s"$db taken time: ${ formatMillis(dbTime) }")
  }

  def showDetail(db: String, dbTimes: Seq[(String, Long)]): Unit = {
    val head = Seq(Seq("file name", db))
    val body = dbTimes.map { case (key, value) =>
      Seq(key, formatMillis(value))
    }.sortBy(_.head)
    show((head ++ body).toArray, 2)
  }
}
