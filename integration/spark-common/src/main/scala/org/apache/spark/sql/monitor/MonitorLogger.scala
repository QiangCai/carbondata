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

package org.apache.spark.sql.monitor

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Comparator

import scala.collection.mutable.ArrayBuffer

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.stats.TaskStatistics
import org.apache.carbondata.core.util.CarbonUtil

object MonitorLogger {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  lazy val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def format(time: Long): String = this.synchronized {
    if (time < 0) {
      ""
    } else {
      val timestamp = new Timestamp(time)
      simpleDateFormat.format(timestamp)
    }
  }

  def logQuerySummary(statementId: Long, messages: ArrayBuffer[MonitorMessage]): Unit = {
    LOGGER.info(new StatementSummary(statementId, messages).toString)
  }

  def logExecutionSummary(executionId: Long, messages: ArrayBuffer[MonitorMessage]): Unit = {
    LOGGER.info(new ExecutionSummary(executionId, messages).toString)
  }
}

class StatementSummary(statementId: Long, messages: ArrayBuffer[MonitorMessage]) {

  private var sqlText: String = ""

  private var isCommand = false

  private var startTime: Long = -1

  private var parserEnd: Long = -1

  private var analyzerEnd: Long = -1

  private var optimizerStart: Long = -1

  private var optimizerEnd: Long = -1

  private var optimizerTaken: Long = -1

  private var endTime: Long = -1

  messages.foreach {
    case sqlStart: SQLStart =>
      sqlText = sqlStart.sqlText.trim
      isCommand = sqlStart.isCommand
      startTime = sqlStart.startTime
      parserEnd = sqlStart.parseEnd
      analyzerEnd = sqlStart.analyzerEnd
      endTime = sqlStart.endTime
    case optimizer: Optimizer =>
      optimizerTaken = optimizer.timeTaken
      optimizerStart = optimizer.startTime
      optimizerEnd = optimizerStart + optimizerTaken
  }

  private def totalTaken: Long = endTime - startTime

  private def parserTaken: Long = parserEnd - startTime

  private def analyzerTaken: Long = analyzerEnd - parserEnd

  private def parserToOptimizer: Long = optimizerEnd - startTime

  private def commandTaken: Long = endTime - analyzerEnd

  override def toString: String = {
    if (isCommand) {
      buildForComand()
    } else {
      buildForQuery()
    }
  }

  private def buildForComand(): String = {
    val builder = new java.lang.StringBuilder(1000)
    builder.append(s"\n[statement id]: ${ statementId }")
    builder.append(s"\n[sql text]:\n")
    CarbonUtil.logTable(builder, sqlText, "")
    builder.append(s"\n[start time]: ${ MonitorLogger.format(startTime) }\n")
    builder.append(s"[total taken]: $totalTaken ms\n")
    builder.append(s"  |__ 1.parser taken: $parserTaken ms\n")
    builder.append(s"  |__ 2.analyzer taken: $analyzerTaken ms\n")
    builder.append(s"  |__ 3.execution taken: $commandTaken ms")
    builder.toString
  }

  private def buildForQuery(): String = {
    val builder = new java.lang.StringBuilder(1000)
    builder.append(s"\n[statement id]: ${ statementId }")
    builder.append(s"\n[sql text]:\n")
    CarbonUtil.logTable(builder, sqlText, "")
    builder.append(s"\n[start time]: ${ MonitorLogger.format(startTime) }\n")
    builder.append(s"[parser ~ optimizer taken]: ${ parserToOptimizer }ms\n")
    builder.append(s"  |__ 1.parser taken: $parserTaken ms\n")
    builder.append(s"  |__ 2.analyzer taken: $analyzerTaken ms\n")
    builder.append(s"  |__ 3.(${ optimizerStart - analyzerEnd }ms)\n")
    builder.append(s"  |__ 4.carbon optimizer taken: $optimizerTaken ms\n")
    builder.append(s"        end time: ${ MonitorLogger.format(optimizerEnd) }")
    builder.toString
  }
}

class ExecutionSummary(executionId: Long, messages: ArrayBuffer[MonitorMessage]) {

  private var sqlPlan: String = ""

  private var startTime: Long = -1

  private var endTime: Long = -1

  private val partitions = new util.ArrayList[GetPartition]()

  private val tasks = new util.ArrayList[TaskStatistics]()

  messages.foreach {
    case start: ExecutionStart =>
      sqlPlan = start.plan
      startTime = start.startTime
    case end: ExecutionEnd =>
      endTime = end.endTime
    case partition: GetPartition =>
      partitions.add(partition)
    case task: QueryTaskEnd =>
      tasks.add(new TaskStatistics(task.queryId, task.values))
  }

  private def totalTaken: Long = endTime - startTime

  private def detail(p: GetPartition): String = {
    val builder = new java.lang.StringBuilder(128)
    builder.append("(prepare inputFormat ").append(p.getSplitsStart - p.startTime).append("ms)~")
    builder.append("(getSplits ").append(p.getSplitsEnd - p.getSplitsStart).append("ms)~")
    val gap1 = p.distributeStart - p.getSplitsEnd
    if (gap1 > 0) {
      builder.append("(").append(gap1).append("ms)~")
    }
    builder.append("(distributeSplits").append(p.distributeEnd - p.distributeStart).append("ms)")
    val gap2 = p.endTime - p.distributeEnd
    if (gap2 > 0) {
      builder.append("~(").append(gap2).append("ms)")
    }
    builder.toString
  }

  private def printGetPartitionTable(builder: java.lang.StringBuilder, indent: String): Unit = {
    val header = Array(
      "query_id",
      "table_name",
      "start_time",
      "total_time",
      "partition_nums",
      "filter",
      "projection")
    util.Collections.sort(partitions)
    val rows = new Array[Array[String]](partitions.size())
    for (rowIndex <- 0 until partitions.size()) {
      val partition = partitions.get(rowIndex)
      rows(rowIndex) = Array(
        partition.queryId,
        partition.tableName,
        MonitorLogger.format(partition.startTime),
        (partition.endTime - partition.startTime).toString + "ms[" + detail(partition) + "]",
        partition.numOfPartitions.toString,
        partition.filter,
        partition.projection
      )
    }
    CarbonUtil.logTable(builder, header, rows, indent)
  }

  private lazy val comparator = new Comparator[TaskStatistics]() {
    override def compare(o1: TaskStatistics,
        o2: TaskStatistics) = {
      val result = o1.getQueryId.compareTo(o2.getQueryId())
      if (result != 0) {
        result
      } else {
        val task = o1.getValues()(1) - o2.getValues()(1)
        if (task > 0) {
          1
        } else if (task < 0) {
          -1
        } else {
          0
        }
      }
    }
  }

  override def toString: String = {
    val builder = new java.lang.StringBuilder(1000)
    builder.append(s"\n[execution id]: ${ executionId }\n")
    builder.append(s"[start time]: ${ MonitorLogger.format(startTime) }\n")
    builder.append(s"[total taken]: $totalTaken ms")
    builder.append(s"\n  |_1.getPartition\n")
    printGetPartitionTable(builder, "    ")
    builder.append(s"\n  |_2.task statistics\n")
    util.Collections.sort(tasks, comparator)
    TaskStatistics.printStatisticTable(tasks, builder, "    ")
    builder.append(s"\n[sql plan]:\n")
    CarbonUtil.logTable(builder, sqlPlan, "")
    builder.toString
  }
}
