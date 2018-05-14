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

package org.apache.carbondata.spark.rdd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{RecordReader, TaskAttemptID, TaskType}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecution

import org.apache.carbondata.ai.model.Model
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.util.{CarbonTimeStatisticsFactory, TaskMetricsMap}
import org.apache.carbondata.hadoop.{CarbonProjection, CarbonRecordReader}
import org.apache.carbondata.spark.InitInputMetrics

class PushDownUdfRDD(
    @transient spark: SparkSession,
    columnProjection: CarbonProjection,
    filterExpression: Expression,
    identifier: AbsoluteTableIdentifier,
    @transient serializedTableInfo: Array[Byte],
    @transient tableInfo: TableInfo,
    inputMetricsStats: InitInputMetrics,
    aiModel: Model
) extends CarbonScanRDD[InternalRow](spark,
  columnProjection,
  filterExpression,
  identifier,
  serializedTableInfo,
  tableInfo,
  inputMetricsStats,
  null) {

  override def internalCompute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val queryStartTime = System.currentTimeMillis
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
      )
    }
    val executionId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val taskId = split.index
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val format = prepareInputFormatForExecutor(attemptContext.getConfiguration)
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    TaskMetricsMap.getInstance().registerThreadCallback()
    inputMetricsStats.initBytesReadCallback(context, inputSplit)
    val iterator = if (inputSplit.getAllSplits.size() > 0) {
      val model = format.createQueryModel(inputSplit, attemptContext)
      // one query id per table
      model.setQueryId(queryId)
      model.setVectorReader(false)
      model.setForcedDetailRawQuery(false)
      model.setRequiredRowId(false)
      model.setAiModel(aiModel)
      // get RecordReader by FileFormat
      var reader: RecordReader[Void, Object] =
        new CarbonRecordReader(model,
          format.getReadSupportClass(attemptContext.getConfiguration),
          inputMetricsStats)

      val closeReader = () => {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
          }
          reader = null
        }
      }

      // add task completion before calling initialize as initialize method will internally call
      // for usage of unsafe method for processing of one blocklet and if there is any exception
      // while doing that the unsafe memory occupied for that task will not get cleared
      context.addTaskCompletionListener { _ =>
        closeReader.apply()
        close()
        logStatistics(executionId, taskId, queryStartTime, model.getStatisticsRecorder, split)
      }
      // create a statistics recorder
      val recorder = CarbonTimeStatisticsFactory.createExecutorRecorder(model.getQueryId())
      model.setStatisticsRecorder(recorder)
      // initialize the reader
      reader.initialize(inputSplit, attemptContext)

      new Iterator[Any] {
        private var havePair = false
        private var finished = false

        override def hasNext: Boolean = {
          if (context.isInterrupted) {
            throw new TaskKilledException
          }
          if (!finished && !havePair) {
            finished = !reader.nextKeyValue
            havePair = !finished
          }
          if (finished) {
            closeReader.apply()
          }
          !finished
        }

        override def next(): Any = {
          if (!hasNext) {
            throw new java.util.NoSuchElementException("End of stream")
          }
          havePair = false
          val value = reader.getCurrentValue
          value
        }

      }
    } else {
      new Iterator[Any] {
        override def hasNext: Boolean = false

        override def next(): Any = throw new java.util.NoSuchElementException("End of stream")
      }
    }

    iterator.asInstanceOf[Iterator[InternalRow]]
  }

}
