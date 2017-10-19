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

package org.apache.spark.sql.execution.streaming

import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.spark.{SparkHadoopWriter, TaskContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{SerializableConfiguration, Utils}

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.file.mapreduce.CarbonRowStoreOutputFormat
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.carbondata.streaming.{CarbonStreamException, DataWriterTaskExecutor}

object CarbonStreamProcessor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  case class WriteDataFileJobDescription(
      serializableHadoopConf: SerializableConfiguration,
      batchId: Long,
      segmentId: String)

  def writeDataFileJob(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      parameters: Map[String, String],
      batchId: Long,
      segmentId: String,
      queryExecution: QueryExecution,
      committer: FileCommitProtocol,
      hadoopConf: Configuration): Unit = {

    // prepare configuration for executor
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val optionsFinal = CommonUtil.getFinalOptions(carbonProperty, parameters)
    val carbonLoadModel = CommonUtil.buildCarbonLoadModel(
      carbonTable,
      carbonProperty,
      parameters,
      optionsFinal
    )
    CarbonRowStoreOutputFormat.setCarbonLoadModel(hadoopConf, carbonLoadModel)
    // create job
    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    job.setOutputValueClass(classOf[InternalRow])

    val description = WriteDataFileJobDescription(
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      batchId,
      segmentId
    )

    // run write data file job
    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      var result: Array[TaskCommitMessage] = null
      try {
        committer.setupJob(job)
        result = sparkSession.sparkContext.runJob(queryExecution.toRdd,
          (taskContext: TaskContext, iterator: Iterator[InternalRow]) => {
            writeDataFileTask(
              description,
              sparkStageId = taskContext.stageId(),
              sparkPartitionId = taskContext.partitionId(),
              sparkAttemptNumber = taskContext.attemptNumber(),
              committer,
              iterator
            )
          })
      } catch {
        // catch fault of executor side
        case t: Throwable =>
          CarbonStreamProcessor.deleteTempFolder()
          LOGGER.error(t, s"Aborting job ${ job.getJobID }.")
          committer.abortJob(job)
          throw new CarbonStreamException("Task failed to write temporary file", t)
      }
      committer.commitJob(job, result)
      LOGGER.info(s"Job ${ job.getJobID } committed.")
    }
  }

  def writeDataFileTask(
      description: WriteDataFileJobDescription,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[InternalRow]
  ): TaskCommitMessage = {

    val jobId = SparkHadoopWriter.createJobID(new Date, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapred.job.id", jobId.toString)
      hadoopConf.set("mapred.tip.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapred.task.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapred.task.is.map", true)
      hadoopConf.setInt("mapred.task.partition", 0)
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        new DataWriterTaskExecutor().execute(new InputIterator(iterator), taskAttemptContext)
      })(catchBlock = {
        committer.abortTask(taskAttemptContext)
        LOGGER.error(s"Job $jobId aborted.")
      })
      committer.commitTask(taskAttemptContext)
    } catch {
      case t: Throwable =>
        throw new CarbonStreamException("Task failed while writing rows", t)
    }
  }

  class InputIterator(rddIter: Iterator[InternalRow]) extends CarbonIterator[Array[String]] {
    def hasNext: Boolean = rddIter.hasNext
    def next: Array[String] = {
      val row = rddIter.next()
      val columns = new Array[String](row.numFields)
      for (i <- 0 until columns.length) {
        columns(i) = row.getString(i)
      }
      columns
    }
  }

  def appendSegmentJob(): Unit = ???


  def createTempFolder() = ???

  def deleteTempFolder() = ???
}
