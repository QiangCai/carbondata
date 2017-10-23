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

import java.io._
import java.nio.charset.Charset
import java.util.Date

import scala.collection.mutable.ArrayBuffer

import com.google.gson.Gson
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
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.fileoperations.{AtomicFileOperationsImpl, FileWriteOperation}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.file.mapreduce.CarbonRowStoreOutputFormat
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.segment.RowStoreSegmentOutputFormat.AppendPlan
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.carbondata.streaming.{CarbonStreamException, DataWriterTaskExecutor}

object CarbonStreamProcessor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private val SMALL_FILE_THRESHOLD_DEFAULT = 32L * 1024 * 1024

  case class WriteDataFileJobDescription(
      serializableHadoopConf: SerializableConfiguration,
      batchId: Long,
      segmentId: String)

  /**
   * run a job to write data files to temporary folder
   *
   * @param sparkSession
   * @param carbonTable
   * @param parameters
   * @param batchId
   * @param segmentId
   * @param queryExecution
   * @param committer
   * @param hadoopConf
   */
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
          val tablePath =
            CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
          CarbonLoaderUtil.deleteStorePath(tablePath.getStreamingTempDir("0", segmentId))
          LOGGER.error(t, s"Aborting job ${ job.getJobID }.")
          committer.abortJob(job)
          throw new CarbonStreamException("Task failed to write temporary file", t)
      }
      committer.commitJob(job, result)
      LOGGER.info(s"Job ${ job.getJobID } committed.")
    }
  }

  /**
   * execute a task for each partition to write a data file
   *
   * @param description
   * @param sparkStageId
   * @param sparkPartitionId
   * @param sparkAttemptNumber
   * @param committer
   * @param iterator
   * @return
   */
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

  /**
   * convert spark iterator to carbon iterator, so that java module can use it.
   *
   * @param rddIter
   */
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

  /**
   * append data files in temporary folder to the current streaming segment
   * if the file is small, it will be append to another small file
   *
   * @param sparkSession
   * @param carbonTable
   * @param currentSegmentId
   * @param committer
   */
  def appendSegmentJob(
      sparkSession: SparkSession,
      carbonTable: CarbonTable,
      currentSegmentId: String,
      committer: FileCommitProtocol
  ): Unit = {
    val appendPlans = refreshAppendPlan(carbonTable, currentSegmentId)


  }

  // generate AppendPlan
  private def refreshAppendPlan(
      carbonTable: CarbonTable,
      currentSegmentId: String): Array[AppendPlan] = {
    val tablePath = CarbonStorePath.getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
    val segmentDir = tablePath.getSegmentDir("0", currentSegmentId)
    val tempDir = tablePath.getStreamingTempDir(segmentDir)
    val planFilePath = tablePath.getStreamingAppendPlanFile(tempDir)
    val planFile = FileFactory.getCarbonFile(planFilePath, FileFactory.getFileType(planFilePath))
    if (planFile.exists()) {
      // refresh target size
      val plans = readPlanFile(planFilePath)
      plans.foreach { plan =>
        val file = FileFactory.getCarbonFile(segmentDir + File.separator + plan.getTargetFile)
        if (file.exists()) {
          plan.setTargetFileSizeNew(file.getSize)
        } else {
          plan.setTargetFileSize(0)
          plan.setTargetFileSizeNew(0)
        }
      }
      plans
    } else {
      // TODO better to config it in Cofiguration
      val thresholdSize = SMALL_FILE_THRESHOLD_DEFAULT
      val sourceFiles = listCarbonFiles(tempDir)
      val targetFiles = listCarbonFiles(segmentDir).filter(_.getSize < thresholdSize)
        .sortBy(_.getSize)

      if (sourceFiles.nonEmpty) {
        val plans = if (targetFiles.isEmpty) {
          // directly move to streaming segment
          sourceFiles.map { file =>
            new AppendPlan(file.getName, file.getSize)
          }
        } else {
          // move big files directly
          val plan1 = sourceFiles.filter(_.getSize >= thresholdSize).map { file =>
            new AppendPlan(file.getName, file.getSize)
          }
          val smallFiles =  sourceFiles.filter(_.getSize < thresholdSize).sortBy(_.getSize)
          // append small file to big file
          val plan2 = smallFiles.zipWithIndex.map { file =>
            if (file._2 < targetFiles.length) {
              val targetFile = targetFiles(targetFiles.length - 1 - file._2)
              new AppendPlan(file._1.getName, file._1.getSize,
                targetFile.getName, targetFile.getSize)
            } else {
              new AppendPlan(file._1.getName, file._1.getSize)
            }
          }
          // combine plan
          if (plan1.isEmpty) {
            plan2
          } else if (plan2.isEmpty) {
            plan1
          } else {
            val buffer = new ArrayBuffer[AppendPlan](plan1.length + plan2.length)
            buffer.appendAll(plan1)
            buffer.appendAll(plan2)
            buffer.toArray
          }
        }
        writePlanFile(planFilePath, plans)
        plans
      } else {
        Array.empty[AppendPlan]
      }
    }

  }

  private def writePlanFile(planFilePath: String, plans: Array[AppendPlan]): Unit = {
    val planString = new Gson().toJson(plans)
    var oper: AtomicFileOperationsImpl = null
    var outputStream: DataOutputStream = null
    var brWriter: BufferedWriter = null
    try {
      oper = new AtomicFileOperationsImpl(planFilePath, FileFactory.getFileType(planFilePath))
      outputStream = oper.openForWrite(FileWriteOperation.OVERWRITE)
      brWriter = new BufferedWriter(new OutputStreamWriter(outputStream,
        Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)))
      brWriter.write(planString)
      brWriter.flush()
    } finally {
      CarbonUtil.closeStreams(brWriter)
      oper.close()
    }
  }

  private def readPlanFile(planFilePath: String): Array[AppendPlan] = {
    var inputStream: DataInputStream = null
    var reader: InputStreamReader = null
    var buffReader: BufferedReader = null
    try {
      inputStream =
        FileFactory.getDataInputStream(planFilePath, FileFactory.getFileType(planFilePath))
      reader = new InputStreamReader(inputStream,
        Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
      buffReader = new BufferedReader(reader)
      new Gson().fromJson(buffReader, classOf[Array[AppendPlan]])
    } finally {
      CarbonUtil.closeStreams(buffReader)
    }
  }

  /**
   * list all .carbondata files under the the path
   *
   * @param path
   * @return
   */
  private def listCarbonFiles(path: String): Array[CarbonFile] = {
    val carbonDir = FileFactory.getCarbonFile(path, FileFactory.getFileType(path))
    if (carbonDir.exists()) {
      carbonDir.listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          CarbonTablePath.isCarbonDataFile(file.getName)
        }
      })
    } else {
      Array.empty[CarbonFile]
    }
  }
}
