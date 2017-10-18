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

package org.apache.carbondata.streaming.format

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SparkSqlUtil}
import org.apache.spark.sql.execution.streaming.{FileStreamSinkLog, ManifestFileCommitProtocol, Sink}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.streaming.{CarbonStreamingException, CarbonStreamingUtil}

class CarbonRowStoreSink(sparkSession: SparkSession,
    tablePath: String) extends Sink {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private val identifier = AbsoluteTableIdentifier.fromTablePath(tablePath)
  private val carbonTablePath = CarbonStorePath.getCarbonTablePath(identifier)
  private val currentSegmentId = "0"
  private val fileLogPath = tablePath + "/streaming"
  private val fileLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, fileLogPath)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      LOGGER.info(s"Skipping already committed batch $batchId")
    } else {

      validateSegment(carbonTablePath, currentSegmentId)

      val committer = FileCommitProtocol.instantiate(
        className = SparkSqlUtil.streamingFileCommitProtocolClass(sparkSession),
        jobId = batchId.toString,
        outputPath = fileLogPath,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      // step 1: write batch data to temporary folder
      val result = data.mapPartitions { iterator =>
        val partitionindex = TaskContext.getPartitionId()
        CarbonStreamingUtil
          .generateDataFile(batchId, tablePath, currentSegmentId, partitionindex, iterator)
      }.collect()

      // TODO check result

      // step 2: append temporary data to streaming segment
      CarbonStreamingUtil.appendDataFileToStreamingFile()

    }

  }

  private def validateSegment(carbonTablePath: CarbonTablePath, currentSegmentId: String): Unit = {
    val segmentDir = carbonTablePath.getSegmentDir("0", currentSegmentId)
    val streamingTempDir = carbonTablePath.getStreamingTempDir(segmentDir)
    if

    if (FileFactory.isFileExist(streamingTempDir, FileFactory.getFileType(streamingTempDir))) {
      throw new CarbonStreamingException(s"Require to recover streaming segment $currentSegmentId" +
                                         s" at first.")
    }
  }
}