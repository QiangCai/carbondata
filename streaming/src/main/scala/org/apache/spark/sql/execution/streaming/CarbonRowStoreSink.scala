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

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.{CarbonStorePath, CarbonTablePath}
import org.apache.carbondata.streaming.CarbonStreamException

class CarbonRowStoreSink(
    sparkSession: SparkSession,
    carbonTable: CarbonTable,
    parameters: Map[String, String]) extends Sink {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private val carbonTablePath = CarbonStorePath
    .getCarbonTablePath(carbonTable.getAbsoluteTableIdentifier)
  private val currentSegmentId = getStreamingSegmentId
  private val fileLogPath = carbonTablePath.getStreamingLogDir
  private val fileLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, sparkSession, fileLogPath)
  private val hadoopConf = sparkSession.sessionState.newHadoopConf()

  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    if (batchId <= fileLog.getLatest().map(_._1).getOrElse(-1L)) {
      LOGGER.info(s"Skipping already committed batch $batchId")
    } else {

      validateSchema(data.schema)
      validateSegment(carbonTablePath)

      val committer = FileCommitProtocol.instantiate(
        className = sparkSession.sessionState.conf.streamingFileCommitProtocolClass,
        jobId = batchId.toString,
        outputPath = fileLogPath,
        isAppend = false)

      committer match {
        case manifestCommitter: ManifestFileCommitProtocol =>
          manifestCommitter.setupManifestOptions(fileLog, batchId)
        case _ => // Do nothing
      }

      CarbonStreamProcessor.writeDataFileJob(
        sparkSession,
        carbonTable,
        parameters,
        batchId,
        currentSegmentId,
        data.queryExecution,
        committer,
        hadoopConf)
    }
  }

  private def validateSegment(carbonTablePath: CarbonTablePath): Unit = {
    val segmentDir = carbonTablePath.getSegmentDir("0", currentSegmentId)
    val streamingTempDir = carbonTablePath.getStreamingTempDir(segmentDir)
    // TODO fault tolerant

    if (FileFactory.isFileExist(streamingTempDir, FileFactory.getFileType(streamingTempDir))) {
      throw new CarbonStreamException(s"Require to recover streaming segment $currentSegmentId" +
                                      s" at first.")
    }
  }

  private def validateSchema(dataSchema: StructType): Unit = {
    // TODO check whether source schema is same with target table
  }

  private def getStreamingSegmentId: String = {
    // TODO get streaming segment id
    "0"
  }
}