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

package org.apache.spark.sql.execution.command.management

import java.io.IOException
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, ExecutorService}

import org.apache.hadoop.conf.Configuration
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.command.{Checker, MetadataCommand}
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * collect loads from load_details folder
 */
case class CarbonCollectLoadsCommand(
    databaseNameOp: Option[String],
    tableName: String
) extends MetadataCommand {

  @transient var LOGGER: Logger = _


  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    Checker.validateTableExists(databaseNameOp, tableName, sparkSession)
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    FileFactory.getConfiguration.addResource(hadoopConf)
    setAuditTable(carbonTable)
    if (!carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }

    val lock = acquiredTableLock(carbonTable)
    // if insert overwrite in progress, do not allow add segment
    if (SegmentStatusManager.isOverwriteInProgressInTable(carbonTable)) {
      throw new ConcurrentOperationException(carbonTable, "insert overwrite", "collect segments")
    }
    val tablePath = carbonTable.getTablePath()
    val timestamp = System.currentTimeMillis()
    val loadDetailsHistoryDir = CarbonTablePath.getLoadDetailsHistoryDir(tablePath) +
                                CarbonCommonConstants.FILE_SEPARATOR + timestamp
    val backupFolder = FileFactory.getCarbonFile(loadDetailsHistoryDir, hadoopConf)
    if (!backupFolder.exists()) {
      backupFolder.mkdirs(loadDetailsHistoryDir)
    }
    try {
      val loadDetailsDir = CarbonTablePath.getLoadDetailsDir(tablePath)
      val detailFiles = listLoadDetails(loadDetailsDir, hadoopConf)
      if (detailFiles.isEmpty) {
        LOGGER.warn("files not found under load_details")
      } else {
        val numThreads = Math.min(Math.max(detailFiles.length, 1), 10)
        val executorService = Executors.newFixedThreadPool(numThreads)
        val detailList = Collections.synchronizedList(new util.ArrayList[LoadMetadataDetails]())
        // 1.read load_details dir
        readLoadDetailDir(executorService, loadDetailsHistoryDir, detailFiles)
        // 2.backup load_details dir to load_details_history dir
        // 3.update tablestatus
        // 4.delete load_detals dir
      }
      Seq.empty
    } catch {
      case ex =>
        LOGGER.error("failed to collect segments, timestamp:" + timestamp)
        throw ex
    } finally {
      lock.unlock()
    }
  }

  def readLoadDetailDir(
      executorService: ExecutorService,
      loadDetailsHistoryDir: String,
      detailFiles: Array[(CarbonFile, CarbonFile)]): Unit = {
    val startTime= System.currentTimeMillis()
    detailFiles.map { detail =>
      executorService.submit(

      )
    }
  }


  def listLoadDetails(
      loadDetailsDir: String,
      hadoopConf: Configuration
  ): Array[(CarbonFile, CarbonFile)] = {
    val dir = FileFactory.getCarbonFile(loadDetailsDir, hadoopConf)
    if (dir.exists()) {
      val allFiles = dir.listFiles()
      val successFiles = allFiles.filter { file =>
        file.getName.endsWith(CarbonTablePath.LOAD_DETAILS_SUBFIX)
      }.map { file =>
        (file.getName.substring(0, file.getName.indexOf(".")), file)
      }.toMap
      allFiles.filter { file =>
        !file.getName.endsWith(CarbonTablePath.LOAD_DETAILS_SUBFIX)
      }.filter { file =>
        successFiles.contains(file.getName)
      }.map { file =>
        (file, successFiles.get(file.getName).get)
      }
    } else {
      Array.empty
    }
  }

  private def acquiredTableLock(table: CarbonTable): ICarbonLock = {
    val tableIdentifier = table.getAbsoluteTableIdentifier
    val lock =
      CarbonLockFactory.getCarbonLockObj(tableIdentifier, "table_collect_segments.lock")
    val retryCount = CarbonLockUtil.getLockProperty(
      CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK,
      CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK_DEFAULT
    )
    val maxTimeout = CarbonLockUtil.getLockProperty(
      CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
      CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT
    )
    if (lock.lockWithRetries(retryCount, maxTimeout)) {
      lock
    } else {
      throw new IOException(
        s"Not able to acquire the lock for Table status updation for table $tableIdentifier")
    }
  }

  override protected def opName: String = "COLLECT SEGMENTS"
}
