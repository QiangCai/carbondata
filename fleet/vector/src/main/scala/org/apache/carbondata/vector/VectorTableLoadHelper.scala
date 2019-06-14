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

package org.apache.carbondata.vector

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.vector.table.VectorTableWriter

/**
 * load util for vector table
 */
object VectorTableLoadHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * submit job to insert data into table
   * @param sqlContext
   * @param dataFrame
   * @param carbonLoadModel
   * @param hadoopConf
   * @return
   */
  def loadDataFrameForVector(
      sqlContext: SQLContext,
      dataFrame: Option[DataFrame],
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    val serializedConf = SparkSQLUtil.getSerializableConfigurableInstance(hadoopConf)
    val rt = dataFrame
      .get
      .rdd
      .repartition(1)
      .mapPartitions { iter =>
        writeRows(iter, carbonLoadModel, serializedConf.value)
      }
      .collect()

    rt
  }

  /**
   * implement load task
   * @param iter
   * @param carbonLoadModel
   * @param hadoopConf
   * @return
   */
  private def writeRows(
      iter: Iterator[Row],
      carbonLoadModel: CarbonLoadModel,
      hadoopConf: Configuration
  ): Iterator[(String, (LoadMetadataDetails, ExecutionErrors))] = {

    val loadMetadataDetails = new LoadMetadataDetails()
    loadMetadataDetails.setPartitionCount(CarbonTablePath.DEPRECATED_PARTITION_ID)
    loadMetadataDetails.setFileFormat(FileFormat.VECTOR_V1)
    val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
    val tableWriter = new VectorTableWriter(carbonLoadModel, hadoopConf)
    try {
      iter.foreach { row =>
        tableWriter.write(row.toSeq.toArray[Any].asInstanceOf[Array[Object]])
      }
    } catch {
      case e: Exception =>
        loadMetadataDetails.setSegmentStatus(SegmentStatus.LOAD_FAILURE)
        executionErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
        executionErrors.errorMsg = e.getMessage
        LOGGER.error("Failed to write rows", e)
        throw e
    } finally {
      tableWriter.close()
    }

    loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS)
    Iterator(("0", (loadMetadataDetails, executionErrors)))
  }

}
