package org.apache.carbondata.vector

import org.apache.spark.sql.execution.command.ExecutionErrors
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.vector.table.VectorTableWriter

object VectorTableHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def loadDataFrameForVector(
    sqlContext: SQLContext,
    dataFrame: Option[DataFrame],
    carbonLoadModel: CarbonLoadModel
  ): Array[(String, (LoadMetadataDetails, ExecutionErrors))] = {
    val rt = dataFrame
      .get
      .rdd
      .repartition(1)
      .mapPartitions { iter =>
        writeRows(iter, carbonLoadModel)
      }
      .collect()

    rt
  }

  private def writeRows(
    iter: Iterator[Row],
    carbonLoadModel: CarbonLoadModel
  ): Iterator[(String, (LoadMetadataDetails, ExecutionErrors))] = {

    val loadMetadataDetails = new LoadMetadataDetails()
    loadMetadataDetails.setPartitionCount(CarbonTablePath.DEPRECATED_PARTITION_ID)
    loadMetadataDetails.setFileFormat(FileFormat.VECTOR_V1)
    val executionErrors = new ExecutionErrors(FailureCauses.NONE, "")
    val tableWriter = new VectorTableWriter(carbonLoadModel, FileFactory.getConfiguration)
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
