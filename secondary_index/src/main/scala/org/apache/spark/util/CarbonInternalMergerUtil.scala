/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */

package org.apache.spark.util

import java.util
import java.util.{Collections, Comparator, List}

import scala.collection.JavaConverters._

import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.MergeResultImpl
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.rdd.CarbonSIRebuildRDD


object CarbonInternalMergerUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Merge the data files of the SI segments in case of small files
   *
   * @param segmentIdToLoadStartTimeMapping
   * @param indexCarbonTable
   * @param loadsToMerge
   * @param carbonLoadModel
   * @param sqlContext
   * @return
   */
  def mergeDataFilesSISegments(segmentIdToLoadStartTimeMapping: scala.collection.mutable
  .Map[String, java.lang.Long],
    indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails],
    carbonLoadModel: CarbonLoadModel,
    isRebuildCommand: Boolean = false)
    (sqlContext: SQLContext): Set[String] = {
    var rebuildSegmentProperty = false
    try {
      rebuildSegmentProperty = CarbonProperties.getInstance().getProperty(
        CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE,
        CarbonInternalCommonConstants.DEFAULT_CARBON_SI_SEGMENT_MERGE).toBoolean

    } catch {
      case _: Exception =>
        rebuildSegmentProperty = CarbonInternalCommonConstants.DEFAULT_CARBON_SI_SEGMENT_MERGE
          .toBoolean
    }
    try {
      // in case of manual rebuild command, no need to consider the carbon property
      if (rebuildSegmentProperty || isRebuildCommand) {
        scanSegmentsAndSubmitJob(segmentIdToLoadStartTimeMapping,
          indexCarbonTable,
          loadsToMerge, carbonLoadModel)(sqlContext)
      } else {
        Set.empty
      }
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  /**
   * This will submit the loads to be merged into the executor.
   */
  def scanSegmentsAndSubmitJob(
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
    indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails], carbonLoadModel: CarbonLoadModel)
    (sqlContext: SQLContext): Set[String] = {
    loadsToMerge.asScala.foreach { seg =>
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    if (loadsToMerge.isEmpty) {
      Set.empty
    } else {
      val compactionCallableModel = CompactionCallableModel(
        carbonLoadModel,
        indexCarbonTable,
        loadsToMerge,
        sqlContext,
        null,
        CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
          TableIdentifier(indexCarbonTable.getTableName,
            Some(indexCarbonTable.getDatabaseName))),
        null)
      triggerCompaction(compactionCallableModel, segmentIdToLoadStartTimeMapping)(sqlContext)
    }
  }

  /**
   * Get a new CarbonLoadModel
   *
   * @param indexCarbonTable
   * @param loadsToMerge
   * @param factTimeStamp
   * @return
   */
  def getCarbonLoadModel(indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails],
    factTimeStamp: Long,
    columnCompressor: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(indexCarbonTable))
    carbonLoadModel.setTableName(indexCarbonTable.getTableName)
    carbonLoadModel.setDatabaseName(indexCarbonTable.getDatabaseName)
    carbonLoadModel.setLoadMetadataDetails(loadsToMerge)
    carbonLoadModel.setTablePath(indexCarbonTable.getTablePath)
    carbonLoadModel.setFactTimeStamp(factTimeStamp)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel
  }

  private def triggerCompaction(compactionCallableModel: CompactionCallableModel,
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long])
    (sqlContext: SQLContext): Set[String] = {
    val indexCarbonTable = compactionCallableModel.carbonTable
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType
    val partitions = compactionCallableModel.currentPartitions
    val tablePath = indexCarbonTable.getTablePath
    val startTime = System.nanoTime()
    var finalMergeStatus = false
    val databaseName: String = indexCarbonTable.getDatabaseName
    val factTableName = indexCarbonTable.getTableName
    val validSegments: List[Segment] = CarbonDataMergerUtil
      .getValidSegments(compactionCallableModel.loadsToMerge)
    val mergedLoadName: String = ""
    val carbonMergerMapping = CarbonMergerMapping(
      tablePath,
      indexCarbonTable.getMetadataPath,
      mergedLoadName,
      databaseName,
      factTableName,
      validSegments.asScala.toArray,
      indexCarbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
      compactionType,
      maxSegmentColCardinality = null,
      maxSegmentColumnSchemaList = null,
      currentPartitions = partitions)
    carbonLoadModel.setTablePath(carbonMergerMapping.hdfsStoreLocation)
    carbonLoadModel.setLoadMetadataDetails(
      SegmentStatusManager.readLoadMetadata(indexCarbonTable.getMetadataPath).toList.asJava)

    val mergedSegments: util.Set[LoadMetadataDetails] = new util.HashSet[LoadMetadataDetails]()
    var rebuiltSegments: Set[String] = Set[String]()
    val segmentIdToLoadStartTimeMap: util.Map[String, String] = new util.HashMap()

    try {
      val mergeStatus =
        new CarbonSIRebuildRDD(
          sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping
        ).collect
      if (null != mergeStatus && mergeStatus.length == 0) {
        finalMergeStatus = true
      } else {
        finalMergeStatus = mergeStatus.forall(_._1._2)
        rebuiltSegments = mergeStatus.map(_._2).toSet
        compactionCallableModel.loadsToMerge.asScala.foreach(metadataDetails => {
          if (rebuiltSegments.contains(metadataDetails.getLoadName)) {
            mergedSegments.add(metadataDetails)
            segmentIdToLoadStartTimeMap
              .put(metadataDetails.getLoadName, String.valueOf(metadataDetails.getLoadStartTime))
          }
        })
      }
      if (finalMergeStatus) {
        if (null != mergeStatus && mergeStatus.length != 0) {
          mergedSegments.asScala.map { seg =>
            val file = SegmentFileStore.writeSegmentFile(
              indexCarbonTable,
              seg.getLoadName,
              segmentIdToLoadStartTimeMapping(seg.getLoadName).toString,
              carbonLoadModel.getFactTimeStamp.toString,
              null)
            val segment = new Segment(seg.getLoadName, file)
            SegmentFileStore.updateSegmentFile(indexCarbonTable,
              seg.getLoadName,
              file,
              indexCarbonTable.getCarbonTableIdentifier.getTableId,
              new SegmentFileStore(tablePath, segment.getSegmentFileName()))
            segment
          }

          val endTime = System.currentTimeMillis()
          val loadMetadataDetails = SegmentStatusManager
            .readLoadMetadata(indexCarbonTable.getMetadataPath)
          loadMetadataDetails.foreach(loadMetadataDetail => {
            if (rebuiltSegments.contains(loadMetadataDetail.getLoadName)) {
              loadMetadataDetail.setLoadStartTime(carbonLoadModel.getFactTimeStamp)
              loadMetadataDetail.setLoadEndTime(endTime)
              CarbonLoaderUtil
                .addDataIndexSizeIntoMetaEntry(loadMetadataDetail,
                  loadMetadataDetail.getLoadName,
                  indexCarbonTable)
            }
          })

          SegmentStatusManager
            .writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(tablePath),
              loadMetadataDetails)

          // clear the datamap cache for the merged segments, as the index files and
          // data files are rewritten after compaction
          if (mergedSegments.size > 0) {

            // merge index files for merged segments
            CarbonMergeFilesRDD.mergeIndexFiles(sc.sparkSession,
              rebuiltSegments.toSeq,
              segmentIdToLoadStartTimeMap,
              indexCarbonTable.getTablePath,
              indexCarbonTable, mergeIndexProperty = false
            )

            if (CarbonProperties.getInstance()
              .isDistributedPruningEnabled(indexCarbonTable.getDatabaseName,
                indexCarbonTable.getTableName)) {
              try {
                IndexServer.getClient
                  .invalidateSegmentCache(indexCarbonTable,
                    rebuiltSegments.toArray,
                    SparkSQLUtil.getTaskGroupId(sc.sparkSession))
              } catch {
                case _: Exception =>
              }
            }

            DataMapStoreManager.getInstance
              .clearInvalidSegments(indexCarbonTable, rebuiltSegments.toList.asJava)
          }
        }

        val endTime = System.nanoTime()
        LOGGER.info(s"Time taken to merge is(in nano) ${endTime - startTime}")
        LOGGER.info(s"Merge data files request completed for table " +
                    s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        rebuiltSegments
      } else {
        LOGGER.error(s"Merge data files request failed for table " +
                     s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        throw new Exception("Merge data files Failure in Merger Rdd.")
      }
    } catch {
      case e: Exception =>
        LOGGER.error(s"Merge data files request failed for table " +
                     s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        throw new Exception("Merge data files Failure in Merger Rdd.", e)
    }
  }

  /**
   * Identifies the group of blocks to be merged based on the merge size.
   * This should be per segment grouping.
   *
   * @param splits
   * @param mergeSize
   * @return List of List of blocks(grouped based on the size)
   */
  def identifyBlocksToBeMerged(splits: util.List[CarbonInputSplit], mergeSize: Long):
  List[List[CarbonInputSplit]] = {
    val blockGroupsToMerge: List[List[CarbonInputSplit]] = new util.ArrayList()
    var totalSize: Long = 0L
    var blocksSelected: List[CarbonInputSplit] = new util.ArrayList()
    // sort the splits based on the block size and then make groups based on the threshold
    Collections.sort(splits, new Comparator[CarbonInputSplit]() {
      def compare(split1: CarbonInputSplit, split2: CarbonInputSplit): Int = {
        (split1.getLength - split2.getLength).toInt
      }
    })
    for (i <- 0 to splits.size() - 1) {
      val block = splits.get(i)
      val blockFileSize = block.getLength
      blocksSelected.add(block)
      totalSize += blockFileSize
      if (totalSize >= mergeSize) {
        if (!blocksSelected.isEmpty) {
          blockGroupsToMerge.add(blocksSelected)
        }
        totalSize = 0L
        blocksSelected = new util.ArrayList()
      }
    }
    if (!blocksSelected.isEmpty) {
      blockGroupsToMerge.add(blocksSelected)
    }
    // check if all the groups are having only one split, then ignore rebuilding that segment
    if (blockGroupsToMerge.size() == splits.size()) {
      new util.ArrayList[util.List[CarbonInputSplit]]()
    } else {
      blockGroupsToMerge
    }
  }

}
