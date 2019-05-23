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
package org.apache.carbondata.spark.spark.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.log4j.Logger;
import org.apache.spark.util.CarbonInternalScalaUtil;

/**
 *
 */
public class CarbonInternalLoaderUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonInternalLoaderUtil.class.getName());

  public static List<String> getListOfValidSlices(LoadMetadataDetails[] details) {
    List<String> activeSlices =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : details) {
      if (SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.MARKED_FOR_UPDATE.equals(oneLoad.getSegmentStatus())) {
        activeSlices.add(oneLoad.getLoadName());
      }
    }
    return activeSlices;
  }

  /**
   * This method will return the mapping of valid segments to segment laod start time
   *
   * @param details
   * @return
   */
  public static Map<String, Long> getSegmentToLoadStartTimeMapping(LoadMetadataDetails[] details) {
    Map<String, Long> segmentToLoadStartTimeMap = new HashMap<>(details.length);
    for (LoadMetadataDetails oneLoad : details) {
      // valid segments will only have Success status
      if (SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())) {
        segmentToLoadStartTimeMap.put(oneLoad.getLoadName(), oneLoad.getLoadStartTime());
      }
    }
    return segmentToLoadStartTimeMap;
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param newLoadMetadataDetails
   * @param validSegments
   * @param databaseName
   * @param tableName
   * @param carbonTable
   * @param indexCarbonTables
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordLoadMetadata(List<LoadMetadataDetails> newLoadMetadataDetails,
      List<String> validSegments, CarbonTable carbonTable, List<CarbonTable> indexCarbonTables,
      String databaseName, String tableName) throws IOException {
    boolean status = false;
    String metaDataFilepath = carbonTable.getMetadataPath();
    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for table" + databaseName + "." + tableName
            + " for table status updation");

        if (isSegmentsAlreadyCompactedForNewMetaDataDetails(indexCarbonTables, tableName,
            newLoadMetadataDetails)) {
          return false;
        }

        LoadMetadataDetails[] currentLoadMetadataDetails =
            SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        List<LoadMetadataDetails> updatedLoadMetadataDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        // check which load needs to be overwritten which are in in progress state
        boolean found = false;
        for (int i = 0; i < currentLoadMetadataDetails.length; i++) {
          for (int j = 0; j < newLoadMetadataDetails.size(); j++) {
            if (currentLoadMetadataDetails[i].getLoadName()
                .equals(newLoadMetadataDetails.get(j).getLoadName())) {
              currentLoadMetadataDetails[i] = newLoadMetadataDetails.get(j);
              found = true;
              break;
            }
          }
          updatedLoadMetadataDetails.add(currentLoadMetadataDetails[i]);
        }

        // when data load is done for first time, add all the details
        if (currentLoadMetadataDetails.length == 0 || !found) {
          updatedLoadMetadataDetails.addAll(newLoadMetadataDetails);
        }

        List<String> indexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable);
        if (!indexTables.isEmpty()) {
          List<LoadMetadataDetails> newSegmentDetailsListForIndexTable =
              new ArrayList<>(validSegments.size());
          for (String segmentId : validSegments) {
            LoadMetadataDetails newSegmentDetailsObject = new LoadMetadataDetails();
            newSegmentDetailsObject.setLoadName(segmentId);
            newSegmentDetailsListForIndexTable.add(newSegmentDetailsObject);
          }
          for (CarbonTable indexTable : indexCarbonTables) {
            List<LoadMetadataDetails> indexTableDetailsList = CarbonInternalScalaUtil
                .getTableStatusDetailsForIndexTable(updatedLoadMetadataDetails, indexTable,
                    newSegmentDetailsListForIndexTable);

            segmentStatusManager.writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath()),
                indexTableDetailsList
                    .toArray(new LoadMetadataDetails[indexTableDetailsList.size()]));
          }
        } else if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
          segmentStatusManager.writeLoadDetailsIntoFile(
              metaDataFilepath + CarbonCommonConstants.FILE_SEPARATOR
                  + CarbonTablePath.TABLE_STATUS_FILE, updatedLoadMetadataDetails
                  .toArray(new LoadMetadataDetails[updatedLoadMetadataDetails.size()]));
        }
        status = true;
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table " + databaseName + "."
                + tableName);
      }
    } catch (IOException e) {
      status = false;
      LOGGER.error(
          "Not able to acquire the lock for Table status updation for table " + databaseName + "."
              + tableName);
    }
    finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + databaseName + "."
            + tableName);
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + databaseName + "." + tableName
            + " during table status updation");
      }
    }
    return status;
  }

  /**
   * This method read the details of SI table and check whether new metadatadetails are already
   * compacted, if it is, then already compaction for SI is completed and updating with new segment
   * status is useless, this can happen in case of updating the status of index while loading
   * segments for failed segments, so do not update anything, just exit gracefully
   */
  private static boolean isSegmentsAlreadyCompactedForNewMetaDataDetails(
      List<CarbonTable> indexTables, String indexTableName,
      List<LoadMetadataDetails> newLoadMetadataDetails) {
    CarbonTable currentIndexTable = null;
    for (CarbonTable indexTable : indexTables) {
      if (indexTable.getTableName().equalsIgnoreCase(indexTableName)) {
        currentIndexTable = indexTable;
        break;
      }
    }
    boolean isIndexTableSegmentsCompacted = false;
    if (null != currentIndexTable) {
      LoadMetadataDetails[] existingLoadMetaDataDetails =
          SegmentStatusManager.readLoadMetadata(currentIndexTable.getMetadataPath());
      for (int i = 0; i < existingLoadMetaDataDetails.length; i++) {
        for (int j = 0; j < newLoadMetadataDetails.size(); j++) {
          if (existingLoadMetaDataDetails[i].getLoadName()
              .equalsIgnoreCase(newLoadMetadataDetails.get(j).getLoadName())
              && existingLoadMetaDataDetails[i].getSegmentStatus() == SegmentStatus.COMPACTED) {
            isIndexTableSegmentsCompacted = true;
            break;
          }
        }
        if (isIndexTableSegmentsCompacted) {
          break;
        }
      }
      return isIndexTableSegmentsCompacted;
    } else {
      return false;
    }
  }

  /**
   * method to update table status in case of IUD Update Delta Compaction.
   *
   * @param indexCarbonTable
   * @param loadsToMerge
   * @param mergedLoadNumber
   * @param carbonLoadModel
   * @return
   */
  public static boolean updateLoadMetadataWithMergeStatus(CarbonTable indexCarbonTable,
      String[] loadsToMerge, String mergedLoadNumber, CarbonLoadModel carbonLoadModel,
      Map<String, String> segmentToLoadStartTimeMap, long mergeLoadStartTime) throws IOException {
    boolean tableStatusUpdationStatus = false;
    List<String> loadMergeList = new ArrayList<>(Arrays.asList(loadsToMerge));
    AbsoluteTableIdentifier absoluteTableIdentifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for the table " + carbonLoadModel.getDatabaseName() + "."
            + carbonLoadModel.getTableName() + " for table status updation ");
        LoadMetadataDetails[] loadDetails =
            SegmentStatusManager.readLoadMetadata(indexCarbonTable.getMetadataPath());

        long modificationOrDeletionTimeStamp = CarbonUpdateUtil.readCurrentTime();
        for (LoadMetadataDetails loadDetail : loadDetails) {
          // check if this segment is merged.
          if (loadMergeList.contains(loadDetail.getLoadName()) || loadMergeList
              .contains(loadDetail.getMergedLoadName())) {
            // if the compacted load is deleted after the start of the compaction process,
            // then need to discard the compaction process and treat it as failed compaction.
            if (loadDetail.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
              LOGGER.error("Compaction is aborted as the segment " + loadDetail.getLoadName()
                  + " is deleted after the compaction is started.");
              return false;
            }
            loadDetail.setSegmentStatus(SegmentStatus.COMPACTED);
            loadDetail.setModificationOrdeletionTimesStamp(modificationOrDeletionTimeStamp);
            loadDetail.setMergedLoadName(mergedLoadNumber);
          }
        }

        // create entry for merged one.
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setPartitionCount("0");
        loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
        long loadEnddate = CarbonUpdateUtil.readCurrentTime();
        loadMetadataDetails.setLoadEndTime(loadEnddate);
        loadMetadataDetails.setLoadName(mergedLoadNumber);
        loadMetadataDetails.setSegmentFile(SegmentFileStore.genSegmentFileName(mergedLoadNumber,
            String.valueOf(segmentToLoadStartTimeMap.get(mergedLoadNumber)))
            + CarbonTablePath.SEGMENT_EXT);
        CarbonLoaderUtil
            .addDataIndexSizeIntoMetaEntry(loadMetadataDetails, mergedLoadNumber, indexCarbonTable);
        loadMetadataDetails.setLoadStartTime(mergeLoadStartTime);
        loadMetadataDetails.setPartitionCount("0");

        // put the merged folder entry
        for (int i = 0; i < loadDetails.length; i++) {
          if (loadDetails[i].getLoadName().equals(loadMetadataDetails.getLoadName())) {
            loadDetails[i] = loadMetadataDetails;
          }
        }

        // if this is a major compaction then set the segment as major compaction.
        List<LoadMetadataDetails> updatedDetailsList = new ArrayList<>(Arrays.asList(loadDetails));

        segmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(indexCarbonTable.getTablePath()),
            updatedDetailsList.toArray(new LoadMetadataDetails[updatedDetailsList.size()]));
        tableStatusUpdationStatus = true;
      } else {
        LOGGER.error(
            "Could not able to obtain lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + "for table status updation");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + carbonLoadModel
            .getDatabaseName() + "." + carbonLoadModel.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + " during table status updation");
      }
    }
    return tableStatusUpdationStatus;
  }

}
