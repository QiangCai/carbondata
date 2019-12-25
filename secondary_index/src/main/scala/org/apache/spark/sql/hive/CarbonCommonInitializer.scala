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

package org.apache.spark.sql.hive

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.acl.{
  ACLIUDDeleteEventListener, ACLIUDUpdateEventListener,
  ACLRefreshTableEventListener, CarbonEnvInitPreEventListener, _
}
import org.apache.spark.sql.events.{DeleteFromTableEventListener, _}
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonPluginProperties
import org.apache.carbondata.events.{RefreshTablePostExecutionEvent, _}
import org.apache.carbondata.processing.loading.events.LoadEvents._
import org.apache.carbondata.spark.acl.ACLFileFactory

/**
 * Initializes the listener
 */
object CarbonCommonInitializer {

  def init(enableACL: Boolean = true): Unit = {
    CarbonEnv.init
    // register internal carbon property to propertySet
    CarbonPluginProperties.validateAndLoadDefaultInternalProperties()

    val operationListenerBus = OperationListenerBus.getInstance()

    // Listener added for blocking bucketing feature
    if (SparkUtil.isFI) {
      operationListenerBus
        .addListener(classOf[CreateTablePreExecutionEvent],
          new BlockEventListener
        )
    }

    if (enableACL) {
      initACL0()
    }

    operationListenerBus
      .addListener(classOf[CarbonEnvInitPreEvent],
        new CarbonEnvInitPreEventListener
      )

    // SI Listeners
    operationListenerBus
      .addListener(classOf[LoadTablePreStatusUpdateEvent], new SILoadEventListener)
    operationListenerBus
      .addListener(classOf[LoadTablePostStatusUpdateEvent],
        new SILoadEventListenerForFailedSegments)
    operationListenerBus
      .addListener(classOf[LookupRelationPostEvent], new SIRefreshEventListener)
    // TODO: get create relation event
    operationListenerBus
      .addListener(classOf[CreateCarbonRelationPostEvent], new
          CreateCarbonRelationEventListener
      )
    operationListenerBus
      .addListener(classOf[DropTablePreEvent], new SIDropEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableDropColumnPreEvent], new AlterTableDropColumnEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableRenamePostEvent], new AlterTableRenameEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        new AlterTableColumnRenameEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePostEvent],
        new AlterTableColumnRenameEventListener)
    operationListenerBus
      .addListener(classOf[DeleteSegmentByIdPostEvent], new DeleteSegmentByIdListener)
    operationListenerBus
      .addListener(classOf[DeleteSegmentByDatePostEvent], new DeleteSegmentByDateListener)
    operationListenerBus
      .addListener(classOf[CleanFilesPostEvent], new CleanFilesPostEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
        new AlterTableCompactionPostEventListener)
    operationListenerBus
      .addListener(classOf[AlterTableMergeIndexEvent],
        new AlterTableMergeIndexSIEventListener)
    operationListenerBus
      .addListener(classOf[UpdateTablePreEvent], new UpdateTablePreEventListener)
    operationListenerBus
      .addListener(classOf[DeleteFromTablePostEvent], new DeleteFromTableEventListener)
    operationListenerBus
      .addListener(classOf[DeleteFromTablePreEvent], new DeleteFromTableEventListener)
    operationListenerBus
      .addListener(classOf[DropTableCacheEvent], DropCacheSIEventListener)
    operationListenerBus
      .addListener(classOf[ShowTableCacheEvent], ShowCacheSIEventListener)
    if (enableACL) {
      initACL1()
    } else {
      operationListenerBus
        .addListener(classOf[CreateDataMapPreExecutionEvent],
          new ACLDataMapEventListener.PreDataMapEventListener
        )
    }
  }

  def initACL0(): Unit = {
    val operationListenerBus = OperationListenerBus.getInstance()
    // ACL Listeners
    FileFactory.setFileTypeInterface(new ACLFileFactory())
    operationListenerBus
      .addListener(classOf[LoadTablePreExecutionEvent],
        new ACLLoadEventListener.ACLPreLoadEventListener
      )
    operationListenerBus
      .addListener(classOf[LoadTablePostExecutionEvent],
        new ACLLoadEventListener.ACLPostLoadEventListener
      )

    operationListenerBus
      .addListener(classOf[CreateTablePreExecutionEvent],
        new ACLCreateTableEventListener.ACLPreCreateTableEventListener
      )
    operationListenerBus
      .addListener(classOf[CreateTablePostExecutionEvent],
        new ACLCreateTableEventListener.ACLPostCreateTableEventListener
      )
    operationListenerBus
      .addListener(classOf[CreateTableAbortExecutionEvent],
        new ACLCreateTableEventListener.ACLAbortCreateTableEventListener
      )

    operationListenerBus
      .addListener(classOf[AlterTableAddColumnPreEvent],
        new ACLAlterTableAddColumnEventListener.ACLPreAlterTableAddColumnEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableAddColumnPostEvent],
        new ACLAlterTableAddColumnEventListener.ACLPostAlterTableAddColumnEventListener
      )

    operationListenerBus
      .addListener(classOf[PreAlterTableHivePartitionCommandEvent],
        new AlterTableHivePartitionCommandEventListeners
        .ACLPreAlterTableHivePartitionCommandEventListener
      )
    operationListenerBus
      .addListener(classOf[PostAlterTableHivePartitionCommandEvent],
        new AlterTableHivePartitionCommandEventListeners
        .ACLPostAlterTableHivePartitionCommandEventListener
      )

    operationListenerBus
      .addListener(classOf[AlterTableDropColumnPreEvent],
        new ACLAlterTableDropColumnEventListener.ACLPreAlterTableDropColumnEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableDropColumnPostEvent],
        new ACLAlterTableDropColumnEventListener.ACLPostAlterTableDropColumnEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableDropColumnAbortEvent],
        new ACLAlterTableDropColumnEventListener.ACLAbortAlterTableDropColumnEventListener
      )

    operationListenerBus
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePreEvent],
        new ACLAlterTableDataTypeChangeEventListener
        .ACLPreAlterTableDataTypeChangeEventListener
      )

    operationListenerBus
      .addListener(classOf[AlterTableColRenameAndDataTypeChangePostEvent],
        new ACLAlterTableDataTypeChangeEventListener
        .ACLPostAlterTableDataTypeChangeEventListener
      )

    operationListenerBus
      .addListener(classOf[DeleteSegmentByDatePreEvent],
        new ACLDeleteSegmentByDateEventListener.ACLPreDeleteSegmentByDateEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteSegmentByDatePostEvent],
        new ACLDeleteSegmentByDateEventListener.ACLPostDeleteSegmentByDateEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteSegmentByDateAbortEvent],
        new ACLDeleteSegmentByDateEventListener.ACLAbortDeleteSegmentByDateEventListener
      )

    operationListenerBus
      .addListener(classOf[DeleteSegmentByIdPreEvent],
        new ACLDeleteSegmentByIdEventListener.ACLPreDeleteSegmentByIdEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteSegmentByIdPostEvent],
        new ACLDeleteSegmentByIdEventListener.ACLPostDeleteSegmentByIdEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteSegmentByIdAbortEvent],
        new ACLDeleteSegmentByIdEventListener.ACLAbortDeleteSegmentByIdEventListener
      )

    operationListenerBus
      .addListener(classOf[CleanFilesPreEvent],
        new ACLCleanFilesEventListener.ACLPreCleanFilesEventListener
      )
    operationListenerBus
      .addListener(classOf[CleanFilesPostEvent],
        new ACLCleanFilesEventListener.ACLPostCleanFilesEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableCompactionPreEvent],
        new ACLCompactionEventListener.ACLPreCompactionEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableCompactionPostEvent],
        new ACLCompactionEventListener.ACLPostCompactionEventListener
      )
    operationListenerBus
      .addListener(classOf[AlterTableCompactionAbortEvent],
        new ACLCompactionEventListener.ACLAbortCompactionEventListener
      )

    operationListenerBus
      .addListener(classOf[CreateDatabasePreExecutionEvent],
        new ACLCreateDatabaseListener.ACLPreCreateDatabaseListener
      )
    operationListenerBus
      .addListener(classOf[CreateDatabasePostExecutionEvent],
        new ACLCreateDatabaseListener.ACLPostCreateDatabaseListener
      )
    operationListenerBus
      .addListener(classOf[CreateDatabaseAbortExecutionEvent],
        new ACLCreateDatabaseListener.ACLAbortCreateDatabaseListener
      )

    operationListenerBus
      .addListener(classOf[DeleteFromTablePreEvent],
        new ACLIUDDeleteEventListener.ACLPreIUDDeleteEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteFromTablePostEvent],
        new ACLIUDDeleteEventListener.ACLPostIUDDeleteEventListener
      )
    operationListenerBus
      .addListener(classOf[DeleteFromTableAbortEvent],
        new ACLIUDDeleteEventListener.ACLAbortIUDDeleteEventListener
      )

    operationListenerBus
      .addListener(classOf[UpdateTablePreEvent],
        new ACLIUDUpdateEventListener.ACLPreIUDUpdateEventListener
      )
    operationListenerBus
      .addListener(classOf[UpdateTablePostEvent],
        new ACLIUDUpdateEventListener.ACLPostIUDUpdateEventListener
      )
    operationListenerBus
      .addListener(classOf[UpdateTableAbortEvent],
        new ACLIUDUpdateEventListener.ACLAbortIUDUpdateEventListener
      )
  }


  def initACL1(): Unit = {
    val operationListenerBus = OperationListenerBus.getInstance()
    // refresh table listner
    operationListenerBus
      .addListener(classOf[RefreshTablePreExecutionEvent],
        new ACLRefreshTableEventListener.ACLPreRefreshTableEventListener
      )
    operationListenerBus
      .addListener(classOf[RefreshTablePostExecutionEvent],
        new ACLRefreshTableEventListener.ACLPostRefreshTableEventListener
      )
    operationListenerBus
      .addListener(classOf[LoadTableSIPreExecutionEvent],
        new ACLIndexLoadEventListener.ACLPreCreateIndexEventListener
      )
    operationListenerBus
      .addListener(classOf[LoadTableSIPostExecutionEvent],
        new ACLIndexLoadEventListener.ACLPostCreateIndexEventListener
      )
    // create datamap
    operationListenerBus
      .addListener(classOf[CreateDataMapPreExecutionEvent],
        new ACLDataMapEventListener.ACLPreDataMapEventListener
      )
    operationListenerBus
      .addListener(classOf[CreateDataMapPostExecutionEvent],
        new ACLDataMapEventListener.ACLPostDataMapEventListener
      )
    // uodate datamap status
    operationListenerBus
      .addListener(classOf[UpdateDataMapPreExecutionEvent],
        new ACLDataMapEventListener.ACLPreDataMapEventListener
      )
    operationListenerBus
      .addListener(classOf[UpdateDataMapPostExecutionEvent],
        new ACLDataMapEventListener.ACLPostDataMapEventListener
      )
    // rebuild datamap
    operationListenerBus
      .addListener(classOf[BuildDataMapPreExecutionEvent],
        new ACLBuildDataMapEventListener.ACLPreBuildDataMapEventListener
      )
    operationListenerBus
      .addListener(classOf[BuildDataMapPostExecutionEvent],
        new ACLBuildDataMapEventListener.ACLPostBuildDataMapEventListener
      )
  }
}
