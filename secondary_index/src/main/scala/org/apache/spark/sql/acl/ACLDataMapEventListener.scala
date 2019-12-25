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
package org.apache.spark.sql.acl

import java.security.PrivilegedExceptionAction

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.acl.ACLFileUtils.{getFolderListKey, getPathListKey}
import org.apache.spark.sql.command.ErrorMessage
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.events.{CreateDataMapPostExecutionEvent, CreateDataMapPreExecutionEvent, _}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

object ACLDataMapEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * The class will handle the Create DataMap Events, to apply he file permission
   * on the _system folder and schema and datamap schema datamapstatus file.
   */
  class ACLPreDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = createDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = createDataMapPreExecutionEvent.storePath
          val tableIdentifier = createDataMapPreExecutionEvent.tableIdentifier
          var carbonTableIdentifier: CarbonTableIdentifier = null
            if (null != tableIdentifier) {
            val dbName: String = tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase)
            carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableIdentifier.table, "")
            val carbonTable = CarbonEnv
              .getCarbonTable(Some(dbName), tableIdentifier.table)(sparkSession)
            if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
              throw new ErrorMessage(
                "Datamap creation on Pre-aggregate table or Secondary Index table is not supported")
            }
          }
          if (!FileFactory.isFileExist(systemDirectoryPath)) {
            CarbonUserGroupInformation.getInstance.getCurrentUser
              .doAs(new PrivilegedExceptionAction[Unit]() {
                override def run(): Unit = {
                  FileFactory.createDirectoryAndSetPermission(systemDirectoryPath,
                    ACLFileUtils.getPermissionsOnDatabase())
                }
              })
          }
          val folderListBeforeReBuild = List[String](systemDirectoryPath)
          val pathArrBeforeLoadOperation = ACLFileUtils
            .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild)
          // Incase of MV datamap, the identifier cannot be defined, as it can build MV on multiple
          // tables also. So handling that also
          operationContext.setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier),
            folderListBeforeReBuild)
          operationContext.setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
            pathArrBeforeLoadOperation)

          // This event will be for index datamaps, so no need to handle specially for MV datamap
        case updateDataMapPreExecutionEvent: UpdateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = updateDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = updateDataMapPreExecutionEvent.storePath
          val tableIdentifier = updateDataMapPreExecutionEvent.tableIdentifier
          if (tableIdentifier != null) {
            val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase), tableIdentifier.table, "")
            val folderListBeforeReBuild = List[String](systemDirectoryPath)
            val pathArrBeforeLoadOperation = ACLFileUtils
              .takeRecurTraverseSnapshot(sparkSession.sqlContext, folderListBeforeReBuild)
            operationContext
              .setProperty(ACLFileUtils.getFolderListKey(carbonTableIdentifier),
                folderListBeforeReBuild)
            operationContext
              .setProperty(ACLFileUtils.getPathListKey(carbonTableIdentifier),
                pathArrBeforeLoadOperation)
          }
      }
    }
  }

  class PreDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPreExecutionEvent: CreateDataMapPreExecutionEvent =>
          val sparkSession: SparkSession = createDataMapPreExecutionEvent.sparkSession
          val systemDirectoryPath: String = createDataMapPreExecutionEvent.storePath
          val tableIdentifier = createDataMapPreExecutionEvent.tableIdentifier
          var carbonTableIdentifier: CarbonTableIdentifier = null
          if (null != tableIdentifier) {
            val dbName: String = tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase)
            carbonTableIdentifier = new CarbonTableIdentifier(dbName, tableIdentifier.table, "")
            val carbonTable = CarbonEnv
              .getCarbonTable(Some(dbName), tableIdentifier.table)(sparkSession)
            if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
              throw new ErrorMessage(
                "Datamap creation on Pre-aggregate table or Secondary Index table is not supported")
            }
          }
      }
    }
  }

  class ACLPostDataMapEventListener extends OperationEventListener {

    override def onEvent(event: Event, operationContext: OperationContext): Unit = {
      event match {
        case createDataMapPostExecutionEvent: CreateDataMapPostExecutionEvent =>
          val sparkSession = createDataMapPostExecutionEvent.sparkSession
          val tableIdentifier = createDataMapPostExecutionEvent.tableIdentifier
          var carbonTableIdentifier: CarbonTableIdentifier = null
          if (tableIdentifier.isDefined) {
            val carbonTable = CarbonEnv
              .getCarbonTable(tableIdentifier.get.database, tableIdentifier.get.table)(sparkSession)
            carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
            val dmProviderName = createDataMapPostExecutionEvent.dmProviderName
            // Checking only for preaggregate as for preagregate only requires to take snapshot
            // of schema file. PredataMapEvent is already skipped in case of Preaggreagte and here
            // setting only schema file to change ownership and Permission
            if (dmProviderName.equalsIgnoreCase(DataMapClassProvider.PREAGGREGATE.toString)) {
              val schemaPath = carbonTable.getTablePath +
                               CarbonCommonConstants.FILE_SEPARATOR + "Metadata" +
                               CarbonCommonConstants.FILE_SEPARATOR + "schema"
              operationContext
                .setProperty(getFolderListKey(carbonTable.getCarbonTableIdentifier),
                  List(schemaPath))
              operationContext
                .setProperty(getPathListKey(carbonTable.getCarbonTableIdentifier),
                  ArrayBuffer(""))
            }
          }
          ACLFileUtils
            .takeSnapAfterOperationAndApplyACL(sparkSession,
              operationContext,
              carbonTableIdentifier)

        case updateDataMapPostExecutionEvent: UpdateDataMapPostExecutionEvent =>
          val sparkSession = updateDataMapPostExecutionEvent.sparkSession
          val tableIdentifier = updateDataMapPostExecutionEvent.tableIdentifier
          if (tableIdentifier != null) {
            val carbonTableIdentifier = new CarbonTableIdentifier(tableIdentifier.database
              .getOrElse(sparkSession.catalog.currentDatabase), tableIdentifier.table, "")
            ACLFileUtils
              .takeSnapAfterOperationAndApplyACL(sparkSession,
                operationContext,
                carbonTableIdentifier)
          }
      }
    }
  }

}
