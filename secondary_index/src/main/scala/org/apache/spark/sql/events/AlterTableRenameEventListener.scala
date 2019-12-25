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
package org.apache.spark.sql.events

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive._
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{AlterTableRenamePostEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class AlterTableRenameEventListener extends OperationEventListener with Logging {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableRenamePreEvent: AlterTableRenamePostEvent =>
        LOGGER.info("alter table rename Pre event listener called")
        val alterTableRenameModel = alterTableRenamePreEvent.alterTableRenameModel
        val carbonTable = alterTableRenamePreEvent.carbonTable
        val sparkSession = alterTableRenamePreEvent.sparkSession
        val newTablePath = alterTableRenamePreEvent.newTablePath
        val oldDatabaseName = carbonTable.getDatabaseName
        val newTableName = alterTableRenameModel.newTableIdentifier.table
        val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        val table: CarbonTable = metastore
          .lookupRelation(Some(oldDatabaseName), newTableName)(sparkSession)
          .asInstanceOf[CarbonRelation].carbonTable
        CarbonInternalScalaUtil.getIndexesMap(table)
          .asScala.map {
          entry =>
            CarbonSessionCatalogUtil.getClient(sparkSession).runSqlHive(
              s"ALTER TABLE $oldDatabaseName.${
                entry
                  ._1
              } SET SERDEPROPERTIES ('parentTableName'='$newTableName')")
        }
    }
  }
}
