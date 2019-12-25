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

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.util.si.FileInternalUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.spark.indextable.IndexTableUtil


/**
 *
 */
object CarbonInternalHiveMetadataUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def refreshTable(dbName: String, tableName: String, sparkSession: SparkSession): Unit = {
    val tableWithDb = dbName + "." + tableName
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableWithDb)
    sparkSession.sessionState.catalog.refreshTable(tableIdent)
  }

  /**
   * This method invalidates the table from HiveMetastoreCatalog before dropping table and also
   * removes the index table info from parent carbon table.
   *
   * @param indexTableIdentifier
   * @param indexInfo
   * @param parentCarbonTable
   * @param sparkSession
   */
  def invalidateAndUpdateIndexInfo(indexTableIdentifier: TableIdentifier,
      indexInfo: String, parentCarbonTable: CarbonTable)(sparkSession: SparkSession): Unit = {
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val dbName = indexTableIdentifier.database
      .getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME)
    val tableName = indexTableIdentifier.table
    try {
      if (indexInfo != null) {
        removeIndexInfoFromParentTable(indexInfo,
          parentCarbonTable,
          dbName,
          tableName)(sparkSession)
      }
    } catch {
      case e: Exception =>
        LOGGER.error(
          s"Error While deleting the table $dbName.$tableName during drop carbon table" +
          e.getMessage)
    }
  }

  def removeIndexInfoFromParentTable(indexInfo: String,
    parentCarbonTable: CarbonTable,
    dbName: String,
    tableName: String)(sparkSession: SparkSession): Unit = {
    val parentTableName = parentCarbonTable.getTableName
    val newIndexInfo = IndexTableUtil.removeIndexTable(indexInfo, dbName, tableName)
    CarbonInternalScalaUtil.removeIndexTableInfo(parentCarbonTable, tableName)
    sparkSession.sql(
      s"""ALTER TABLE $dbName.$parentTableName SET SERDEPROPERTIES ('indexInfo'='$newIndexInfo')
        """.stripMargin)
    FileInternalUtil.touchSchemaFileTimestamp(dbName, parentTableName,
      parentCarbonTable.getTablePath, System.currentTimeMillis())
    FileInternalUtil.touchStoreTimeStamp()
    refreshTable(dbName, parentTableName, sparkSession)
  }

  def transformToRemoveNI(expression: Expression): Expression = {
    expression.transform {
      case hiveUDF: HiveSimpleUDF if hiveUDF.function.isInstanceOf[NonIndexUDFExpression] =>
        hiveUDF.asInstanceOf[HiveSimpleUDF].children.head
      case scalaUDF: ScalaUDF if "NI".equalsIgnoreCase(scalaUDF.udfName.get) =>
        scalaUDF.children.head
    }
  }
}
