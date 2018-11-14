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
package org.apache.spark.sql.command

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants

/**
 * Register index table with main table
 * 1. check if the main and index table exist
 * 2. call the create index command with isCreateSIndex = false
 * (do not create the si table in store path & avoid data load for si)
 */
case class RegisterIndexTableCommand(dbName: Option[String], indexTableName: String,
  parentTable: String, var registerSql: String = null)
  extends AtomicRunnableCommand {
  val LOGGER =
    LogServiceFactory.getLogService(this.getClass.getName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val databaseName = CarbonEnv.getDatabaseName(dbName)(sparkSession)
    val databaseLocation = CarbonEnv.getDatabaseLocation(databaseName, sparkSession)
    val tablePath = databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + indexTableName
    val absoluteTableIdentifier = AbsoluteTableIdentifier.from(tablePath, databaseName,
      indexTableName
    )
    setAuditTable(databaseName, indexTableName)
    setAuditInfo(Map("Parent TableName" -> parentTable))
    // 1. check if the main and index table exist
    val tables: Seq[TableIdentifier] = sparkSession.sessionState.catalog.listTables(databaseName)
    if (!tables.exists(_.table.equalsIgnoreCase(parentTable))) {
      val message: String = s"Secondary Index Table registration for table [$indexTableName] with" +
        s" table" +
        s" [$databaseName.$parentTable] failed." +
        s"Table [$parentTable] does not exists under database [$databaseName]"
      CarbonException.analysisException(message)
    }
    if (!tables.exists(_.table.equalsIgnoreCase(indexTableName))) {
      val message: String = s"Secondary Index Table registration for table [$indexTableName] with" +
        s" table" +
        s" [$databaseName.$parentTable] failed." +
        s"Secondary Index Table [$indexTableName] does not exists under database [$databaseName]"
      CarbonException.analysisException(message)
    }
    // 2. Read TableInfo
    val tableInfo = SchemaReader.getTableInfo(absoluteTableIdentifier)
    val columns: List[String] = getIndexColumn(tableInfo)
    val secondaryIndex = SecondaryIndex(dbName, parentTable.toLowerCase, columns,
      indexTableName.toLowerCase)
    // 3. Call the create index command with isCreateSIndex = false
    // (do not create the si table in store path)
    CreateIndexTable(indexModel = secondaryIndex,
      tableProperties = tableInfo.getFactTable.getTableProperties.asScala,
      isCreateSIndex = false).run(sparkSession)
     LOGGER.info(s"Table [$indexTableName] registered as Secondary Index table with" +
                       s" table [$databaseName.$parentTable] successfully.")
    Seq.empty
  }

  /**
   * The method return's the List of dimension columns excluding the positionReference dimension
    *
   * @param tableInfo TableInfo object
   * @return List[String] List of dimension column names
   */
  def getIndexColumn(tableInfo: TableInfo) : List[String] = {
    val columns: util.List[ColumnSchema] = tableInfo.getFactTable.getListOfColumns
    columns.asScala.filter(f => (f.isDimensionColumn &&
      !f.getColumnName.equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_REFERENCE))
    ).map(column => column.getColumnName.toLowerCase()).toList
  }

  override protected def opName: String = "Register Index Table"

}
