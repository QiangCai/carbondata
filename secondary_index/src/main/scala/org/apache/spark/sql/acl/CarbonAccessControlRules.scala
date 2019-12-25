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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.CarbonExpressions.{CarbonDescribeTable => DescribeTableCommand}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.{CreateIndexTable, DropIndex, SIRebuildSegmentCommand}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.partition.CarbonAlterTableSplitPartitionCommand
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand, CarbonAlterTableRenameCommand}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.hive.acl.{HiveACLInterface, ObjectType, PrivObject, PrivType}
import org.apache.spark.sql.hive.execution.command.CarbonDropDatabaseCommand
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.exceptions.sql.{MalformedCarbonCommandException, NoSuchDataMapException}
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, TableInfo}
import org.apache.carbondata.processing.merger.CompactionType

private[sql] case class CarbonAccessControlRules(sparkSession: SparkSession,
    hCatalog: SessionCatalog,
    aclInterface: HiveACLInterface)
  extends Rule[LogicalPlan] {

  private val LOGGER = LogServiceFactory.getLogService(classOf[CarbonAccessControlRules].getName)

  override def apply(plan: LogicalPlan): LogicalPlan = {

    if (ACLFileUtils.isSecureModeEnabled) {
      plan match {
        case c@CarbonCreateTableCommand(tableInfo: TableInfo, _, _, _, _, _) =>
          var databaseOpt : Option[String] = None
          if(tableInfo.getDatabaseName != null) {
            databaseOpt = Some(tableInfo.getDatabaseName)
          }
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            CarbonEnv.getDatabaseName(databaseOpt)(sparkSession),
            null,
            null,
            Set(PrivType.CREATE_NOGRANT))))

        case c@CarbonCreateDataMapCommand(dataMapName: String,
        tableIdentifier: Option[TableIdentifier],
        dmClassName: String,
        dmproperties: Map[String, String],
        queryString: Option[String],
        ifNotExistsSet: Boolean,
        deferredRebuild: Boolean) =>
          if (dmClassName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
            val databaseName =
              if (tableIdentifier.isDefined) {
                tableIdentifier.get.database
                  .getOrElse(sparkSession.sessionState.catalog.getCurrentDatabase)
              } else {
                sparkSession.sessionState.catalog.getCurrentDatabase
              }
            checkPrivilege(c, Set(new PrivObject(
              ObjectType.DATABASE,
              databaseName,
              null,
              null,
              Set(PrivType.CREATE_NOGRANT))))
          } else {
            checkPrivilege(c, Set(new PrivObject(
              ObjectType.TABLE,
              CarbonEnv.getDatabaseName(tableIdentifier.get.database)(sparkSession),
              tableIdentifier.get.table,
              null,
              Set(PrivType.OWNER_PRIV))))
          }

        case c@CreateIndexTable(indexModel, _, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(indexModel.databaseName)(sparkSession),
            indexModel.tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case r@SIRebuildSegmentCommand(alterTableModel, tableInfoOp, _, _) =>
          checkPrivilegeRecursively(r,
            alterTableModel.dbName,
            alterTableModel.tableName,
            PrivType.INSERT_NOGRANT)

        case c@CarbonLoadDataCommand(dbNameOp: Option[String],
        tableName: String, _, _, _, _, _, _, _, _, _, _, _, _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.INSERT_NOGRANT)
        case c@InsertIntoCarbonTable(relation, _, _, _, _) =>
          checkPrivilegeRecursively(c,
            Some(relation.carbonTable.getDatabaseName),
            relation.carbonTable.getTableName,
            PrivType.INSERT_NOGRANT)

        case c@CarbonDeleteLoadByIdCommand(_,
        dbNameOp: Option[String],
        tableName: String) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.DELETE_NOGRANT)

        case c@CarbonDeleteLoadByLoadDateCommand(dbNameOp: Option[String],
        tableName: String,
        _,
        _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName, PrivType.DELETE_NOGRANT)

        case c@DropIndex(_, dbNameOp: Option[String], tableName: String,
        parentTableName: String, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            parentTableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonDropDataMapCommand(
        dataMapName: String,
        ifExistsSet: Boolean,
        table: Option[TableIdentifier],
        forceDrop: Boolean) =>
          var tableName = ""
          var databaseName = ""
          // here just check for datamap owner , because in case of preagg, only owner can create
          // and in case of MV other user can create, so datamap owner check will be more suitable
          // check for drop
          var dataMapSchema: DataMapSchema = null
          try {
            dataMapSchema = DataMapStoreManager.getInstance().getDataMapSchema(dataMapName)
            // If drop command is for MV then construct the table name using 'table' keyword.
            if (dataMapSchema.getProviderName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
              // in case of datamaps like MV, lucene, bloom datamap, get the details from schema
              tableName = dataMapName + "_table"
              databaseName = dataMapSchema.getRelationIdentifier.getDatabaseName
            } else if (table.isDefined) {
              tableName = table.get.table
              databaseName = CarbonEnv.getDatabaseName(table.get.database)(sparkSession)
            } else {
              tableName = dataMapSchema.getRelationIdentifier.getTableName
              databaseName = dataMapSchema.getRelationIdentifier.getDatabaseName
            }
          } catch {
            case ex: NoSuchDataMapException =>
              // in case of preagg , schema wont be present, so get the table from identifier if
              // defined, if not throw back the exception
              if (table.isDefined) {
                tableName = table.get.table
                databaseName = CarbonEnv.getDatabaseName(table.get.database)(sparkSession)
              } else {
                if (!ifExistsSet) {
                  throw ex
                } else {
                  LOGGER
                    .warn(s"Datamap $dataMapName does not exists, Ignoring the ACL check for drop" +
                          s" datamap")
                  return plan
                }
              }
          }

          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            databaseName,
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))),
            null, ifExistsSet)

        case c@DropTableCommand(identifier, ifExists, _, _) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(identifier.database)(sparkSession)),
            identifier.table,
            PrivType.OWNER_PRIV,
            ifExists)

        case c@DropDatabaseCommand(dbName, ifExists, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            dbName,
            null,
            null,
            Set(PrivType.OWNER_PRIV))),
            null,
            ifExists)

          hCatalog.listTables(dbName).foreach(tableIdentifier =>
            checkPrivilege(c, Set(new PrivObject(
              ObjectType.TABLE,
              CarbonEnv.getDatabaseName(Some(dbName))(sparkSession),
              tableIdentifier.table,
              null,
              Set(PrivType.OWNER_PRIV))))
          )
          c

        case c@CarbonDropTableCommand(_,
        dbNameOp: Option[String],
        tableName: String, _) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)),
            tableName,
            PrivType.OWNER_PRIV)

        case c@CarbonShowLoadsCommand(dbNameOp, tableName, _, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.SELECT_NOGRANT))))

        case c@ShowIndexesCommand(dbNameOp, tableName, showIndexSql) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(PrivType.SELECT_NOGRANT))))

        case c@CarbonDataMapShowCommand(table) if table.isDefined =>
        checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(table.get.database)(sparkSession)),
            table.get.table,
            PrivType.SELECT_NOGRANT)
        case c@CarbonDataMapRebuildCommand(
        dataMapName: String,
        table: Option[TableIdentifier]) =>
          val dataMapSchema = DataMapStoreManager.getInstance().getDataMapSchema(dataMapName)
          val tableName: String = if (dataMapSchema.getProviderName
            .equalsIgnoreCase(DataMapClassProvider.MV.getShortName)) {
            dataMapName + "_table"
          } else {
            // if not MV datamap, parent tables will be one for preagg, or bloom etc
            dataMapSchema.getParentTables.asScala.head.getTableName
          }
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            dataMapSchema.getRelationIdentifier.getDatabaseName,
            tableName,
            null,
            Set(PrivType.INSERT_NOGRANT))))

          // check the select privilege on all parent tables
          val parentTables = dataMapSchema.getParentTables
          parentTables.asScala.foreach {
            parentRelationIdentifier =>
              checkPrivilege(c, Set(new PrivObject(
                ObjectType.TABLE,
                parentRelationIdentifier.getDatabaseName,
                parentRelationIdentifier.getTableName,
                null,
                Set(PrivType.SELECT_NOGRANT))))
          }
          plan
        case c@DescribeTableCommand(identifier, _, _) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(identifier.database)(sparkSession),
            identifier.table,
            null,
            Set(PrivType.SELECT_NOGRANT))),
            identifier)

        case c@CarbonCleanFilesCommand(dbNameOp: Option[String], tableName: Option[String], _, _, _) =>
          checkPrivilegeRecursively(c, dbNameOp, tableName.getOrElse(""), PrivType.DELETE_NOGRANT)

        case c@CarbonAlterTableCompactionCommand(alterTableModel, tableInfoOp, _) =>
          // for executing close streaming compaction user should have owner Privilege
          val compactionType = try {
            CompactionType.valueOf(alterTableModel.compactionType.toUpperCase)
          } catch {
            case ex: Exception =>
              throw new MalformedCarbonCommandException(s"unsupported alter operation on carbon" +
                                                        s" table ${alterTableModel.tableName}")
          }
          if (compactionType == CompactionType.CLOSE_STREAMING) {
            checkPrivilegeRecursively(c,
              alterTableModel.dbName,
              alterTableModel.tableName,
              PrivType.OWNER_PRIV)
          }
          else {
            checkPrivilegeRecursively(c,
              alterTableModel.dbName,
              alterTableModel.tableName,
              PrivType.INSERT_NOGRANT)
          }

        case c@CarbonDropDatabaseCommand(command) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.DATABASE,
            command.databaseName,
            null,
            null,
            Set(PrivType.OWNER_PRIV))),
            null,
            command.ifExists)

        case c@CarbonAlterTableColRenameDataTypeChangeCommand(alterTableChangeDataTypeModel, _) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(alterTableChangeDataTypeModel.databaseName)(sparkSession)),
            alterTableChangeDataTypeModel.tableName,
            PrivType.OWNER_PRIV)

        case c@CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel) =>
          checkPrivilege(c, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(alterTableAddColumnsModel.databaseName)(sparkSession),
            alterTableAddColumnsModel.tableName,
            null,
            Set(PrivType.OWNER_PRIV))))

        case c@CarbonAlterTableDropColumnCommand(alterTableDropColumnModel) =>
          checkPrivilegeRecursively(c, Some(
            CarbonEnv.getDatabaseName(alterTableDropColumnModel.databaseName)(sparkSession)),
            alterTableDropColumnModel.tableName,
            PrivType.OWNER_PRIV)

        case c@CarbonAlterTableRenameCommand(alterTableRenameModel) =>
          checkPrivilegeRecursively(c,
            Some(CarbonEnv
              .getDatabaseName(alterTableRenameModel.oldTableIdentifier.database)(sparkSession)),
            alterTableRenameModel.oldTableIdentifier.table,
            PrivType.OWNER_PRIV)
        case c@AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists) =>
            checkPrivilegeRecursively(c,
              Some(CarbonEnv
                .getDatabaseName(tableName.database)(sparkSession)),
              tableName.table,
              PrivType.OWNER_PRIV)
        case c@AlterTableDropPartitionCommand(tableIdentifier, specs, ifExists, purge,
        retainData) =>
            checkPrivilegeRecursively(c,
              Some(CarbonEnv
                .getDatabaseName(tableIdentifier.database)(sparkSession)),
              tableIdentifier.table,
              PrivType.OWNER_PRIV)
        case c@CarbonAlterTableSplitPartitionCommand(splitPartitionModel) =>
          checkPrivilegeRecursively(c,
            Some(CarbonEnv
              .getDatabaseName(splitPartitionModel.databaseName)(sparkSession)),
            splitPartitionModel.tableName,
            PrivType.OWNER_PRIV)
        case s@CarbonAlterTableFinishStreaming(dbName, tableName) =>
          checkPrivilege(s, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbName)(sparkSession),
            tableName,
            null,
            Set(PrivType.OWNER_PRIV))))
        case a@AlterTableSetPropertiesCommand(tableName, properties, isView) =>
          checkPrivilege(a, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(tableName.database)(sparkSession),
            tableName.table,
            null,
            Set(PrivType.OWNER_PRIV))))
        case u@UpdateTable(table, cols, _, sel, where) =>
          checkPrivilegeRecursively(u,
            Some(CarbonEnv.getDatabaseName(table.tableIdentifier.database)(sparkSession)),
            table.tableIdentifier.table,
            PrivType.UPDATE_NOGRANT)

        case d@DeleteRecords(statement, _, table) =>
          checkPrivilegeRecursively(d,
            Some(CarbonEnv.getDatabaseName(table.tableIdentifier.database)(sparkSession)),
            table.tableIdentifier.table,
            PrivType.DELETE_NOGRANT)

        case l: LogicalPlan => l
      }
    } else {
      plan
    }
  }

  private def checkPrivilege(
      l: LogicalPlan,
      privSet: Set[PrivObject],
      tableIdentifier: TableIdentifier = null,
      ifExists: Boolean = false): LogicalPlan = {
    val newSet = new mutable.HashSet[PrivObject]()
    for (priv <- privSet) {
      if (priv.objType == ObjectType.COLUMN || priv.objType == ObjectType.TABLE) {
        if (tableIdentifier == null) {
          newSet += priv
        } else if (!hCatalog.isTemporaryTable(tableIdentifier)) {
          newSet += priv
        }
      } else {
        newSet += priv
      }
    }
    if (aclInterface.checkPrivilege(newSet.toSet, ifExists)) {
      l
    } else {
      throw new AnalysisException("Missing Privileges", l.origin.line, l.origin.startPosition)
    }
  }

  private def checkPrivilegeRecursively(plan: LogicalPlan,
      dbNameOp: Option[String],
      tableName: String,
      privType: PrivType.PrivType,
      ifexists: Boolean = false): LogicalPlan = {
    var isCarbonTable: Boolean = false
    isCarbonTable =
      try {
        CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(tableName,
            Some(CarbonEnv.getDatabaseName(dbNameOp)(sparkSession))))(
            sparkSession)
      } catch {
        case ex: Exception =>
          if (ex.getMessage.contains("Permission denied") ||
              ex.getMessage.contains("Missing Privileges")) {
            throw new AnalysisException("Missing Privileges")
          }
          throw ex
      }
    if (isCarbonTable) {
      checkPrivilege(plan, Set(new PrivObject(
        ObjectType.TABLE,
        CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
        tableName,
        null,
        Set(privType))),
        null,
        ifexists)
      val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(dbNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
        .carbonTable
      val tableList = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
      if (!tableList.isEmpty) {
        tableList.asScala.foreach { tableName =>
          checkPrivilege(plan, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName,
            null,
            Set(privType))))
        }
      }
      val preAggregateTableList = carbonTable.getTableInfo.getDataMapSchemaList
      if (!preAggregateTableList.isEmpty) {
        preAggregateTableList.asScala.foreach { tableName =>
          checkPrivilege(plan, Set(new PrivObject(
            ObjectType.TABLE,
            CarbonEnv.getDatabaseName(dbNameOp)(sparkSession),
            tableName.getRelationIdentifier.getTableName,
            null,
            Set(privType))))
        }
      }

      if (!carbonTable.isChildTable || !CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
        DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable).asScala.filter {
          schema => schema.getProviderName.equalsIgnoreCase(DataMapClassProvider.MV.getShortName)
        }.foreach { datamap =>
          checkPrivilege(plan, Set(new PrivObject(
            ObjectType.TABLE,
            datamap.getRelationIdentifier.getDatabaseName,
            datamap.getRelationIdentifier.getTableName,
            null,
            Set(privType))))
        }
      }
    }
    plan
  }

}

