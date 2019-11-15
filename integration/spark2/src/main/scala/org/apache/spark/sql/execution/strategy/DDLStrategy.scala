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
package org.apache.spark.sql.execution.strategy

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand, CarbonInsertIntoHadoopFsRelationCommand, CarbonLoadDataCommand, RefreshCarbonTableCommand}
import org.apache.spark.sql.execution.command.mutation.CarbonTruncateCommand
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonDropTableCommand, CarbonShowCreateTableCommand}
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand, MatchResetCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, RefreshResource, RefreshTable}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory

  /**
   * Carbon strategies for ddl commands
   * CreateDataSourceTableAsSelectCommand class has extra argument in
   * 2.3, so need to add wrapper to match the case
   */
object MatchCreateDataSourceTable {
  def unapply(plan: LogicalPlan): Option[(CatalogTable, SaveMode, LogicalPlan)] = plan match {
    case t: CreateDataSourceTableAsSelectCommand => Some(t.table, t.mode, t.query)
    case _ => None
  }
}

class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case _ : ReturnAnswer => Nil
      // load data / insert into
      case loadData: LoadDataCommand
        if isCarbonTable(loadData.table) =>
        ExecutedCommandExec(DMLHelper.loadData(loadData, sparkSession)) :: Nil
      case InsertIntoCarbonTable(relation: CarbonDatasourceHadoopRelation,
      partition, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(CarbonInsertIntoCommand(relation, child, overwrite, partition)) :: Nil
      case InsertIntoHadoopFsRelationCommand(
      outputPath, staticPartitions, ifPartitionNotExists, partitionColumns,
      bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex, outputColumnNames)
        if catalogTable.isDefined && isCarbonTable(catalogTable.get.identifier) =>
        DataWritingCommandExec(
          CarbonInsertIntoHadoopFsRelationCommand(
            outputPath, staticPartitions, ifPartitionNotExists, partitionColumns,
            bucketSpec, fileFormat, options, query, mode, catalogTable, fileIndex,
            outputColumnNames),
          planLater(query)) :: Nil
      // alter table
      case renameTable: AlterTableRenameCommand
        if isCarbonTable(renameTable.oldName) =>
        ExecutedCommandExec(DDLHelper.renameTable(renameTable)) :: Nil
      case compaction: CarbonAlterTableCompactionCommand =>
        CarbonPlanHelper.compact(compaction, sparkSession)
      case changeColumn: AlterTableChangeColumnCommand
        if isCarbonTable(changeColumn.tableName) =>
        ExecutedCommandExec(DDLHelper.changeColumn(changeColumn, sparkSession)) :: Nil
      case colRenameDataTypeChange: CarbonAlterTableColRenameDataTypeChangeCommand =>
        CarbonPlanHelper.changeColumn(colRenameDataTypeChange, sparkSession)
      case addColumns: AlterTableAddColumnsCommand if isCarbonTable(addColumns.table) =>
        ExecutedCommandExec(DDLHelper.addColumns(addColumns, sparkSession)):: Nil
      case addColumn: CarbonAlterTableAddColumnCommand =>
        CarbonPlanHelper.addColumn(addColumn, sparkSession)
      case dropColumn: CarbonAlterTableDropColumnCommand
        if isCarbonTable(TableIdentifier(
          dropColumn.alterTableDropColumnModel.tableName,
          dropColumn.alterTableDropColumnModel.databaseName)) =>
        CarbonPlanHelper.dropColumn(dropColumn, sparkSession)
      case AlterTableSetLocationCommand(tableName, _, _) if isCarbonTable(tableName) =>
        throw new UnsupportedOperationException("Set partition location is not supported")
      // partition
      case showPartitions: ShowPartitionsCommand if isCarbonTable(showPartitions.tableName) =>
        ExecutedCommandExec(DDLHelper.showPartitions(showPartitions, sparkSession)) :: Nil
      case dropPartition: AlterTableDropPartitionCommand
        if isCarbonTable(dropPartition.tableName) =>
        ExecutedCommandExec(DDLHelper.dropPartition(dropPartition)) :: Nil
      case renamePartition: AlterTableRenamePartitionCommand
        if isCarbonTable(renamePartition.tableName) =>
        throw new UnsupportedOperationException("Renaming partition on table is not supported")
      case addPartition: AlterTableAddPartitionCommand
        if isCarbonTable(addPartition.tableName) =>
        ExecutedCommandExec(DDLHelper.addPartition(addPartition)) :: Nil
      // set/unset/reset
      case set: SetCommand =>
        ExecutedCommandExec(CarbonSetCommand(set)) :: Nil
      case MatchResetCommand(_) =>
        ExecutedCommandExec(CarbonResetCommand()) :: Nil
      case setProperties: AlterTableSetPropertiesCommand
        if isCarbonTable(setProperties.tableName) =>
        ExecutedCommandExec(DDLHelper.setProperties(setProperties, sparkSession)) :: Nil
      case unsetProperties: AlterTableUnsetPropertiesCommand
        if isCarbonTable(unsetProperties.tableName) =>
        ExecutedCommandExec(DDLHelper.unsetProperties(unsetProperties)) :: Nil
      // create/describe/drop table
      case createTable: CreateTableCommand
        if createTable.table.provider.isDefined
           && DDLUtils.HIVE_PROVIDER == createTable.table.provider.get
           && createTable.table.storage.serde.get == "org.apache.carbondata.hive.CarbonHiveSerDe" =>
        ExecutedCommandExec(DDLHelper.createHiveTable(createTable, sparkSession)) :: Nil
      case ctas: CreateHiveTableAsSelectCommand
        if ctas.tableDesc.provider.isDefined
           && DDLUtils.HIVE_PROVIDER == ctas.tableDesc.provider.get
           && ctas.tableDesc.storage.serde.get == "org.apache.carbondata.hive.CarbonHiveSerDe" =>
        ExecutedCommandExec(
          DDLHelper.createHiveTableAsSelect(ctas, sparkSession)
        ) :: Nil
      case showCreateTable: ShowCreateTableCommand
        if isCarbonTable(showCreateTable.table) =>
        ExecutedCommandExec(CarbonShowCreateTableCommand(showCreateTable)) :: Nil
      case createLikeTable: CreateTableLikeCommand
        if isCarbonTable(createLikeTable.sourceTable)=>
        throw new MalformedCarbonCommandException(
          "Operation not allowed, when source table is carbon table")
      case truncateTable: TruncateTableCommand
        if isCarbonTable(truncateTable.tableName) =>
        ExecutedCommandExec(CarbonTruncateCommand(truncateTable)) :: Nil
      case createTable@org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, _, None)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
          && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        ExecutedCommandExec(DDLHelper.createDataSourceTable(createTable, sparkSession)) :: Nil
      case MatchCreateDataSourceTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        ExecutedCommandExec(
          DDLHelper.createDataSourceTableAsSelect(tableDesc, query, mode, sparkSession)
        ) :: Nil
      case org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        ExecutedCommandExec(
          DDLHelper.createDataSourceTableAsSelect(tableDesc, query.get, mode, sparkSession)
        ) :: Nil
      case createTable@CreateDataSourceTableCommand(table, _)
        if table.provider.get != DDLUtils.HIVE_PROVIDER
          && (table.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || table.provider.get.equalsIgnoreCase("carbondata")) =>
        ExecutedCommandExec(
          DDLHelper.createDataSourceTable(createTable, sparkSession)
        ) :: Nil
      case desc: DescribeTableCommand if isCarbonTable(desc.table) =>
        ExecutedCommandExec(DDLHelper.describeTable(desc, sparkSession)) :: Nil
      case DropTableCommand(identifier, ifNotExists, _, _)
        if isCarbonTable(identifier) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database, identifier.table.toLowerCase)
        ) :: Nil
      // refresh
      case refreshTable: RefreshTable =>
        ExecutedCommandExec(DDLHelper.refreshTable(refreshTable)) :: Nil
      case refreshResource: RefreshResource =>
        DDLHelper.refreshResource(refreshResource)
      // database
      case createDb: CreateDatabaseCommand =>
        ExecutedCommandExec(DDLHelper.createDatabase(createDb, sparkSession)) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, _)
        if CarbonEnv.databaseLocationExists(dbName, sparkSession, ifExists) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      // explain
      case explain : ExplainCommand =>
        DDLHelper.explain(explain, sparkSession)
      case showTables : ShowTablesCommand =>
        DDLHelper.showTables(showTables, sparkSession)
      case _ => Nil
    }
  }

  def isCarbonTable(tableIdent: TableIdentifier): Boolean = {
    CarbonPlanHelper.isCarbonTable(tableIdent, sparkSession)
  }

}
