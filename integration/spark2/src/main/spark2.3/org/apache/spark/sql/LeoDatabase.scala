package org.apache.spark.sql

import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Expression, ListQuery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.cache.{CarbonDropCacheCommand, CarbonShowCacheCommand}
import org.apache.spark.sql.execution.command.datamap.{CarbonCreateDataMapCommand, CarbonDataMapRebuildCommand, CarbonDataMapShowCommand, CarbonDropDataMapCommand}
import org.apache.spark.sql.execution.command.management._
import org.apache.spark.sql.execution.command.mutation.{CarbonProjectForDeleteCommand, CarbonProjectForUpdateCommand}
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand, CarbonAlterTableDropPartitionCommand, CarbonAlterTableSplitPartitionCommand, CarbonShowCarbonPartitionsCommand}
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.stream.{CarbonCreateStreamCommand, CarbonDropStreamCommand, CarbonShowStreamsCommand}
import org.apache.spark.sql.execution.datasources.{CreateTable, RefreshTable}

object LeoDatabase {
  var DEFAULT_PROJECTID: String = "defaultprojectid"
  val dbNamePattern = Pattern.compile("^[a-z][a-z0-9]{0,31}$")

  /**
   * convert user table identifier to leo table identifier
   */
  def convertUserTableIdentifierToLeo(userTable: TableIdentifier): TableIdentifier = {
    TableIdentifier(
      userTable.table,
      Some(convertUserDBNameToLeo(userTable.database.get)))
  }

  /**
   * convert user database name to leo database name
   */
  def convertUserDBNameToLeo(userDBName: String): String = {
    validateDBName(userDBName)
    if (userDBName.startsWith(leoDBNamePrefix)) {
      userDBName
    } else {
      leoDBNamePrefix + userDBName
    }
  }

  def validateDBName(userDBName: String): Unit = {
    var toValidate = userDBName
    if (toValidate.startsWith(leoDBNamePrefix)) {
      toValidate = toValidate.substring(leoDBNamePrefix.length)
    }
    // len, char,
    if (!dbNamePattern.matcher(toValidate).matches()) {
      throw new IllegalArgumentException(
        "database name is illegal, it should starts with [a-z], contains [a-z] or [0-9] and the" +
        " total length from 1 to 32 characters")
    }
  }

  /**
   * convert leo database name to user database name
   */
  def convertLeoDBNameToUser(leoDBName: String): String = {
    leoDBName.substring(leoDBNamePrefix.length)
  }

  // TODO: add user projectid here
  def leoDBNamePrefix: String = {
    DEFAULT_PROJECTID + "666"
  }

  /**
   * Return false if plan is invalid, otherwise return updated plan after
   * modifying database name that user gives.
   *
   * Following case the plan is invalid:
   * 1. user does not give database name
   * 2. USE DATABASE command
   */
  def convertUserDBNameToLeoInPlan(parsedPlan: LogicalPlan): (Option[LogicalPlan], String) = {
    // In order to support same DB name for different tenant,
    // we do following checks and modification of the DB name
    // 1. For all commands, ensure database name must exist and not default,
    // otherwise return false
    // 2. USE DATABASE command is not allowed
    // 3. For all commands, add ProjectID prefix in database name after parser
    // 4. For command that expose database name, wrap it by Leo command and
    // remove the ProjectID before return to user

    val updatedPlan = parsedPlan.transform {
      case relation: UnresolvedRelation =>
        if (relation.tableIdentifier.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (relation.tableIdentifier.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        } else {
          UnresolvedRelation(LeoDatabase.convertUserTableIdentifierToLeo(relation.tableIdentifier))
        }

      case filter: Filter => transformFilter(filter)

      case cmd@CreateDatabaseCommand(databaseName, ifNotExists, path, comment, props) =>
        if (databaseName.equals("default")) {
          return (None, "database name default is not allowed")
        }
        val db = LeoDatabase.convertUserDBNameToLeo(databaseName)
        CreateDatabaseCommand(db, ifNotExists, path, comment, props)

      case cmd@RefreshTable(tableIdentifier) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get)))
        RefreshTable(newTable)

      case cmd@DropDatabaseCommand(databaseName, ifExists, cascade) =>
        val db = LeoDatabase.convertUserDBNameToLeo(databaseName)
        DropDatabaseCommand(db, ifExists, cascade)

      case cmd@CreateTable(table, saveMode, query) =>
        requireDBNameNonEmpty(table.identifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          table.identifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.identifier.database.get)))
        CreateTable(table.copy(identifier = newTable), saveMode, query)

      case cmd@DropTableCommand(table, ifExists, isView, purge) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        DropTableCommand(newTable, ifExists, isView, purge)

      case cmd@CreateViewCommand(tableIdentifier, userSpecifiedColumns, comment, properties, originalText, child, allowExisting, replace, viewType) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(
          tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get)))
        CreateViewCommand(newTable, userSpecifiedColumns, comment, properties, originalText, child, allowExisting, replace, viewType)

      case cmd@UpdateTable(table, columns, selectStmt, alias, filer) =>
        requireDBNameNonEmpty(table.tableIdentifier.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTableIdent = new TableIdentifier(
          table.tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.tableIdentifier.database.get)))
        val newTable = table.copy(newTableIdent)
        UpdateTable(newTable, columns, selectStmt, alias, filer)

      case cmd@DeleteRecords(statement, alias, table) =>
        requireDBNameNonEmpty(table.tableIdentifier.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTableIdent = new TableIdentifier(
          table.tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.tableIdentifier.database.get)))
        val newTable = table.copy(newTableIdent)
        DeleteRecords(statement, alias, newTable)

      case cmd@SetDatabaseCommand(_) =>
        return (None, "use database command is not allowed, " +
                      "specify database name in the query instead")

      case cmd@ShowDatabasesCommand(databasePattern) =>
        if (databasePattern.isDefined) {
          return (None, "database pattern is not supported")
        }
        ShowDatabasesCommand(Some(LeoDatabase.leoDBNamePrefix + "*"))

      case cmd@ShowTablesCommand(dbNameOp, t, isExtended, partitionSpec) =>
        requireDBNameNonEmpty(dbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        ShowTablesCommand(
          Some(LeoDatabase.convertUserDBNameToLeo(dbNameOp.get)), t, isExtended, partitionSpec)

      case cmd@DescribeTableCommand(table, partitionSpec, isExtended) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        DescribeTableCommand(
          LeoDatabase.convertUserTableIdentifierToLeo(table), partitionSpec, isExtended)
      case cmd@ExplainCommand(plan, extended, codegen, cost) =>
        val (newPlanOp, msg) = convertUserDBNameToLeoInPlan(plan)
        if (newPlanOp.isEmpty) {
          return (None, msg)
        }
        ExplainCommand(newPlanOp.get, extended, codegen, cost)

      case cmd@CarbonLoadDataCommand(databaseNameOp, tableName, factPathFromUser, dimFilesPath,
      options, isOverwriteTable, inputSqlString, dataFrame, updateModel, tableInfoOp,
      internalOptions, partition, logicalPlan, operationContext) =>
        if (databaseNameOp.get.isEmpty) {
          return (None, "database name must be specified")
        } else if (databaseNameOp.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        CarbonLoadDataCommand(Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)),
          tableName, factPathFromUser, dimFilesPath, options, isOverwriteTable, inputSqlString,
          dataFrame, updateModel, tableInfoOp, internalOptions, partition, logicalPlan,
          operationContext)

      case cmd@LoadDataCommand(identifier, path, isLocal, isOverwrite, partition) =>
        if (identifier.database.isEmpty) {
          return (None, "database name must be specified")
        } else if (identifier.database.get.equals("default")) {
          return (None, "default database is not allowed, create a new database")
        }
        val newTable = new TableIdentifier(
          identifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(identifier.database.get)))
        LoadDataCommand(newTable, path, isLocal, isOverwrite, partition)

      case cmd@InsertIntoTable(table, partition, query, overwrite, ifPartitionNotExists) =>
        if (table.isInstanceOf[UnresolvedRelation]) {
          val newTable = UnresolvedRelation(LeoDatabase.convertUserTableIdentifierToLeo(
            table.asInstanceOf[UnresolvedRelation].tableIdentifier))
          InsertIntoTable(newTable, partition, query, overwrite, ifPartitionNotExists)
        } else {
          cmd
        }


      //////////////////////////////////////////////////
      //                Alter Table                   //
      //////////////////////////////////////////////////

      case cmd@CarbonAlterTableAddColumnCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableAddColumnsModel = newModel)

      case cmd@CarbonAlterTableDropColumnCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableDropColumnModel = newModel)

      case cmd@CarbonAlterTableColRenameDataTypeChangeCommand(model, _) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          databaseName = Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(alterTableColRenameAndDataTypeChangeModel = newModel)

      case cmd@CarbonAlterTableRenameCommand(model) =>
        requireDBNameNonEmpty(model.oldTableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        requireDBNameNonEmpty(model.newTableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val src = model.oldTableIdentifier.copy(
          database = Some(LeoDatabase.convertUserDBNameToLeo(
            model.oldTableIdentifier.database.get)))
        val dest = model.newTableIdentifier.copy(
          database = Some(LeoDatabase.convertUserDBNameToLeo(
            model.newTableIdentifier.database.get)))
        val newModel = model.copy(oldTableIdentifier = src, newTableIdentifier = dest)
        cmd.copy(alterTableRenameModel = newModel)

      case cmd@CarbonAlterTableSetCommand(tableIdentifier, _, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(
          tableIdentifier = new TableIdentifier(
            tableIdentifier.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonAlterTableUnsetCommand(tableIdentifier, _, _, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(
          tableIdentifier = new TableIdentifier(
            tableIdentifier.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonAlterTableCompactionCommand(model, _, _) =>
        requireDBNameNonEmpty(model.dbName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(
          dbName = Some(LeoDatabase.convertUserDBNameToLeo(model.dbName.get)))
        cmd.copy(alterTableModel = newModel)

      case cmd@CarbonAlterTableFinishStreaming(dbNameOp, _) =>
        requireDBNameNonEmpty(dbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(dbName = Some(LeoDatabase.convertUserDBNameToLeo(dbNameOp.get)))


      //////////////////////////////////////////////////
      //                Partition                     //
      //////////////////////////////////////////////////

      case cmd@CarbonAlterTableAddHivePartitionCommand(table, _, _) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        cmd.copy(tableName = newTable)

      case cmd@CarbonAlterTableDropHivePartitionCommand(table, _, _, _, _, _) =>
        requireDBNameNonEmpty(table.database) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newTable = new TableIdentifier(table.table,
          Some(LeoDatabase.convertUserDBNameToLeo(table.database.get)))
        cmd.copy(tableName = newTable)

      case cmd@CarbonAlterTableDropPartitionCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(databaseName =
          Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(model = newModel)

      case cmd@CarbonAlterTableSplitPartitionCommand(model) =>
        requireDBNameNonEmpty(model.databaseName) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        val newModel = model.copy(databaseName =
          Some(LeoDatabase.convertUserDBNameToLeo(model.databaseName.get)))
        cmd.copy(splitPartitionModel = newModel)

      case cmd@CarbonShowCarbonPartitionsCommand(tableIdentifier) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(tableIdentifier = new TableIdentifier(tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      //////////////////////////////////////////////////
      //                   Cache                      //
      //////////////////////////////////////////////////

      case cmd@CarbonDropCacheCommand(tableIdentifier, _) =>
        requireDBNameNonEmpty(tableIdentifier) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(tableIdentifier = new TableIdentifier(tableIdentifier.table,
          Some(LeoDatabase.convertUserDBNameToLeo(tableIdentifier.database.get))))

      case cmd@CarbonShowCacheCommand(tableOp, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      //////////////////////////////////////////////////
      //                   DataMap                    //
      //////////////////////////////////////////////////

      case cmd@CarbonCreateDataMapCommand(_, tableOp, _, _, _, _, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDropDataMapCommand(_, _, tableOp, _) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(table = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDataMapShowCommand(tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case cmd@CarbonDataMapRebuildCommand(_, tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableIdentifier = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      //////////////////////////////////////////////////
      //               Data Management                //
      //////////////////////////////////////////////////

      case cmd@CarbonCleanFilesCommand(databaseNameOp, _, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonCliCommand(databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonDeleteLoadByIdCommand(_, databaseNameOp, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonDeleteLoadByLoadDateCommand(databaseNameOp, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonInsertColumnsCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(dbName = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonShowLoadsCommand(databaseNameOp, _, _, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@RefreshCarbonTableCommand(databaseNameOp, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonProjectForDeleteCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))

      case cmd@CarbonProjectForUpdateCommand(_, databaseNameOp, _, _) =>
        requireDBNameNonEmpty(databaseNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(databaseNameOp = Some(LeoDatabase.convertUserDBNameToLeo(databaseNameOp.get)))


      //////////////////////////////////////////////////
      //                   Streaming                  //
      //////////////////////////////////////////////////

      case cmd@CarbonCreateStreamCommand(_, sinkDbNameOp, _, _, _, _) =>
        requireDBNameNonEmpty(sinkDbNameOp) match {
          case Some(msg) => return (None, msg)
          case None =>
        }
        cmd.copy(sinkDbName = Some(LeoDatabase.convertUserDBNameToLeo(sinkDbNameOp.get)))

      case cmd@CarbonDropStreamCommand(_, _) =>
        cmd

      case cmd@CarbonShowStreamsCommand(tableOp) =>
        if (tableOp.nonEmpty) {
          requireDBNameNonEmpty(tableOp.get) match {
            case Some(msg) => return (None, msg)
            case None =>
          }
          cmd.copy(tableOp = Some(new TableIdentifier(
            tableOp.get.table,
            Some(LeoDatabase.convertUserDBNameToLeo(tableOp.get.database.get)))))
        } else {
          cmd
        }

      case plan => plan
    }

    (Some(updatedPlan), "")
  }

  private def requireDBNameNonEmpty(tableIdentifier: TableIdentifier): Option[String] = {
    if (tableIdentifier.database.isEmpty) {
      return Some("database name must be specified")
    } else if (tableIdentifier.database.get.equals("default")) {
      return Some("default database is not allowed, create a new database")
    }
    None
  }

  private def requireDBNameNonEmpty(databaseNameOp: Option[String]): Option[String] = {
    if (databaseNameOp.isEmpty) {
      return Some("database name must be specified")
    } else if (databaseNameOp.get.equals("default")) {
      return Some("default database is not allowed, create a new database")
    }
    None
  }

  private def transformFilter(filter: Filter): Filter = {
    if (filter.subqueries.nonEmpty) {
      val newCondition = filter.condition.transform {
        case query: ListQuery =>
          val newPlan = query.plan.transform {
            case relation: UnresolvedRelation =>
              UnresolvedRelation(LeoDatabase
                .convertUserTableIdentifierToLeo(relation.tableIdentifier))
            case filter: Filter =>
              transformFilter(filter)
          }
          ListQuery(newPlan, query.children, query.exprId, query.childOutputs)
        case expr: Expression => expr
      }
      Filter(newCondition, filter.child)
    } else {
      filter
    }
  }
}
