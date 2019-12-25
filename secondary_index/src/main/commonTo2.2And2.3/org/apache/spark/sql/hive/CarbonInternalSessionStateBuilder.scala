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

import java.util.concurrent.Callable

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation
import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.acl._
import org.apache.spark.sql.catalyst.{CarbonParserUtil, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.InternalDDLStrategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand}
import org.apache.spark.sql.execution.command.table.CarbonShowTablesCommand
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel}
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, PreprocessTableCreation, PreprocessTableInsertion, _}
import org.apache.spark.sql.execution.strategy.{CarbonInternalLateDecodeStrategy, CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.helper.SparkObjectCreationHelper
import org.apache.spark.sql.hive.acl.ACLInterface
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState, SparkSessionListener}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonSITransformationRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser._
import org.apache.spark.sql.types.DecimalType


/**
  * This class will have carbon catalog and refresh the relation from cache if the carbontable in
  * carbon catalog is not same as cached carbon relation's carbon table
  *
  * @param externalCatalog
  * @param globalTempViewManager
  * @param sparkSession
  * @param functionResourceLoader
  * @param functionRegistry
  * @param conf
  * @param hadoopConf
  */
class CarbonACLSessionCatalog(
  externalCatalog: HiveACLExternalCatalog,
  globalTempViewManager: GlobalTempViewManager,
  functionRegistry: FunctionRegistry,
  sparkSession: SparkSession,
  conf: SQLConf,
  hadoopConf: Configuration,
  parser: ParserInterface,
  functionResourceLoader: FunctionResourceLoader)
  extends HiveACLSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) with CarbonSessionCatalog {
  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  var hiveClient: HiveClient = null
  var aclInterface: ACLInterface = null

  def setHiveClient(client: HiveClient): Unit = {
    hiveClient = client
  }

  /**
   * to set acl interface
   * @param aCLInterface
   */
  def setACLInterface(aCLInterface: ACLInterface): Unit = {
    aclInterface = aCLInterface
  }
  /**
   * return's the corbonEnv instance
    *
    * @return
   */
  override def getCarbonEnv(): CarbonEnv = {
    carbonEnv
  }

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    var rtnRelation: LogicalPlan =
      try {
        super.lookupRelation(name)
      } catch {
        case ex: Exception =>
          if (ex.getMessage.contains("Permission denied")) {
            throw new AnalysisException("Missing Privileges")
          }
          throw ex
      }
    val isRelationRefreshed =
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    if (isRelationRefreshed) {
      rtnRelation = super.lookupRelation(name)
      // Reset the stats after lookup.
      CarbonSessionUtil.refreshRelationAndSetStats(rtnRelation, name)(sparkSession)
    }
    rtnRelation
  }

  override def getCachedPlan(t: QualifiedTableName,
      c: Callable[LogicalPlan]): LogicalPlan = {
    val plan = super.getCachedPlan(t, c)
    CarbonSessionUtil.updateCachedPlan(plan)
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    hiveClient
  }

  /**
   * Return's the aclinterface
   * @return
   */
  def getACLInterface(): ACLInterface = {
    aclInterface
  }


  override def createPartitions(
    tableName: TableIdentifier,
    parts: Seq[CatalogTablePartition],
    ignoreIfExists: Boolean): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      val updatedParts = CarbonScalaUtil.updatePartitions(parts, table)
      super.createPartitions(tableName, updatedParts, ignoreIfExists)
    } catch {
      case e: Exception =>
        super.createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   *
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  override def getPartitionsAlternate(partitionFilters: Seq[Expression], sparkSession: SparkSession,
    identifier: TableIdentifier): Seq[CatalogTablePartition] = {
    CarbonSessionUtil.prunePartitionsByFilter(partitionFilters, sparkSession, identifier)
  }
  /**
   * Update the storageformat with new location information
   */
  override def updateStorageLocation(
    path: Path,
    storage: CatalogStorageFormat,
      newTableName: String,
      dbName: String): CatalogStorageFormat = {
    storage.copy(locationUri = Some(path.toUri))
  }

  override def alterAddColumns(tableIdentifier: TableIdentifier,
    schemaParts: String,
    cols: Option[Seq[ColumnSchema]]): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols)
  }

  override def alterDropColumns(tableIdentifier: TableIdentifier,
    schemaParts: String,
    cols: Option[Seq[ColumnSchema]]): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols)
  }

  override def alterColumnChangeDataTypeOrRename(tableIdentifier: TableIdentifier,
      schemaParts: String,
      cols: Option[Seq[ColumnSchema]]): Unit = {
    updateCatalogTableForAlter(tableIdentifier, schemaParts, cols)
  }

  /**
   * This method alters table to set serde properties and updates the catalog table with new updated
   * schema for all the alter operations like add column, drop column, change datatype or rename
   * column
   * @param tableIdentifier
   * @param schemaParts
   * @param cols
   */
  private def updateCatalogTableForAlter(tableIdentifier: TableIdentifier,
    schemaParts: String,
    cols: Option[Seq[ColumnSchema]]) = {
    alterTable(tableIdentifier, schemaParts, cols)
    CarbonSessionUtil
      .alterExternalCatalogForTableWithUpdatedSchema(tableIdentifier,
        cols,
        schemaParts,
        sparkSession)
  }
}


class CarbonACLInternalSessionStateBuilder(sparkSession: SparkSession,
  parentState: Option[SessionState] = None)
  extends HiveACLSessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser: ParserInterface = new CarbonInternalSparkSqlParser(conf,
    sparkSession
  )

  experimentalMethods.extraStrategies =
    Seq(new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession),
      new CarbonInternalLateDecodeStrategy,
      new InternalDDLStrategy(sparkSession)
    )

  experimentalMethods.extraOptimizations = Seq(new CarbonIUDRule,
    new CarbonUDFTransformRule,
    new CarbonSITransformationRule(sparkSession),
    new CarbonLateDecodeRule
  )

  var preExecutionRules: Rule[SparkPlan] =
    CarbonPrivCheck(sparkSession, catalog, aclInterface)

  override def getPreOptimizerRules: Seq[Rule[LogicalPlan]] = {
    super.getPreOptimizerRules ++ Seq(new CarbonPreOptimizerRule)
  }

  /**
    * Internal catalog for managing table and database states.
    */
  /**
    * Create a [CarbonSessionCatalogBuild].
    */
  override protected lazy val catalog: CarbonACLSessionCatalog = {
    val catalog = new CarbonACLSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      sparkSession,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader
    )
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: HiveACLExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveACLExternalCatalog]

  //override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override protected def analyzer: Analyzer = new CarbonAnalyzer(catalog,
    conf,
    sparkSession,
    getAnalyzer(super.analyzer))

  /**
   * This method adds carbon rules to Hive Analyzer and returns new analyzer
   * @param analyzer HiveACLSessionStateBuilder analyzer
   * @return new analyzer
   */
  def getAnalyzer(analyzer: Analyzer): Analyzer = {
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        Seq(CarbonAccessControlRules(sparkSession, catalog, aclInterface)) ++
        analyzer.extendedResolutionRules ++
        Seq(CarbonIUDAnalysisRule(sparkSession)) ++
        Seq(CarbonPreInsertionCasts(sparkSession)) ++ customResolutionRules

      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
        analyzer.extendedCheckRules

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        analyzer.postHocResolutionRules
    }
  }

  // Register all the required listeners using the singleton instance as the listeners
  // need to be registered only once
  CarbonCommonInitializer.init()

  /**
   * Listener on session to handle clean during close session.
   */
  class CarbonSessionListener(sparkSession: SparkSession, sessionState:
  SessionState) extends SparkSessionListener {

    override def closeSession(): Unit = {
      CarbonUserGroupInformation.cleanUpUGIFromSession(sparkSession)

      // Remove the listener from session state
      sessionState.sessionStateListenerManager.removeListener(this)
    }
  }

  override def build(): SessionState = {
    val state = super.build()
    state.preExecutionRules = state.preExecutionRules :+ preExecutionRules
    catalog.setHiveClient(client)
    catalog.setACLInterface(aclInterface)
    state.sessionStateListenerManager.addListener(new CarbonSessionListener(sparkSession, state))
    state
  }

  override protected def newBuilder: NewBuilder = new CarbonACLInternalSessionStateBuilder(_, _)
}

class CarbonInternalSqlAstBuilder(conf: SQLConf, parser: CarbonInternalSpark2SqlParser,
  sparkSession: SparkSession) extends CarbonACLSqlAstBuilder(conf, parser, sparkSession) {

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }
}

class CarbonACLSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser,
  sparkSession: SparkSession) extends SparkACLSqlAstBuilder(conf) {

  val helper = new CarbonHelperACLSqlAstBuilder(conf, parser, sparkSession)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = {
    val fileStorage = CarbonSparkSqlParserUtil
      .getFileStorage(SparkObjectCreationHelper.getFileFormat(ctx))

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
        fileStorage.equalsIgnoreCase("carbondata") ||
        fileStorage.equalsIgnoreCase("'carbonfile'") ||
        fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      SparkObjectCreationHelper.createTableTuple(ctx, helper, fileStorage)
    } else {
      super.visitCreateHiveTable(ctx)
    }
  }

  override def visitChangeColumn(ctx: ChangeColumnContext): LogicalPlan = {

    val newColumn = visitColType(ctx.colType)
    val isColumnRename = if (!ctx.identifier.getText.equalsIgnoreCase(newColumn.name)) {
      true
    } else {
      false
    }

    val (typeString, values): (String, Option[List[(Int, Int)]]) = newColumn.dataType match {
      case d: DecimalType => ("decimal", Some(List((d.precision, d.scale))))
      case _ => (newColumn.dataType.typeName.toLowerCase, None)
    }

    val alterTableChangeDataTypeModel =
      AlterTableDataTypeChangeModel(
        CarbonParserUtil.parseDataType(typeString, values, isColumnRename),
        CarbonParserUtil.convertDbNameToLowerCase(Option(ctx.tableIdentifier().db).map(_.getText)),
        ctx.tableIdentifier().table.getText.toLowerCase,
        ctx.identifier.getText.toLowerCase,
        newColumn.name.toLowerCase,
        isColumnRename
      )

    CarbonAlterTableColRenameDataTypeChangeCommand(alterTableChangeDataTypeModel)
  }


  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = {

    val cols = Option(ctx.columns).toSeq.flatMap(visitColTypeList)
    val fields = parser.getFields(cols)
    val tblProperties = scala.collection.mutable.Map.empty[String, String]
    val tableModel = CarbonParserUtil.prepareTableModel(false,
      CarbonParserUtil.convertDbNameToLowerCase(Option(ctx.tableIdentifier().db).map(_.getText)
      ),
      ctx.tableIdentifier.table.getText.toLowerCase,
      fields,
      Seq.empty,
      tblProperties,
      None,
      true
    )

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      Option(ctx.tableIdentifier().db).map(_.getText),
      ctx.tableIdentifier.table.getText,
      tblProperties.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highcardinalitydims.getOrElse(Seq.empty)
    )

    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }

  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = {
    withOrigin(ctx) {
      if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,
          CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT).toBoolean) {
        super.visitShowTables(ctx)
      } else {
        CarbonShowTablesCommand(
          Option(ctx.db).map(_.getText),
          Option(ctx.pattern).map(string))
      }
    }
  }
}
