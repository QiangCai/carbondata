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
package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.ShowIndexesCommand
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.command._
import org.apache.spark.sql.execution.command.AlterTableModel

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.spark.util.CommonUtil
/**
 * sql parse for secondary index related commands
 */
class CarbonInternalSpark2SqlParser extends CarbonSpark2SqlParser {

  protected val INDEX = carbonInternalKeyWord("INDEX")
  protected val INDEXES = carbonInternalKeyWord("INDEXES")
  override protected val ON = carbonInternalKeyWord("ON")
  protected val REGISTER = carbonKeyWord("REGISTER")

  override def parse(input: String): LogicalPlan = {
    synchronized {
      // Initialize the Keywords.
      initLexical
      phrase(internalStartCommand)(new lexical.Scanner(input)) match {
        case Success(plan, _) => plan match {
          case x: CreateIndexTable =>
            x.inputSqlString = input
            x
          case x: ShowIndexesCommand =>
            x.showIndexSql = input
            x
          case x: DropIndex =>
            x.dropIndexSql = input
            x
          case x: RegisterIndexTableCommand =>
            x.registerSql = input
            x
          case x: SIRebuildSegmentCommand =>
            x.rebuildSIsql = input
            x
          case _ => super.parse(input)
        }
        case failureOrError =>
          sys.error(failureOrError.toString)
      }
    }
  }

  /**
   * This will convert key word to regular expression.
   *
   * @param keys
   * @return
   */
  private def carbonInternalKeyWord(keys: String) = {
    ("(?i)" + keys).r
  }

  protected lazy val internalStartCommand: Parser[LogicalPlan] = indexCommands | start

  protected lazy val indexCommands: Parser[LogicalPlan] =
    showIndexes | createIndexTable | dropIndexTable | registerIndexes | rebuildIndex

  protected lazy val createIndexTable: Parser[LogicalPlan] =
    CREATE ~> INDEX ~> ident ~ (ON ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    ("(" ~> repsep(ident, ",") <~ ")") ~ (AS ~> stringLit) ~
    (TBLPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case indexTableName ~ table ~ cols ~ indexStoreType ~ tblProp =>

        if (!("carbondata".equalsIgnoreCase(indexStoreType) ||
              "org.apache.carbondata.format".equalsIgnoreCase(indexStoreType))) {
          sys.error("Not a carbon format request")
        }

        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }

        val tableProperties = if (tblProp.isDefined) {
          val tblProps = tblProp.get.map(f => f._1 -> f._2)
          scala.collection.mutable.Map(tblProps: _*)
        } else {
          scala.collection.mutable.Map.empty[String, String]
        }
        // validate the tableBlockSize from table properties
        CommonUtil.validateSize(tableProperties, CarbonCommonConstants.TABLE_BLOCKSIZE)
        // validate for supported table properties
        validateTableProperties(tableProperties)
        // validate column_meta_cache proeperty if defined
        val tableColumns: List[String] = cols.map(f => f.toLowerCase)
        validateColumnMetaCacheAndCacheLevelProeprties(dbName,
          indexTableName.toLowerCase,
          tableColumns,
          tableProperties)
        validateColumnCompressorProperty(tableProperties
          .getOrElse(CarbonCommonConstants.COMPRESSOR, null))
        val indexTableModel = SecondaryIndex(dbName,
          tableName.toLowerCase,
          tableColumns,
          indexTableName.toLowerCase)
        CreateIndexTable(indexTableModel, tableProperties)
    }

  private def validateColumnMetaCacheAndCacheLevelProeprties(dbName: Option[String],
      tableName: String,
      tableColumns: Seq[String],
      tableProperties: scala.collection.mutable.Map[String, String]): Unit = {
    // validate column_meta_cache property
    if (tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
      CommonUtil.validateColumnMetaCacheFields(
        dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        tableName,
        tableColumns,
        tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).get,
        tableProperties)
    }
    // validate cache_level property
    if (tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).isDefined) {
      CommonUtil.validateCacheLevel(
        tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).get,
        tableProperties)
    }
  }

  private def validateColumnCompressorProperty(columnCompressor: String): Unit = {
    // Add validatation for column compressor when creating index table
    try {
      if (null != columnCompressor) {
        CompressorFactory.getInstance().getCompressor(columnCompressor)
      }
    } catch {
      case ex: UnsupportedOperationException =>
        throw new InvalidConfigurationException(ex.getMessage)
    }
  }

 /**
  * this method validates if index table properties contains other than supported ones
  *
  * @param tableProperties
  */
  private def validateTableProperties(tableProperties: scala.collection.mutable.Map[String,
    String]) = {
    val supportedPropertiesForIndexTable = Seq("TABLE_BLOCKSIZE",
      "COLUMN_META_CACHE",
      "CACHE_LEVEL",
      CarbonCommonConstants.COMPRESSOR.toUpperCase)
    tableProperties.foreach { property =>
      if (!supportedPropertiesForIndexTable.contains(property._1.toUpperCase)) {
        val errorMessage = "Unsupported Table property in index creation: " + property._1.toString
        throw new MalformedCarbonCommandException(errorMessage)
      }
    }
  }

  protected lazy val dropIndexTable: Parser[LogicalPlan] =
    DROP ~> INDEX ~> opt(IF ~> EXISTS) ~ ident ~ (ON ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case ifexist ~ indexTableName ~ table =>
        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        DropIndex(ifexist.isDefined, dbName, indexTableName.toLowerCase, tableName)
    }

  protected lazy val showIndexes: Parser[LogicalPlan] =
    (SHOW ~> opt(FORMATTED)) ~> (INDEXES | INDEX) ~> ON ~> ident ~ opt((FROM | IN) ~> ident) <~
    opt(";") ^^ {
      case tableName ~ databaseName =>
        ShowIndexesCommand(databaseName, tableName.toLowerCase)
    }

  protected lazy val registerIndexes: Parser[LogicalPlan] =
    REGISTER ~> INDEX ~> TABLE ~> ident ~ (ON ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case indexTable ~ table =>
        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        RegisterIndexTableCommand(dbName, indexTable, tableName)
    }

  protected lazy val rebuildIndex: Parser[LogicalPlan] =
    REBUILD ~> INDEX ~> (ident <~ ".").? ~ ident ~
    (WHERE ~> (SEGMENT ~ "." ~ ID) ~> IN ~> "(" ~> repsep(segmentId, ",") <~ ")").? <~
    opt(";") ^^ {
      case dbName ~ table ~ segs =>
        val alterTableModel =
          AlterTableModel(CarbonParserUtil.convertDbNameToLowerCase(dbName), table, None, null,
            Some(System.currentTimeMillis()), null, segs)
        SIRebuildSegmentCommand(alterTableModel)
    }


}
