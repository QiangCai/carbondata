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

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{SparkACLSqlAstBuilder, SparkSession}

import scala.collection.mutable

class CarbonHelperACLSqlAstBuilder(conf: SQLConf,
  parser: CarbonSpark2SqlParser,
  sparkSession: SparkSession)
  extends SparkACLSqlAstBuilder(conf) {

  // For uquery, if it is external table, only parquet or orc is allowed
  def validateFileFormat(
    tableHeader: CreateTableHeaderContext,
    locationSpec: LocationSpecContext,
    createFileFormat: CreateFileFormatContext): Unit = {
    val (_, _, _, external) = visitCreateTableHeader(tableHeader)
    val location = Option(locationSpec).map(visitLocationSpec)
    Option(createFileFormat).foreach { ctx =>
      (ctx.fileFormat, ctx.storageHandler) match {
        // STORED AS
        case (c: GenericFileFormatContext, null) =>
          val source = c.identifier.getText
          // If location is defined, we'll assume this is an external table.
          if (external || location.isDefined) {
            if (!source.equalsIgnoreCase("parquet") && !source.equalsIgnoreCase("orc")) {
              operationNotAllowed(
                "Only support 'STORED AS' PARQUET/ORC for external table.", ctx
              )
            }
          } else {
            operationNotAllowed(
              "'STORED AS' FileFormat is not supported for internal table.", ctx
            )
          }
        // STORED AS
        case (null, s: StorageHandlerContext) =>
          operationNotAllowed(
            "'STORED AS' StorageHandler is not supported for internal table.", ctx
          )
        case _ =>
      }
    }

  }

  /**
   * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
   */
  override def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    CarbonSparkSqlParserUtil.visitPropertyKeyValues(ctx, props)
  }

  def getPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String]
  = {
    Option(ctx).map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
  }

  def createCarbonTable(createTableTuple: (CreateTableHeaderContext, SkewSpecContext,
    BucketSpecContext, ColTypeListContext, ColTypeListContext, TablePropertyListContext,
    LocationSpecContext, Option[String], TerminalNode, QueryContext, String)): LogicalPlan = {

    val (tableHeader, skewSpecContext,
    bucketSpecContext,
    partitionColumns,
    columns,
    tablePropertyList,
    locationSpecContext,
    tableComment,
    ctas,
    query,
    provider) = createTableTuple

    val (tableIdentifier, temp, ifNotExists, external) = visitCreateTableHeader(tableHeader)
    val cols: Seq[StructField] = Option(columns).toSeq.flatMap(visitColTypeList)
    val colNames: Seq[String] = CarbonSparkSqlParserUtil
      .validateCreateTableReqAndGetColumns(tableHeader,
        skewSpecContext,
        bucketSpecContext,
        columns,
        cols,
        tableIdentifier,
        temp)

    val tablePath: Option[String] = if (locationSpecContext != null) {
      Some(visitLocationSpec(locationSpecContext))
    } else {
      None
    }

    val tableProperties = mutable.Map[String, String]()
    val properties = getPropertyKeyValues(tablePropertyList)
    properties.foreach { property => tableProperties.put(property._1, property._2) }

    // validate partition clause
    val partitionByStructFields = Option(partitionColumns).toSeq.flatMap(visitColTypeList)
    val partitionFields = CarbonSparkSqlParserUtil.
      validatePartitionFields(partitionColumns, colNames, tableProperties,
        partitionByStructFields)

    // validate for create table as select
    val selectQuery = Option(query).map(plan)
    val extraTableTuple = (cols, external, tableIdentifier, ifNotExists, colNames, tablePath,
      tableProperties, properties, partitionByStructFields, partitionFields,
      parser, sparkSession, selectQuery)
    CarbonSparkSqlParserUtil
      .createCarbonTable(createTableTuple, extraTableTuple)
  }

}

