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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.sql.SparkACLSqlAstBuilder
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand}
import org.apache.spark.sql.execution.command.table.CarbonShowTablesCommand
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.DecimalType

trait SqlAstBuilderHelper extends SparkACLSqlAstBuilder {

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


  def visitAddTableColumns(parser: CarbonSpark2SqlParser, ctx: AddTableColumnsContext): LogicalPlan = {

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
