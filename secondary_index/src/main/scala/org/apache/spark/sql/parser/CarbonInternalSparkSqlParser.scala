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

import org.apache.spark.sql.{CarbonEnv, CarbonSession, CarbonUtils, SparkSession}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hive.CarbonInternalSqlAstBuilder
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.util.{CarbonSessionInfo, ThreadLocalSessionInfo}

/**
 *
 */
class CarbonInternalSparkSqlParser(conf: SQLConf, sparkSession: SparkSession)
  extends CarbonSparkSqlParser(conf, sparkSession) {

  override val parser = new CarbonInternalSpark2SqlParser
  override val astBuilder = new CarbonInternalSqlAstBuilder(conf, parser, sparkSession)

  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
    CarbonUtils.updateSessionInfoToCurrentThread(sparkSession)
    try {
      super.parsePlan(sqlText)
    } catch {
      case ce: MalformedCarbonCommandException =>
        throw ce
      case ex =>
        try {
          parser.parse(sqlText)
        } catch {
          case mce: MalformedCarbonCommandException =>
            throw mce
          case e =>
            sys
              .error("\n" + "BaseSqlParser>>>> " + ex.getMessage + "\n" + "CarbonSqlParser>>>> " +
                     e.getMessage)
        }
    }
  }

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }
}


