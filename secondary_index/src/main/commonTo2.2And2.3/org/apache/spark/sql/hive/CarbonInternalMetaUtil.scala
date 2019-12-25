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

import org.apache.hadoop.security.UserGroupInformation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.spark.sql.CarbonExpressions.{CarbonSubqueryAlias => SubqueryAlias}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ResolvedHint}
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.hive.acl.ACLInterface
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.util.CarbonInternalScalaUtil
/**
 *
 */
object CarbonInternalMetaUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * return's <true> if can push down join
    *
   * @param otherRDDPlan
   * @param session
   * @return
   */
  def canPushDown(otherRDDPlan: LogicalPlan,
    session: SparkSession): Boolean = {
    otherRDDPlan match {
      case ResolvedHint(p, hintInfo) => true
      case p if session.sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
                SparkSQLUtil.invokeStatsMethod(p, session.sqlContext.conf).sizeInBytes <=
                session.sqlContext.conf.autoBroadcastJoinThreshold =>
        LOGGER
          .info("canPushDownJoin statistics:" +
                SparkSQLUtil.invokeStatsMethod(p, session.sqlContext.conf).sizeInBytes)
        true
      case plan if (CarbonInternalScalaUtil.checkIsIndexTable(plan)) => true
      case _ => false
    }
  }

  def retrieveRelation(plan: LogicalPlan)(sparkSession: SparkSession):
  CarbonDatasourceHadoopRelation = {
    plan match {
      case SubqueryAlias(alias, l: UnresolvedCatalogRelation) =>
        val logicalPlan = new FindDataSourceTable(sparkSession).apply(l).collect {
          case lr: LogicalRelation => lr
        }
        if (logicalPlan.head.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          logicalPlan.head.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        } else {
          null
        }
      case SubqueryAlias(alias, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case l: LogicalRelation if l.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation] => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case _ => null
    }
  }

  def retrievePlan(plan: LogicalPlan)(sparkSession: SparkSession):
  LogicalRelation = {
    plan match {
      case SubqueryAlias(alias, l: UnresolvedCatalogRelation) =>
        val logicalPlan = new FindDataSourceTable(sparkSession).apply(l).collect {
          case lr: LogicalRelation => lr
        }
        if (logicalPlan.head.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) {
          logicalPlan.head
        } else {
          null
        }
      case SubqueryAlias(alias, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case _ => null
    }
  }

  /**
   * return's aclInterface from CarbonACLSessionCatalog
   *
   * @param sparkSession
   * @return
   */
  def getACLInterface(sparkSession: SparkSession) : ACLInterface = {
    sparkSession.sessionState.catalog.asInstanceOf[CarbonACLSessionCatalog].getACLInterface()
  }

  /**
    *
    * @param sparkSession
    * @return
    */
  def getClientUser(sparkSession: SparkSession): String = {
    sparkSession
      .sessionState
      .catalog
      .extendedProperties
      .get("owner")
      .map(_.asInstanceOf[String])
      .getOrElse(UserGroupInformation.getCurrentUser.getShortUserName)
  }
}
