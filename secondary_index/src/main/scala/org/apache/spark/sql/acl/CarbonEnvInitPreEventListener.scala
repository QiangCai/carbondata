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

import org.apache.spark.sql.hive.CarbonInternalMetaUtil
import org.apache.spark.util.CarbonInternalReflectionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{CarbonEnvInitPreEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants

/**
 *
 */
class CarbonEnvInitPreEventListener extends OperationEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   * @param operationContext
   */
  override def onEvent(event: Event,
      operationContext: OperationContext): Unit = {
     val carbonEnvInitPreEvent = event.asInstanceOf[CarbonEnvInitPreEvent]
     val storePath = carbonEnvInitPreEvent.storePath
     val sparkSession = carbonEnvInitPreEvent.sparkSession
    LOGGER.info("CarbonEnvInitPreEventListener invoked for session :" + sparkSession.toString)
    val carbonSessionInfo = carbonEnvInitPreEvent.carbonSessionInfo
     CarbonUserGroupInformation.getInstance.enableDriverUser()
    val sessionLevelUGIObject = CarbonUserGroupInformation.getInstance
      .createCurrentUser(CarbonInternalMetaUtil.getClientUser(sparkSession))

    carbonSessionInfo.getNonSerializableExtraInfo
      .put(CarbonInternalCommonConstants.USER_UNIQUE_UGI_OBJECT, sessionLevelUGIObject)
     Utils.initCarbonFoldersPermission(storePath, sparkSession)
    // register position ID UDF
    sparkSession.udf.register("getPositionId", () => "")
    sparkSession.udf.register("NI", (anyRef: AnyRef) => true)
  }
}
