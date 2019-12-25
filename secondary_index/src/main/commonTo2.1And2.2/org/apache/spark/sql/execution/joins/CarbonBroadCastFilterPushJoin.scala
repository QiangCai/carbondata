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
package org.apache.spark.sql.execution.joins

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport}

trait CarbonBroadCastFilterPushJoin extends BinaryExecNode with HashJoin with CodegenSupport {

  protected def setCopyResult(ctx: CodegenContext, needCopyResult: Boolean): Unit = {
    ctx.copyResult = needCopyResult
  }

  protected def addMutableStateToContext(ctx: CodegenContext,
    className: String,
    broadcast: String): String = {
    val relationTerm = ctx.freshName("relation")
    val x = ctx.addMutableState(className, relationTerm,
      s"""
         | $relationTerm = (($className) $broadcast.value()).asReadOnlyCopy();
         | incPeakExecutionMemory($relationTerm.estimatedSize());
       """.stripMargin)
    relationTerm
  }

  protected def performJoinOperation(sparkContext: SparkContext,
    streamedPlanOutput: RDD[InternalRow],
    broadcastRelation: Broadcast[HashedRelation],
    numOutputRows: SQLMetric): RDD[InternalRow] = {
    streamedPlanOutput.mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashedRelation.estimatedSize)
      join(streamedIter, hashedRelation, numOutputRows)
    }
  }

  protected def getBuildPlan : RDD[InternalRow] = {
    buildPlan match {
      case b@CarbonBroadcastExchangeExec(_, _) => b.asInstanceOf[BroadcastExchangeExec].child
        .execute()
      case _ => buildPlan.children.head match {
        case a@CarbonBroadcastExchangeExec(_, _) => a.asInstanceOf[BroadcastExchangeExec].child
          .execute()
        case ReusedExchangeExec(_, broadcast@CarbonBroadcastExchangeExec(_, _)) =>
          broadcast.asInstanceOf[BroadcastExchangeExec].child.execute()
        case _ => buildPlan.execute
      }
    }
  }

}
