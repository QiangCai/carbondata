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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{BroadcastQueryStage, BroadcastQueryStageInput}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport}

/**
  * trait for handling Broadcast filter join related operations
  */
trait CarbonBroadCastFilterPushJoin extends BinaryExecNode with HashJoin with CodegenSupport {

  private var copyResult: Boolean = false

  override def needCopyResult: Boolean = {
    copyResult
  }

  protected def setCopyResult(ctx: CodegenContext, needCopyResult: Boolean): Unit = {
    copyResult = needCopyResult
  }

  protected def addMutableStateToContext(ctx: CodegenContext,
    className: String,
    broadcast: String): String = {
    ctx.addMutableState(className, "relation",
      x =>
        s"""
           | $x = (($className) $broadcast.value()).asReadOnlyCopy();
           | incPeakExecutionMemory($x.estimatedSize());
       """.stripMargin)
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
      case c@CarbonBroadcastQueryStageInput(_, _) => c.asInstanceOf[BroadcastQueryStageInput]
        .childStage.child.asInstanceOf[BroadcastExchangeExec].child.execute()
      case _ => buildPlan.children.head match {
        case a@CarbonBroadcastExchangeExec(_, _) => a.asInstanceOf[BroadcastExchangeExec].child
          .execute()
        case c@CarbonBroadcastQueryStageInput(_, _) => c.asInstanceOf[BroadcastQueryStageInput]
          .childStage.child.asInstanceOf[BroadcastExchangeExec].child.execute()
        case ReusedExchangeExec(_, broadcast@CarbonBroadcastExchangeExec(_, _)) =>
          broadcast.asInstanceOf[BroadcastExchangeExec].child.execute()
        case ReusedExchangeExec(_, broadcast@CarbonBroadcastQueryStageInput(_, _)) =>
          broadcast.asInstanceOf[BroadcastQueryStageInput].childStage.child
            .asInstanceOf[BroadcastExchangeExec].child.execute()
        case _ => buildPlan.execute
      }
    }
  }

}

/**
  * unapply method of BroadcastQueryStageInput
  */
object CarbonBroadcastQueryStageInput {
  def unapply(plan: SparkPlan): Option[(BroadcastQueryStage, Seq[Attribute])] = {
    plan match {
      case cExec: BroadcastQueryStageInput =>
        Some(cExec.childStage, cExec.output)
      case _ => None
    }
  }
}