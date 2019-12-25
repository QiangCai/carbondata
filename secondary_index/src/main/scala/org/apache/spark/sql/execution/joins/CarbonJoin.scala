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

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonDecoderRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BindReferences, BoundReference, Expression, In, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, BroadcastMode, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.api.CarbonTableInputFormatExtended
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends CarbonBroadCastFilterPushJoin {

  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    // Here for CarbonPlan, 2 spark plans are wrapped so that it can be broadcasted
    // 1. BroadcastExchange
    // 2. BroadcastExchangeExec
    // Both the relations to be removed to execute and get the output
    val buildPlanOutput = getBuildPlan

    val input: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()
    val inputCopy: Array[InternalRow] = input.clone()
    (input, inputCopy)
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  private lazy val carbonScan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    BroadCastFilterPushJoin.addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide)
    val streamedPlanOutput = streamedPlan.execute()
    // scalastyle:off
    // scalastyle:on
    performJoinOperation(sparkContext, streamedPlanOutput, broadcastRelation, numOutputRows)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    joinType match {
      case _: InnerLike => codegenInner(ctx, input)
      case LeftOuter | RightOuter => codegenOuter(ctx, input)
      case LeftSemi => codegenSemi(ctx, input)
      case LeftAnti => codegenAnti(ctx, input)
      case j: ExistenceJoin => codegenExistence(ctx, input)
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastHashJoin should not take $x as the JoinType")
    }
  }

  /**
   * Returns a tuple of Broadcast of HashedRelation and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Broadcast[HashedRelation], String) = {
    // create a name for HashedRelation
    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    BroadCastFilterPushJoin.addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide)
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    val clsName = broadcastRelation.value.getClass.getName
    val relationTerm = addMutableStateToContext(ctx, clsName, broadcast)
    (broadcastRelation, relationTerm)
  }

  /**
   * Returns the code for generating join key for stream side, and expression of whether the key
   * has any null in it or not.
   */
  private def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (streamedKeys.length == 1 && streamedKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev = streamedKeys.head.genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx, streamedKeys)
      (ev, s"${ ev.value }.anyNull()")
    }
  }

  /**
   * Generates the code for variable of build side.
   */
  private def genBuildSideVars(ctx: CodegenContext, matched: String): Seq[ExprCode] = {
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).genCode(ctx)
      if (joinType.isInstanceOf[InnerLike]) {
        ev
      } else {
        // the variables are needed even there is no matched rows
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val code =
          s"""
             |boolean $isNull = true;
             |${ ctx.javaType(a.dataType) } $value = ${ ctx.defaultValue(a.dataType) };
             |if ($matched != null) {
             |  ${ ev.code }
             |  $isNull = ${ ev.isNull };
             |  $value = ${ ev.value };
             |}
         """.stripMargin
        ExprCode(code, isNull, value)
      }
    }
  }

  /**
   * Generate the (non-equi) condition used to filter joined rows. This is used in Inner, Left Semi
   * and Left Anti joins.
   */
  private def getJoinCondition(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      anti: Boolean = false): (String, String, Seq[ExprCode]) = {
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      val skipRow = if (!anti) {
        s"${ ev.isNull } || !${ ev.value }"
      } else {
        s"!${ ev.isNull } && ${ ev.value }"
      }
      s"""
         |$eval
         |${ ev.code }
         |if (!$skipRow)
       """.stripMargin
    } else if (anti) {
      "if (false)"
    } else {
      "if (true)"
    }
    (matched, checkCondition, buildVars)
  }

  /**
   * Generates the code for Inner join.
   */
  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    if (broadcastRelation.value.keyIsUnique) {
      setCopyResult(ctx, false)
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${ keyEv.value });
         |if ($matched != null) {
         |$checkCondition {
         |$numOutput.add(1);
         |${ consume(ctx, resultVars) }
         |}
         |}
       """.stripMargin

    } else {
      setCopyResult(ctx, true)
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${
        keyEv
          .value
      });
         |if ($matches != null) {
         |while ($matches.hasNext()) {
         |  UnsafeRow $matched = (UnsafeRow) $matches.next();
         |  $checkCondition {
         |  $numOutput.add(1);
         |  ${ consume(ctx, resultVars) }
         | }
         |}
         |}
       """.stripMargin
    }
  }
  /**
   * Generates the code for left or right outer join.
   */
  private def codegenOuter(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // filter the output via condition
    val conditionPassed = ctx.freshName("conditionPassed")
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |boolean $conditionPassed = true;
         |${ eval.trim }
         |${ ev.code }
         |if ($matched != null) {
         |  $conditionPassed = !${ ev.isNull } && ${ ev.value };
         |}
       """.stripMargin
    } else {
      s"final boolean $conditionPassed = true;"
    }

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    if (broadcastRelation.value.keyIsUnique) {
      setCopyResult(ctx, false)
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${ keyEv.value });
         |${ checkCondition.trim }
         |if (!$conditionPassed) {
         |  $matched = null;
         |  // reset the variables those are already evaluated.
         |  ${ buildVars.filter(_.code == "").map(v => s"${ v.isNull } = true;").mkString("\n") }
         |}
         |$numOutput.add(1);
         |${ consume(ctx, resultVars) }
       """.stripMargin

    } else {
      setCopyResult(ctx, true)
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${
        keyEv
          .value
      });
         |boolean $found = false;
         |// the last iteration of this loop is to emit an empty row if there is no matched rows.
         |while ($matches != null && $matches.hasNext() || !$found) {
         |  UnsafeRow $matched = $matches != null && $matches.hasNext() ?
         |    (UnsafeRow) $matches.next() : null;
         |  ${ checkCondition.trim }
         |  if (!$conditionPassed) continue;
         |  $found = true;
         |  $numOutput.add(1);
         |  ${ consume(ctx, resultVars) }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for left semi join.
   */
  private def codegenSemi(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    if (broadcastRelation.value.keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${ keyEv.value });
         |if ($matched != null) {
         |$checkCondition {
         |$numOutput.add(1);
         |${ consume(ctx, input) }
         |}
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      s"""
         |boolean $found = false;
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${
        keyEv
          .value
      });
         |if ($matches != null) {
         |while (!$found && $matches.hasNext()) {
         |  UnsafeRow $matched = (UnsafeRow) $matches.next();
         |  $checkCondition {
         |  $found = true;
         |  }
         |}
         |if ($found) {
         |$numOutput.add(1);
         |${ consume(ctx, input) }
         |}
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for anti join.
   */
  private def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    val uniqueKeyCodePath = broadcastRelation.value.keyIsUnique
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val (matched, checkCondition, _) = getJoinCondition(ctx, input, uniqueKeyCodePath)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (uniqueKeyCodePath) {
      val found = ctx.freshName("found")
      s"""
         | boolean $found = false;
         |// generate join key for stream side
         |${ keyEv.code }
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  UnsafeRow $matched = (UnsafeRow)$relationTerm.getValue(${ keyEv.value });
         |  if ($matched != null) {
         |    // Evaluate the condition.
         |    $checkCondition {
         |      $found = true;
         |    }
         |  }
         |}
         |if (!$found) {
         |$numOutput.add(1);
         |${ consume(ctx, input) }
         |}
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      val found = ctx.freshName("found")
      s"""
         | boolean $found = false;
         |// generate join key for stream side
         |${ keyEv.code }
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  $iteratorCls $matches = ($iteratorCls)$relationTerm.get(${ keyEv.value });
         |  if ($matches != null) {
         |    // Evaluate the condition.
         |    while (!$found && $matches.hasNext()) {
         |      UnsafeRow $matched = (UnsafeRow) $matches.next();
         |      $checkCondition {
         |        $found = true;
         |      }
         |    }
         |  }
         |}
         |if (!$found) {
         |$numOutput.add(1);
         |${ consume(ctx, input) }
         |}
       """.stripMargin
    }
  }

  /**
   * Generates the code for existence join.
   */
  private def codegenExistence(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val numOutput = metricTerm(ctx, "numOutputRows")
    val existsVar = ctx.freshName("exists")

    val matched = ctx.freshName("matched")
    val buildVars = genBuildSideVars(ctx, matched)
    val checkCondition = if (condition.isDefined) {
      val expr = condition.get
      // evaluate the variables from build side that used by condition
      val eval = evaluateRequiredVariables(buildPlan.output, buildVars, expr.references)
      // filter the output via condition
      ctx.currentVars = input ++ buildVars
      val ev =
        BindReferences.bindReference(expr, streamedPlan.output ++ buildPlan.output).genCode(ctx)
      s"""
         |$eval
         |${ ev.code }
         |$existsVar = !${ ev.isNull } && ${ ev.value };
       """.stripMargin
    } else {
      s"$existsVar = true;"
    }

    val resultVar = input ++ Seq(ExprCode("", "false", existsVar))
    if (broadcastRelation.value.keyIsUnique) {
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${ keyEv.value });
         |boolean $existsVar = false;
         |if ($matched != null) {
         |  $checkCondition
         |}
         |$numOutput.add(1);
         |${ consume(ctx, resultVar) }
       """.stripMargin
    } else {
      val matches = ctx.freshName("matches")
      val iteratorCls = classOf[Iterator[UnsafeRow]].getName
      s"""
         |// generate join key for stream side
         |${ keyEv.code }
         |// find matches from HashRelation
         |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${
        keyEv
          .value
      });
         |boolean $existsVar = false;
         |if ($matches != null) {
         |  while (!$existsVar && $matches.hasNext()) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.next();
         |    $checkCondition
         |  }
         |}
         |$numOutput.add(1);
         |${ consume(ctx, resultVar) }
       """.stripMargin
    }
  }
}

case class BroadCastSIFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryExecNode with HashJoin {

  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    val secondaryIndexRDD = buildPlan.collect {
      case batchData: CarbonDataSourceScan =>
        batchData.rdd
      case rowData: RowDataSourceScanExec =>
        rowData.rdd
    }
    if (partitions.nonEmpty && secondaryIndexRDD.nonEmpty) {
      secondaryIndexRDD.foreach { siRDD =>
        if (siRDD.isInstanceOf[CarbonScanRDD[InternalRow]]) {
          siRDD.asInstanceOf[CarbonScanRDD[InternalRow]].setSegmentsToAccess(partitions)
        }
      }
    }
    // If the partitions that are recognized from the main table are empty then no need to
    // execute the SI plan.
    if (partitions.nonEmpty) {
      val input: Array[InternalRow] = buildPlan.execute.map(_.copy()).collect()
      val inputCopy: Array[InternalRow] = input.clone()
      (input, inputCopy)
    } else {
      (Array.empty[InternalRow], Array.empty[InternalRow])
    }
  }

  val carbonScan: SparkPlan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }

  val mainTableRDD: Option[RDD[InternalRow]] = carbonScan.collectFirst {
    case batchData: CarbonDataSourceScan =>
      batchData.rdd
    case rowData: RowDataSourceScanExec =>
      rowData.rdd
  }

  lazy val partitions: Array[Segment] = if (mainTableRDD.isDefined && mainTableRDD.get
    .isInstanceOf[CarbonScanRDD[InternalRow]]) {
    getFilteredSegments(mainTableRDD.get.asInstanceOf[CarbonScanRDD[InternalRow]])
  } else {
    Array.empty[Segment]
  }

  /**
   * This method is used to get the valid segments for the query based on the filter condition.
   *
   * @return Array of valid segments
   */
  def getFilteredSegments(carbonScanRdd: CarbonScanRDD[InternalRow]): Array[Segment] = {
    val LOGGER = LogServiceFactory.getLogService(BroadCastSIFilterPushJoin.getClass.getName)
    val conf = new Configuration()
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job = Job.getInstance(jobConf)
    val format = carbonScanRdd.prepareInputFormatForDriver(job.getConfiguration)
    val startTime = System.currentTimeMillis()
    val segmentsToAccess: Array[Segment] = CarbonTableInputFormatExtended
      .getFilteredSegments(job, format).asScala.toArray
    LOGGER
      .info("Time taken for getting the splits: " + (System.currentTimeMillis - startTime) +
            " ,Total split: " + segmentsToAccess.length)
    segmentsToAccess

  }

  override def output: Seq[Attribute] = carbonScan.output

  override def doExecute(): RDD[InternalRow] = {
    val LOGGER = LogServiceFactory.getLogService(BroadCastFilterPushJoin.getClass.getName)
    val numOutputRows = longMetric("numOutputRows")
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }

    BroadCastFilterPushJoin.addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide,
      isIndexTable = true)
    carbonScan.execute
  }
}

object BroadCastFilterPushJoin {

  def addInFilterToPlan(buildPlan: SparkPlan,
      carbonScan: SparkPlan,
      inputCopy: Array[InternalRow],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      buildSide: BuildSide,
      isIndexTable: Boolean = false): Unit = {
    val LOGGER = if (isIndexTable) {
      LogServiceFactory.getLogService(BroadCastSIFilterPushJoin.getClass.getName)
    } else {
      LogServiceFactory.getLogService(BroadCastFilterPushJoin.getClass.getName)
    }

    val keys = {
      buildSide match {
        case BuildLeft => (leftKeys)
        case BuildRight => (rightKeys)
      }
    }.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray

    val filters = keys.map {
      k =>
        inputCopy.map(
          r => {
            val curr = k.eval(r)
            curr match {
              case _: UTF8String => Literal(curr.toString).asInstanceOf[Expression]
              case _: Long if k.dataType.isInstanceOf[TimestampType] =>
                Literal(curr, TimestampType).asInstanceOf[Expression]
              case _ => Literal(curr).asInstanceOf[Expression]
            }
          })
    }

    val filterKey = (buildSide match {
      case BuildLeft => rightKeys
      case BuildRight => leftKeys
    }).collectFirst { case a: Attribute => a }

    def resolveAlias(expressions: Seq[Expression]) = {
      val aliasMap = new mutable.HashMap[Attribute, Expression]()
        carbonScan.transformExpressions {
        case alias: Alias =>
          aliasMap.put(alias.toAttribute, alias.child)
          alias
      }
      expressions.map {
        case at: AttributeReference =>
          // cannot use Map.get() as qualifier is different.
          aliasMap.find(_._1.semanticEquals(at)) match {
            case Some(child) => child._2
            case _ => at
          }
        case others => others
      }
    }

    val filterKeys = buildSide match {
      case BuildLeft =>
        resolveAlias(rightKeys)
      case BuildRight =>
        resolveAlias(leftKeys)
    }

    val tableScan = carbonScan.collectFirst {
      case ProjectExec(projectList, batchData: CarbonDataSourceScan)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case ProjectExec(projectList, rowData: RowDataSourceScanExec)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
      case batchData: CarbonDataSourceScan
        if (filterKey.isDefined && (isIndexTable || batchData.output.attrs.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case rowData: RowDataSourceScanExec
        if (filterKey.isDefined && (isIndexTable || rowData.output.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
    }
    val configuredFilterRecordSize = CarbonProperties.getInstance.getProperty(
      CarbonInternalCommonConstants.BROADCAST_RECORD_SIZE,
      CarbonInternalCommonConstants.DEFAULT_BROADCAST_RECORD_SIZE)

    if (tableScan.isDefined && null != filters
        && filters.size > 0
        && ((filters(0).size > 0 && filters(0).size <= configuredFilterRecordSize.toInt) ||
            isIndexTable)) {
      LOGGER.info("Pushing down filter for broadcast join. Filter size:" + filters(0).size)
      if (tableScan.get.isInstanceOf[CarbonDataSourceScan]) {
        addPushdownToCarbonRDD(tableScan.get.asInstanceOf[CarbonDataSourceScan].rdd,
          addPushdownFilters(filterKeys, filters))
      } else {
        addPushdownToCarbonRDD(tableScan.get.asInstanceOf[RowDataSourceScanExec].rdd,
          addPushdownFilters(filterKeys, filters))
      }
    }
  }

  private def addPushdownToCarbonRDD(rdd: RDD[InternalRow],
      expressions: Seq[Expression]): Unit = {
    if (rdd.isInstanceOf[CarbonDecoderRDD]) {
      rdd.asInstanceOf[CarbonDecoderRDD].setFilterExpression(expressions)
    } else if (rdd.isInstanceOf[CarbonScanRDD[InternalRow]]) {
      if (expressions.nonEmpty) {
        val expressionVal = CarbonFilters
          .transformExpression(CarbonFilters.preProcessExpressions(expressions).head)
        if (null != expressionVal) {
          rdd.asInstanceOf[CarbonScanRDD[InternalRow]].setFilterExpression(expressionVal)
        }
      }
    }
  }

  private def addPushdownFilters(keys: Seq[Expression],
      filters: Array[Array[Expression]]): Seq[Expression] = {

    // TODO Values in the IN filter is duplicate. replace the list with set
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    // Let's not pushdown condition. Only filter push down is sufficient.
    // Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And(_, _))
    } else {
      buffer.asJava.get(0)
    }
    Seq(cond)
  }
}

/**
 * unapply method of BroadcastExchangeExec
 */
object CarbonBroadcastExchangeExec {
  def unapply(plan: SparkPlan): Option[(BroadcastMode, SparkPlan)] = {
    plan match {
      case cExec: BroadcastExchangeExec =>
        Some(cExec.mode, cExec.child)
      case _ => None
    }
  }
}