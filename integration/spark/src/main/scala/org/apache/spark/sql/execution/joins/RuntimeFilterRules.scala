/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeMap, BinaryComparison, BindReferences, EqualTo, Expression, In, InSet, Like, ListQuery, Literal, Not, Or, PredicateHelper, StringPredicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Command, Filter, Join, LeafNode, LocalRelation, LogicalPlan, Project, ReturnAnswer, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SubqueryExec}
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.SparkSQLUtil

/**
 * RuntimeFilterRule
 */
object InsertRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper {

  def findExpressionAndTrackLineageDown(
      exp: Expression,
      plan: LogicalPlan): Option[(Expression, LogicalPlan)] = {
    plan match {
      case Project(projectList, child) =>
        val aliases = AttributeMap(projectList.collect {
          case a@Alias(child, _) => (a.toAttribute, child)
        })
        findExpressionAndTrackLineageDown(replaceAlias(exp, aliases), child)
      // we can unwrap only if there are row projections, and no aggregation operation
      case Aggregate(_, aggregateExpressions, child) =>
        val aliasMap = AttributeMap(aggregateExpressions.collect {
          case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
            (a.toAttribute, a.child)
        })
        findExpressionAndTrackLineageDown(replaceAlias(exp, aliasMap), child)
      case l: LeafNode if exp.references.subsetOf(l.outputSet) =>
        Some((exp, l))
      case other =>
        other.children.flatMap {
          child =>
            if (exp.references.subsetOf(child.outputSet)) {
              findExpressionAndTrackLineageDown(exp, child)
            } else {
              None
            }
        }.headOption
    }
  }

  def getCarbonTableScan(a: Expression, plan: LogicalPlan): Option[LogicalRelation] = {
    val srcInfo: Option[(Expression, LogicalPlan)] = findExpressionAndTrackLineageDown(a, plan)
    srcInfo.flatMap {
      case (resExp, l@LogicalRelation(r: CarbonDatasourceHadoopRelation, _, _, _)) =>
        val filterColumns = r.carbonTable.getRuntimeFilterColumns.asScala.map((_, true)).toMap
        if (filterColumns.nonEmpty &&
            resExp.references.map(_.name).exists(filterColumns.get(_).isDefined)) {
          Some(l)
        } else {
          None
        }
      case _ => None
    }
  }

  private def canFilterLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  private def canFilterRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter => true
    case _ => false
  }

  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case Like(_, _) => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _ => false
  }

  private def hasFilter(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  private def hasBenefit(
      mainExpr: Expression,
      mainPlan: LogicalPlan,
      otherExpr: Expression,
      otherPlan: LogicalPlan): Boolean = {
    // get the distinct counts of an attribute for a given table
    def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
      plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
    }
    // the default filtering ratio when CBO stats are missing, but there is a
    // predicate that is likely to be selective
    val fallbackRatio = 0.5
    // the filtering ratio based on the type of the join condition and on the column statistics
    val filterRatio = (mainExpr.references.toList, otherExpr.references.toList) match {
      // filter out expressions with more than one attribute on any side of the operator
      case (leftAttr :: Nil, rightAttr :: Nil)  =>
        // get the CBO stats for each attribute in the join condition
        val partDistinctCount = distinctCounts(leftAttr, mainPlan)
        val otherDistinctCount = distinctCounts(rightAttr, otherPlan)
        val availableStats = partDistinctCount.isDefined && partDistinctCount.get > 0 &&
                             otherDistinctCount.isDefined
        if (!availableStats) {
          fallbackRatio
        } else if (partDistinctCount.get.toDouble <= otherDistinctCount.get.toDouble) {
          // there is likely an estimation error, so we fallback
          fallbackRatio
        } else {
          1 - otherDistinctCount.get.toDouble / partDistinctCount.get.toDouble
        }
      case _ => fallbackRatio
    }
    // the pruning overhead is the total size in bytes of all scan relations
    val overhead = otherPlan.collectLeaves().map(_.stats.sizeInBytes).sum.toFloat
    filterRatio * mainPlan.stats.sizeInBytes.toFloat > overhead.toFloat
  }

  private def insertFilter(
      pruningKey: Expression,
      pruningPlan: LogicalPlan,
      filteringKey: Expression,
      filteringPlan: LogicalPlan,
      joinKeys: Seq[Expression]): LogicalPlan = {
    Filter(
      RuntimeFilter(
        pruningKey,
        filteringPlan,
        joinKeys,
        joinKeys.indexOf(filteringKey)),
      pruningPlan)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: Subquery => plan
      case _: LocalRelation => plan
      case _: Command => plan
      case _ => plan transformUp {
        case j@Join(Filter(_: RuntimeFilter, _), _, _, _) => j
        case j@Join(_, Filter(_: RuntimeFilter, _), _, _) => j
        case j@Join(left, right, joinType, Some(condition)) =>

          var newLeft = left
          var newRight = right

          // extract the left and right keys of the join condition
          val (leftKeys, rightKeys) = j match {
            case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _) => (lkeys, rkeys)
            case _ => (Nil, Nil)
          }

          // checks if two expressions are on opposite sides of the join
          def fromDifferentSides(x: Expression, y: Expression): Boolean = {
            def fromLeftRight(x: Expression, y: Expression) =
              !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
              !y.references.isEmpty && y.references.subsetOf(right.outputSet)

            fromLeftRight(x, y) || fromLeftRight(y, x)
          }

          splitConjunctivePredicates(condition).foreach {
            case EqualTo(a: Expression, b: Expression)
              if fromDifferentSides(a, b) =>
              val (l, r) = if (a.references.subsetOf(left.outputSet) &&
                               b.references.subsetOf(right.outputSet)) {
                a -> b
              } else {
                b -> a
              }
              var tableScan = getCarbonTableScan(l, left)
              if (tableScan.isDefined && canFilterLeft(joinType) && hasFilter(right)) {
                if (hasBenefit(l, tableScan.get, r, right)) {
                  newLeft = insertFilter(l, newLeft, r, right, rightKeys)
                }
              } else {
                tableScan = getCarbonTableScan(r, right)
                if (tableScan.isDefined && canFilterRight(joinType) && hasFilter(left)) {
                  if (hasBenefit(r, tableScan.get, l, left)) {
                    newRight = insertFilter(r, newRight, l, left, leftKeys)
                  }
                }
              }
            case _ =>
          }
          Join(newLeft, newRight, joinType, Some(condition))
      }
    }
  }
}

/**
 * RuntimeFilterCleanupRule
 */
object RemoveRuntimeFilter extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: Subquery => plan
      case _: LocalRelation => plan
      case _: Command => plan
      case _ =>
        var hasRuntimeFilter = false
        val result = plan.transform {
          case p@PhysicalOperation(_, filters,
          LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _, _)) =>
            filters.foreach {
              case f: RuntimeFilter =>
                if (!f.isNeed) {
                  f.isNeed = true
                  hasRuntimeFilter = true
                }
              case _ =>
            }
            p
          case f@Filter(condition, _) =>
            val newCondition = condition.transform {
              case _: RuntimeFilter => Literal.TrueLiteral
            }
            f.copy(condition = newCondition)
        }
        if (hasRuntimeFilter) {
          CarbonRuntimePlan(result)
        } else {
          result
        }
    }
  }
}

class QueryExecutionWrap(sparkSession: SparkSession) extends QueryExecution(sparkSession, null) {
  def createSparkPlan(plan: LogicalPlan): SparkPlan = {
    planner.plan(ReturnAnswer(plan)).next()
  }

  def createExecutedPlan(plan: SparkPlan): SparkPlan = {
    super.prepareForExecution(plan)
  }

  def broadcastMode(keys: Seq[Expression], plan: LogicalPlan): BroadcastMode = {
    val packedKeys =
      HashJoin.rewriteKeyExpr(keys).map(BindReferences.bindReference(_, plan.output))
    HashedRelationBroadcastMode(packedKeys)
  }
}

object PlanRuntimeFilter {
  def apply(logicalPlan: LogicalPlan): SparkPlan = {
    val sparkSession = SparkSQLUtil.getSparkSession
    val queryExecution = new QueryExecutionWrap(sparkSession)
    val plan = queryExecution.createSparkPlan(logicalPlan)
    plan transformAllExpressions {
      case RuntimeFilter(value, buildPlan, buildKeys, broadcastKeyIndex, exprId, _) =>
        val sparkPlan = queryExecution.createSparkPlan(buildPlan)
        val canReuseExchange = if (SQLConf.get.exchangeReuseEnabled && buildKeys.nonEmpty) {
          plan.find {
            case BroadcastHashJoinExec(_, _, _, BuildLeft, _, left, _) =>
              left.sameResult(sparkPlan)
            case BroadcastHashJoinExec(_, _, _, BuildRight, _, _, right) =>
              right.sameResult(sparkPlan)
            case _ => false
          }.isDefined
        } else {
          false
        }
        if (canReuseExchange) {
          val mode = queryExecution.broadcastMode(buildKeys, buildPlan)
          val executedPlan = queryExecution.createExecutedPlan(sparkPlan)
          val exchange = BroadcastExchangeExec(mode, executedPlan)
          val name = s"RuntimeFilter#${exprId.id}"
          val broadcastValues =
            SubqueryBroadcastExec(name, broadcastKeyIndex, buildKeys, exchange)
          RuntimeFilterExpression(
            InSubqueryExec(value, SubqueryExec(name, broadcastValues), broadcastValues, exprId))
        } else {
          val alias = Alias(buildKeys(broadcastKeyIndex), buildKeys(broadcastKeyIndex).toString)()
          val aggregate = Aggregate(Seq(alias), Seq(alias), buildPlan)
          RuntimeFilterExpression(org.apache.spark.sql.catalyst.expressions.InSubquery(
            Seq(value), ListQuery(aggregate, childOutputs = aggregate.output)))
        }
    }
  }
}

