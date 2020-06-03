/*
 * licensed to the apache software foundation (asf) under one or more
 * contributor license agreements.  see the notice file distributed with
 * this work for additional information regarding copyright ownership.
 * the asf licenses this file to you under the apache license, version 2.0
 * (the "license"); you may not use this file except in compliance with
 * the license.  you may obtain a copy of the license at
 *
 *    http://www.apache.org/licenses/license-2.0
 *
 * unless required by applicable law or agreed to in writing, software
 * distributed under the license is distributed on an "as is" basis,
 * without warranties or conditions of any kind, either express or implied.
 * see the license for the specific language governing permissions and
 * limitations under the license.
 */

package org.apache.spark.sql.execution.joins

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSeq, BitwiseAnd, BoundReference, Cast, Expression, ExprId, GenericInternalRow, InSet, Literal, NamedExpression, OuterReference, PlanExpression, Predicate, ShiftRightUnsigned, SortOrder, SubqueryExpression, UnaryExpression, Unevaluable, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OrderPreservingUnaryNode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ExecSubqueryExpression, SparkPlan, SQLExecution, SubqueryExec, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{BooleanType, DataType, LongType, StructType}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.ThreadUtils

case class CarbonRuntimePlan(child: LogicalPlan) extends OrderPreservingUnaryNode {
  override def output: Seq[Attribute] = child.output
}

case class RuntimeFilter(
    filterKey: Expression,
    buildQuery: LogicalPlan,
    buildKeys: Seq[Expression],
    broadcastKeyIndex: Int,
    exprId: ExprId = NamedExpression.newExprId,
    var isNeed: Boolean = false)
  extends SubqueryExpression(buildQuery, Seq(filterKey), exprId) with Predicate with Unevaluable {
  override def children: Seq[Expression] = Seq(filterKey)

  override def plan: LogicalPlan = buildQuery

  override def nullable: Boolean = false

  override def withNewPlan(plan: LogicalPlan): RuntimeFilter = copy(buildQuery = plan)

  override lazy val resolved: Boolean = {
    filterKey.resolved &&
    buildQuery.resolved &&
    buildKeys.nonEmpty &&
    buildKeys.forall(_.resolved) &&
    broadcastKeyIndex >= 0 &&
    broadcastKeyIndex < buildKeys.size &&
    buildKeys.forall(_.references.subsetOf(buildQuery.outputSet)) &&
    filterKey.dataType == buildKeys(broadcastKeyIndex).dataType
  }

  override def toString: String = s"RuntimeFilter#${ exprId.id } $conditionString"

  override lazy val canonicalized: RuntimeFilter = {
    copy(
      filterKey = filterKey.canonicalized,
      buildQuery = buildQuery.canonicalized,
      buildKeys = buildKeys.map(_.canonicalized),
      exprId = ExprId(0))
  }
}

case class RuntimeFilterExpression(child: Expression) extends UnaryExpression with Predicate {
  override def eval(input: InternalRow): Any = child.eval(input)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    child.genCode(ctx)
  }
}

case class SubqueryBroadcastExec(
    name: String,
    index: Int,
    buildKeys: Seq[Expression],
    child: SparkPlan) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override lazy val metrics = Map(
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => SubqueryBroadcastExec.normalizeExpressions(k, output))
    SubqueryBroadcastExec("dpp", index, keys, child.canonicalized)
  }

  @transient
  private lazy val relationFuture: Future[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sqlContext.sparkSession, executionId) {
        val beforeCollect = System.nanoTime()

        val broadcastRelation = child.executeBroadcast[HashedRelation]().value
        val (iter, expr) = if (broadcastRelation.isInstanceOf[LongHashedRelation]) {
          (SubqueryBroadcastExec.keys(broadcastRelation),
            SubqueryBroadcastExec.extractKeyExprAt(buildKeys, index))
        } else {
          (SubqueryBroadcastExec.keys(broadcastRelation),
            BoundReference(index, buildKeys(index).dataType, buildKeys(index).nullable))
        }

        val proj = UnsafeProjection.create(expr)
        val keyIter = iter.map(proj).map(_.copy())

        val rows = keyIter.toArray[InternalRow].distinct
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes).sum
        longMetric("dataSize") += dataSize
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        rows
      }
    }(SubqueryBroadcastExec.executionContext)
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "SubqueryBroadcastExec does not support the execute() code path.")
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }
}

object SubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("RuntimeFilter", 16))

  def normalizeExpressions[T <: Expression](e: T, input: AttributeSeq): T = {
    e.transformUp {
      case s: PlanExpression[QueryPlan[_] @unchecked] =>
        // Normalize the outer references in the subquery plan.
        val normalizedPlan = s.plan.transformAllExpressions {
          case OuterReference(r) => OuterReference(normalizeExpressions(r, input))
        }
        s.withNewPlan(normalizedPlan)

      case ar: AttributeReference =>
        val ordinal = input.indexOf(ar.exprId)
        if (ordinal == -1) {
          ar
        } else {
          ar.withExprId(ExprId(ordinal))
        }
    }.canonicalized.asInstanceOf[T]
  }

  def keys(hashedRelation: HashedRelation): Iterator[InternalRow] = {
    hashedRelation match {
      case unsafeHashedRelation: UnsafeHashedRelation =>
        val binaryMapField = unsafeHashedRelation.getClass.getDeclaredField("binaryMap")
        binaryMapField.setAccessible(true)
        val binaryMap = binaryMapField.get(unsafeHashedRelation).asInstanceOf[BytesToBytesMap]
        val numFieldsField = unsafeHashedRelation.getClass.getDeclaredField("numFields")
        numFieldsField.setAccessible(true)
        val numFields = numFieldsField.get(unsafeHashedRelation).asInstanceOf[Int]
        val iter = binaryMap.iterator()
        new Iterator[InternalRow] {
          val unsafeRow = new UnsafeRow(numFields)

          override def hasNext: Boolean = {
            iter.hasNext
          }

          override def next(): InternalRow = {
            if (!hasNext) {
              throw new NoSuchElementException("End of the iterator")
            } else {
              val loc = iter.next()
              unsafeRow.pointTo(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
              unsafeRow
            }
          }
        }
      case longHashedRelation: LongHashedRelation =>
        val mapField = longHashedRelation.getClass.getDeclaredField("map")
        mapField.setAccessible(true)
        val rowMap = mapField.get(longHashedRelation).asInstanceOf[LongToUnsafeRowMap]
        val isDenseField = rowMap.getClass.getDeclaredField("isDense")
        isDenseField.setAccessible(true)
        val isDense = isDenseField.get(rowMap).asInstanceOf[Boolean]
        val arrayField = rowMap.getClass.getDeclaredField("array")
        arrayField.setAccessible(true)
        val array = arrayField.get(rowMap).asInstanceOf[Array[Long]]
        val minKeyFiled = rowMap.getClass.getDeclaredField("minKey")
        minKeyFiled.setAccessible(true)
        val minKey = minKeyFiled.get(rowMap).asInstanceOf[Long]
        val row = new GenericInternalRow(1)
        // a) in dense mode the array stores the address
        //  => (k, v) = (minKey + index, array(index))
        // b) in sparse mode the array stores both the key and the address
        //  => (k, v) = (array(index), array(index+1))
        new Iterator[InternalRow] {
          // cursor that indicates the position of the next key which was not read by a next() call
          var pos = 0
          // when we iterate in dense mode we need to jump two positions at a time
          val step = if (isDense) {
            0
          } else {
            1
          }

          override def hasNext: Boolean = {
            // go to the next key if the current key slot is empty
            while (pos + step < array.length) {
              if (array(pos + step) > 0) {
                return true
              }
              pos += step + 1
            }
            false
          }

          override def next(): InternalRow = {
            if (!hasNext) {
              throw new NoSuchElementException("End of the iterator")
            } else {
              // the key is retrieved based on the map mode
              val ret = if (isDense) {
                minKey + pos
              } else {
                array(pos)
              } // advance the cursor to the next index
              pos += step + 1
              row.setLong(0, ret)
              row
            }
          }
        }
    }
  }

  def extractKeyExprAt(keys: Seq[Expression], index: Int): Expression = {
    // jump over keys that have a higher index value than the required key
    if (keys.size == 1) {
      assert(index == 0)
      Cast(BoundReference(0, LongType, nullable = false), keys(index).dataType)
    } else {
      val shiftedBits =
        keys.slice(index + 1, keys.size).map(_.dataType.defaultSize * 8).sum
      val mask = (1L << (keys(index).dataType.defaultSize * 8)) - 1
      // build the schema for unpacking the required key
      Cast(BitwiseAnd(
        ShiftRightUnsigned(BoundReference(0, LongType, nullable = false), Literal(shiftedBits)),
        Literal(mask)), keys(index).dataType)
    }
  }
}

case class InSubqueryExec(
    child: Expression,
    plan: SubqueryExec,
    newPlan: SubqueryBroadcastExec,
    exprId: ExprId,
    private var resultBroadcast: Broadcast[Array[Any]] = null
) extends ExecSubqueryExpression {

  @transient private var result: Array[Any] = _

  override def dataType: DataType = BooleanType
  override def children: Seq[Expression] = child :: Nil
  override def nullable: Boolean = child.nullable
  override def toString: String = s"$child IN ${newPlan.name}"
  override def withNewPlan(plan: SubqueryExec): InSubqueryExec = copy(plan = plan)
  override def semanticEquals(other: Expression): Boolean = other match {
    case in: InSubqueryExec => child.semanticEquals(in.child) && newPlan.sameResult(in.newPlan)
    case _ => false
  }

  def updateResult(): Unit = {
    val rows = newPlan.executeCollect()
    result = child.dataType match {
      case _: StructType => rows.toArray
      case _ => rows.map(_.get(0, child.dataType))
    }
    resultBroadcast = newPlan.sqlContext.sparkContext.broadcast(result)
  }

  def values(): Option[Array[Any]] = Option(resultBroadcast).map(_.value)

  private def prepareResult(): Unit = {
    require(resultBroadcast != null, s"$this has not finished")
    if (result == null) {
      result = resultBroadcast.value
    }
  }

  override def eval(input: InternalRow): Any = {
    prepareResult()
    val v = child.eval(input)
    if (v == null) {
      null
    } else {
      result.contains(v)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    prepareResult()
    InSet(child, result.toSet).doGenCode(ctx, ev)
  }

  override lazy val canonicalized: InSubqueryExec = {
    copy(
      child = child.canonicalized,
      newPlan = newPlan.canonicalized.asInstanceOf[SubqueryBroadcastExec],
      exprId = ExprId(0),
      resultBroadcast = null)
  }
}




