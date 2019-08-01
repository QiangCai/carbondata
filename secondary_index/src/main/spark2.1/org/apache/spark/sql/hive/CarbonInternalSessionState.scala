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

package org.apache.spark.sql.hive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.{SparkSessionListener, _}
import org.apache.spark.sql.acl._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.InternalDDLStrategy
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlan}
import org.apache.spark.sql.execution.strategy.CarbonInternalLateDecodeStrategy
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonSITransformationRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.{CarbonInternalSpark2SqlParser, CarbonInternalSparkSqlParser}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateTableContext
import org.apache.spark.sql.internal.SQLConf

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.spark.acl.CarbonUserGroupInformation

/**
 *
 */
class CarbonInternalSessionState(sparkSession: SparkSession)
  extends CarbonSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonInternalSparkSqlParser(conf,
    sparkSession)

  override def extraStrategies: Seq[Strategy] = {
    super.extraStrategies ++
    Seq(new CarbonInternalLateDecodeStrategy, new InternalDDLStrategy(sparkSession))
  }

  override def extraOptimizations: Seq[Rule[LogicalPlan]] = {
    Seq()
  }

  override lazy val otherPrepareRules: Seq[Rule[SparkPlan]] = {
    Seq(CarbonPrivCheck(sparkSession, catalog, aclInterface))
  }

  override def customPreOptimizationRules: Seq[(String, Int, Seq[Rule[LogicalPlan]])] = {
    super.customPreOptimizationRules :+
    ("Carbon PreOptimizer Rule", 1, Seq(new CarbonPreOptimizerRule))
  }

  override def customPostHocOptimizationRules: Seq[(String, Int, Seq[Rule[LogicalPlan]])] = {
    super.customPostHocOptimizationRules :+
    ("Carbon PostOptimizer Rule", conf.optimizerMaxIterations,
      Seq(new CarbonIUDRule,
        new CarbonUDFTransformRule,
        new CarbonSITransformationRule(sparkSession),
        new CarbonLateDecodeRule))
  }

  override lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods) {

    override def extendedAggOptimizationRules: Seq[Rule[LogicalPlan]] = {
      super.extendedAggOptimizationRules ++ customAggOptimizationRules
    }

    override def preOptimizationBatches: Seq[Batch] = {
      super.preOptimizationBatches ++
      customPreOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }

    // TODO shouldnot be there
    override def tailOptimizationBatches: Seq[Batch] = {
      super.tailOptimizationBatches ++
      customTailOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }

    // TODO shouldnot be there
    override def postHocOptimizationBatched: Seq[Batch] = {
      super.postHocOptimizationBatched ++
      customPostHocOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }
  }

  // initialize all listeners
  CarbonInternalSessionState.init(sparkSession)

  class CarbonPreOptimizerRule extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    }
  }

  override def extendedAnalyzerRules: Seq[Rule[LogicalPlan]] = {
    CarbonAccessControlRules(sparkSession, catalog, aclInterface) :: Nil
  }
}

/**
 * Listener on session to handle clean during close session.
 */
class CarbonSessionCloseListener(sparkSession: SparkSession) extends SparkSessionListener {

  override def closeSession(): Unit = {
    CarbonUserGroupInformation.cleanUpUGIFromSession(sparkSession)

    // Remove the listener from session
    sparkSession.sessionStateListenerManager.removeListener(this)
  }
}

// Register all the required listeners using the singleton instance as the listeners
// need to be registered only once
object CarbonInternalSessionState {
  var initialized = false

  def init(sparkSession: SparkSession): Unit = {

    sparkSession.sessionStateListenerManager
      .addListener(new CarbonSessionCloseListener(sparkSession))
    if (!initialized) {
      CarbonCommonInitializer.init(sparkSession)
      initialized = true
    }
  }
}

class CarbonInternalSqlAstBuilder(conf: SQLConf, parser: CarbonInternalSpark2SqlParser,
  sparkSession: SparkSession) extends CarbonSqlAstBuilder(conf, parser, sparkSession) {

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }
}