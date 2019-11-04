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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonExtensionRulesForOnce, CarbonIUDAnalysisRule, CarbonPreInsertionCasts, CarbonPreOptimizerRule}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonExtensionSqlParser

class CarbonExtensions extends ((SparkSessionExtensions) => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Carbon internal parser
    extensions
      .injectParser((sparkSession: SparkSession, parser: ParserInterface) =>
        new CarbonExtensionSqlParser(new SQLConf, sparkSession, parser))

    // carbon analyzer rules
    // TODO: CarbonAccessControlRules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))

    extensions
      .injectPostHocResolutionRule((session: SparkSession) => CarbonExtensionRulesForOnce(session))
    // Carbon Pre optimization rules
    extensions
      .injectPostHocResolutionRule((_: SparkSession) => new CarbonPreOptimizerRule)
    // Carbon optimization rules
    extensions
      .injectPostHocResolutionRule((sparkSession: SparkSession) => {
        CarbonOptimizationRulesWrapper(sparkSession)
      })

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

    // init CarbonEnv
    CarbonEnv.init
  }
}

case class CarbonOptimizationRulesWrapper(session: SparkSession)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (session.sessionState.experimentalMethods.extraOptimizations.isEmpty) {
      session.sessionState.experimentalMethods.extraOptimizations = Seq(
        new CarbonIUDRule,
        new CarbonUDFTransformRule,
        new CarbonLateDecodeRule)
    }
    plan
  }
}


