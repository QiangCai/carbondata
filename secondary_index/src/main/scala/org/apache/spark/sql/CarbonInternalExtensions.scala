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
import org.apache.spark.sql.command.InternalDDLStrategy
import org.apache.spark.sql.execution.strategy.{CarbonInternalLateDecodeStrategy, CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonCommonInitializer, CarbonMVRules, CarbonIUDAnalysisRule, CarbonPreInsertionCasts, CarbonPreOptimizerRule}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonSITransformationRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonExtensionInternalSqlParser

class CarbonInternalExtensions extends ((SparkSessionExtensions) => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Carbon internal parser
    extensions
      .injectParser((sparkSession: SparkSession, parser: ParserInterface) =>
        new CarbonExtensionInternalSqlParser(new SQLConf, sparkSession, parser))

    // carbon analyzer rules
    // TODO: CarbonAccessControlRules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))
    extensions
      .injectFirstBatchOptimizerRule((session: SparkSession) => CarbonMVRules(session))

    // Carbon Pre optimization rules
    extensions
      .injectFirstBatchOptimizerRule((_: SparkSession) => new CarbonPreOptimizerRule)

    // Carbon optimization rules
    extensions
      .injectLastBatchOptimizerRule((_: SparkSession) => new CarbonIUDRule)
    extensions
      .injectLastBatchOptimizerRule((_: SparkSession) => new CarbonUDFTransformRule)
    extensions.injectLastBatchOptimizerRule(
      (session: SparkSession) => new CarbonSITransformationRule(session))
    extensions
      .injectLastBatchOptimizerRule((_: SparkSession) => new CarbonLateDecodeRule)

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonInternalLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new InternalDDLStrategy(session))

    // pre execution
    // TODO: CarbonPrivCheck

    // init Carbon
    CarbonCommonInitializer.init(false)
  }
}
