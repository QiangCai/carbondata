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

package org.apache.carbondata.perf.tpcds.query

import org.apache.spark.sql.CarbonUtils

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.perf.util.SqlHelper

/**
 * QueryStats
 */
object QueryStats extends SqlHelper {

  val sqlFolder =
    s"$codeWorkSpace/tools/perf/src/main/resources/tpcds/query";

  def main(args: Array[String]): Unit = {
    CarbonUtils.threadSet("disable_sql_rewrite", "true")
//    CarbonProperties.getInstance().addProperty(
//      CarbonCommonConstants.CARBON_RUNTIME_FILTER_ENABLE, "false")
    sql("use carbondb")
    // CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_RUNTIME_FILTER_ENABLE, "false")
    runSqlInFile(s"$sqlFolder/query72.sql")
  }

  def runSqlInFile(filePath: String): Unit = {
    val sqlTexts = loadSql(filePath, Map.empty)
    sqlTexts.foreach { sqlText =>
      println("====================================")
      println(sqlText)
      sql("explain extended " + sqlText).show(1000, false)
    }
  }
}
