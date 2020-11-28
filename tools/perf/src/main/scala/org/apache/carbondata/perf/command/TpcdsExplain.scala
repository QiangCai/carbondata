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

package org.apache.carbondata.perf.command

import org.apache.spark.sql.SqlHelper.runQueriesWithOption

import org.apache.carbondata.perf.Constant.sqlDir
import org.apache.carbondata.perf.PerfHelper.{loadSqlDir, showDetail}

case class TpcdsExplain(database: String) extends Command {
  override def run(): Unit = {
    val sqlFiles = loadSqlDir(s"$sqlDir/query")
    val prefix = "explain extended "
    val explainSqlFiles = sqlFiles.map { case (file, sqlList) =>
      (file, sqlList.map(prefix + _))
    }
    val result = runQueriesWithOption(explainSqlFiles, dbOption = Some(database))
    showDetail(database, result)
  }
}
