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

package org.apache.spark.sql.execution.strategy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CarbonParserUtil
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand

object DMLHelper {
  def loadData(
      loadDataCommand: LoadDataCommand,
      sparkSession: SparkSession
  ): CarbonLoadDataCommand = {
    val loadOptions = if (loadDataCommand.options != null && !loadDataCommand.options.isEmpty) {
      val optionsMap = loadDataCommand.options.map { entry =>
        (entry._1.trim.toLowerCase(), entry._2)
      }
      CarbonParserUtil.validateOptions(Option(optionsMap.toList))
      optionsMap
    } else {
      Map.empty[String, String]
    }
    CarbonLoadDataCommand(
      databaseNameOp = loadDataCommand.table.database,
      tableName = loadDataCommand.table.table.toLowerCase,
      factPathFromUser = loadDataCommand.path,
      dimFilesPath = Seq(),
      options = loadOptions,
      isOverwriteTable = loadDataCommand.isOverwrite,
      inputSqlString = null,
      dataFrame = None,
      updateModel = None,
      tableInfoOp = None,
      internalOptions = Map.empty,
      partition =
        loadDataCommand
          .partition
          .getOrElse(Map.empty)
          .map { case (col, value) =>
            (col, Some(value))
          })
  }
}
