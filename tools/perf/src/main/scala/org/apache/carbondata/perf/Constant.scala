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

package org.apache.carbondata.perf

import java.io.File

import org.apache.carbondata.perf.PerfCli.{dataDir, tpcds_scale}

object Constant {
  val sqlDir = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    s"$rootPath/tools/perf/src/main/resources/tpcds/spark"
  }

  // data and metadata location
  val csvFolder = s"$dataDir/tpcds/$tpcds_scale"
  val warehouse = s"$dataDir/warehouse"
  val metadata = s"$dataDir/metadata/metastore_db"

  // file formats and databases
  val csvdb = s"csvdb$tpcds_scale"
  val parquetdb = s"parquetdb$tpcds_scale"
  val orcdb = s"orcdb$tpcds_scale"
  val carbondb = s"carbondb$tpcds_scale"
  val allDatabases = Seq(csvdb, parquetdb, orcdb, carbondb)
  val allFormats = Seq(("parquet", parquetdb), ("orc", orcdb), ("carbon", carbondb))
}
