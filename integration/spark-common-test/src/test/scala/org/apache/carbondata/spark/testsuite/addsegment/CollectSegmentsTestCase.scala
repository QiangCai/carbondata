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
package org.apache.carbondata.spark.testsuite.addsegment


import java.io.IOException

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{CarbonMetadata, CarbonTableIdentifier}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.hadoop.io.IOUtils

class CollectSegmentsTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    dropTable

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "dd-MM-yyyy")

  }

  test("Test collect segments ") {
    sql("drop table if exists collect_segments")
    sql(
      """
        | CREATE TABLE collect_segments (empname String, designation String, doj Timestamp,
        |  workgroupcategory int, workgroupcategoryname String, deptno int, deptname String,
        |  projectcode int, projectjoindate Timestamp, projectenddate Date,attendance int,
        |  utilization int,salary int, empno int)
        | STORED AS carbondata
      """.stripMargin)

    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE collect_segments OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    moveStatus("abc1")
    sql(s"""LOAD DATA local inpath '$resourcesPath/data.csv' INTO TABLE collect_segments OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '"')""")
    moveStatus("abc2")
    sql(s"alter table collect_segments collect segments").show()

  }

  def moveStatus(newName: String ): Unit = {
    val tableIdentifier = new CarbonTableIdentifier(
      CarbonCommonConstants.DATABASE_DEFAULT_NAME, "collect_segments".toLowerCase(), "uniqueid")
    val carbonTable =
      CarbonMetadata.getInstance().getCarbonTable(tableIdentifier.getTableUniqueName)
    val tablestatus = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath)
    val loadFolder = CarbonTablePath.getLoadDetailsDir(carbonTable.getTablePath)
    val file = FileFactory.getCarbonFile(loadFolder)
    if (!file.exists()) {
      FileFactory.mkdirs(loadFolder, FileFactory.getFileType(loadFolder))
    }
    val loadDetails = CarbonTablePath.getLoadDetailsDir(carbonTable.getTablePath) + "/" + newName
    copyFile(tablestatus, loadDetails)
    val loadDetailsSuccess = CarbonTablePath.getLoadDetailsDir(carbonTable.getTablePath) + "/" + newName + ".success"
    FileFactory.createNewFile(loadDetailsSuccess,  FileFactory.getFileType(loadDetailsSuccess))
    FileFactory.deleteFile(tablestatus, FileFactory.getFileType(tablestatus))
  }

  def copyFile(source: String, target: String): Unit = {
    val in = FileFactory.getDataInputStream(source, FileFactory.getFileType(source))
    val out = FileFactory.getDataOutputStream(target, FileFactory.getFileType(target))
    try  {
      IOUtils.copyBytes(in, out, 4096)
    } finally {
      try {
        CarbonUtil.closeStream(in)
        CarbonUtil.closeStream(out)
      } catch {
        case exception: IOException =>
          LOGGER.error(exception.getMessage, exception)
      }
    }
  }

  override def afterAll = {
    dropTable
  }

  def dropTable = {
    sql("drop table if exists collect_segments")
  }

}
