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

package org.apache.carbondata.sql.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCarbonShowSegmentsCommand  extends QueryTest with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    dropTable()
    createTable()
  }

  override protected def afterAll(): Unit = {
    dropTable()
  }

  private def createTable(): Unit = {
    sql(
      """
        | CREATE TABLE show_segments
        | (empno int, empname String, designation String, doj Timestamp, workgroupcategory int,
        |  workgroupcategoryname String, deptno int, deptname String, projectcode int,
        |  projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,
        |  salary int)
        | STORED BY 'org.apache.carbondata.format'
        | TBLPROPERTIES('SORT_SCOPE'='LOCAL_SORT','SORT_COLUMNS'='designation, empname')
      """.stripMargin)
  }

  private def dropTable(): Unit = {
    sql("drop table if exists show_segments")
  }

  private def loadData(sortScope: String): Unit = {
    sql(s"LOAD DATA INPATH '$resourcesPath/data.csv' INTO TABLE show_segments options('SORT_SCOPE'='$sortScope') ")
  }

  test("test show segments with sort_columns") {
    loadData("NO_SORT")
    loadData("LOCAL_SORT")
    loadData("GLOBAL_SORT")
    loadData("BATCH_SORT")
    loadData("NO_SORT")
    checkAnswer(sql("show segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("3","true","designation,empname"),
        Row("2","true","designation,empname"),
        Row("1","true","designation,empname"),
        Row("0","false","")
      )
    )
    checkAnswer(sql("show history segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("3","true","designation,empname"),
        Row("2","true","designation,empname"),
        Row("1","true","designation,empname"),
        Row("0","false","")
      )
    )

    sql("alter table show_segments compact 'minor'")
    checkAnswer(sql("show segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("3","true","designation,empname"),
        Row("2","true","designation,empname"),
        Row("1","true","designation,empname"),
        Row("0.1","true","designation,empname"),
        Row("0","false","")
      )
    )
    checkAnswer(sql("show History segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("3","true","designation,empname"),
        Row("2","true","designation,empname"),
        Row("1","true","designation,empname"),
        Row("0.1","true","designation,empname"),
        Row("0","false","")
      )
    )

    sql("clean files for table show_segments")
    checkAnswer(sql("show segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("0.1","true","designation,empname")
      )
    )
    checkAnswer(sql("show History segments for table show_segments").select("SegmentSequenceId", "Is Sorted", "Sort Columns"),
      Seq(
        Row("4","false",""),
        Row("3","",""),
        Row("2","",""),
        Row("1","",""),
        Row("0.1","true","designation,empname"),
        Row("0","","")
      )
    )
  }

}
