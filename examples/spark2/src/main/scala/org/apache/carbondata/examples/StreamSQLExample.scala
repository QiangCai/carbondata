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

package org.apache.carbondata.examples

import java.io.File

import org.apache.carbondata.examples.util.ExampleUtils

// scalastyle:off println
object StreamSQLExample {
  def main(args: Array[String]) {

    // setup paths
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    val spark = ExampleUtils.createCarbonSession("StructuredStreamingExample", 4)
    val streamTableName = s"stream_table"

    val requireCreateTable = true

    if (requireCreateTable) {
      // drop table if exists previously
      spark.sql(s"DROP TABLE IF EXISTS $streamTableName")
      spark.sql("DROP TABLE IF EXISTS source")

      // Create target carbon table and populate with initial data
      spark.sql(
        s"""
           | CREATE TABLE $streamTableName(
           | id INT,
           | name STRING,
           | city STRING,
           | salary FLOAT
           | )
           | STORED AS carbondata
           | TBLPROPERTIES(
           | 'streaming'='true', 'sort_columns'='name')
          """.stripMargin)

      // batch load
      val path = s"$rootPath/examples/spark2/src/main/resources/streamSample.csv"
      spark.sql(
        s"""
           | LOAD DATA LOCAL INPATH '$path'
           | INTO TABLE $streamTableName
           | OPTIONS('HEADER'='true')
         """.stripMargin)
    }

    spark.sql(
      """
        | CREATE TABLE source (
        | id INT,
        | name STRING,
        | city STRING,
        | salary FLOAT
        | )
        | STORED AS carbondata
        | TBLPROPERTIES(
        | 'streaming'='source',
        | 'format'='kafka',
        | 'kafka.bootstrap.servers'='localhost:9092',
        | 'subscribe'='test')
      """.stripMargin)

    spark.sql(
      s"""
        | CREATE STREAM ingest ON TABLE $streamTableName
        | STMPROPERTIES(
        | 'trigger' = 'ProcessingTime',
        | 'interval' = '3 seconds')
        | AS SELECT * FROM source
      """.stripMargin)

    (1 to 1000).foreach { i =>
      spark.sql(s"select * from $streamTableName")
        .show(100, truncate = false)
      Thread.sleep(5000)
    }

    spark.stop()
    System.out.println("streaming finished")
  }

}

// scalastyle:on println
