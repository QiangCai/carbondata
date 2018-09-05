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

import java.io.{File, PrintWriter}
import java.net.ServerSocket

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.streaming.parser.CarbonStreamParser

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
        | 'interval' = '3 seconds',
        | 'carbon.stream.parser'='org.apache.carbondata.streaming.parser.CSVStreamParserImp',
        | 'BAD_RECORDS_ACTION'='force')
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

  def showTableCount(spark: SparkSession, tableName: String): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        for (_ <- 0 to 1000) {
          spark.sql(s"select count(*) from $tableName").show(truncate = false)
          Thread.sleep(1000 * 3)
        }
      }
    }
    thread.start()
    thread
  }

  def startStreaming(spark: SparkSession, carbonTable: CarbonTable): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        var qry: StreamingQuery = null
        try {
          val readSocketDF = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test")
            .load()
          val df = readSocketDF.selectExpr("CAST(value AS STRING)")

          // Write data from socket stream to carbondata file
          qry = df.writeStream
            .format("carbondata")
            .trigger(ProcessingTime("5 seconds"))
            .option(
              "checkpointLocation",
              CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath))
            .option("dbName", "default")
            .option("tableName", "stream_table")
            .option(
              CarbonStreamParser.CARBON_STREAM_PARSER,
              CarbonStreamParser.CARBON_STREAM_PARSER_CSV)
            .start()

          qry.awaitTermination()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
            println("Done reading and writing streaming data")
        } finally {
          qry.stop()
        }
      }
    }
    thread.start()
    thread
  }

  def writeSocket(serverSocket: ServerSocket): Thread = {
    val thread = new Thread() {
      override def run(): Unit = {
        // wait for client to connection request and accept
        val clientSocket = serverSocket.accept()
        val socketWriter = new PrintWriter(clientSocket.getOutputStream())
        var index = 0
        for (_ <- 1 to 1000) {
          // write 5 records per iteration
          for (_ <- 0 to 1000) {
            index = index + 1
            socketWriter.println(index.toString + ",name_" + index
                                 + ",city_" + index + "," + (index * 10000.00).toString +
                                 ",school_" + index + ":school_" + index + index + "$" + index)
          }
          socketWriter.flush()
          Thread.sleep(1000)
        }
        socketWriter.close()
        System.out.println("Socket closed")
      }
    }
    thread.start()
    thread
  }
}

// scalastyle:on println
