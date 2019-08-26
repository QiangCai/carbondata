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
package org.apache.carbon.flink.writers

import java.sql.Timestamp
import java.util.Properties

import org.apache.carbon.flink.adapter.ProxyFileSystem
import org.apache.carbon.flink.{CarbonWriterFactory, CarbonWriterProperty, TestSource}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.Spark2TestQueryExecutor
import org.apache.spark.sql.test.util.QueryTest

class TestS3Writer extends QueryTest {

  def test(): Unit = {
    TestSource.DATA_COUNT.set(0)

    val accessKey = "OBS AK"
    val secretKey = "OBS SK"
    val endpoint = "OBS Endpoint"
    val bucket = "OBS Bucket"

    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_PATH, "/tmp/locks")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION, CarbonCommonConstants.S3A_PREFIX + bucket + "/root")
    setSparkSessionConfiguration(accessKey, secretKey, endpoint)

    val tableName = "flink_to_carbon_s3"

    // Initialize test environment.
    sql(s"drop table if exists $tableName")
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short, longField long,
         | doubleField double, boolField boolean, dateField date, timeField timestamp, decimalField decimal(8,2))
         | STORED AS carbondata
      """.stripMargin
    )

    // Set up the execution environment.
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
    )
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT
    )

    val writerProperties = new Properties()
    writerProperties.setProperty(CarbonWriterProperty.TYPE, "S3")
    writerProperties.setProperty(CarbonWriterProperty.LOCAL_OUTPUT_PATH, System.getProperty("java.io.tmpdir") + "/" + tableName + "/")
    writerProperties.setProperty(S3Writer.S3_ACCESS_KEY, accessKey)
    writerProperties.setProperty(S3Writer.S3_SECRET_KEY, secretKey)
    writerProperties.setProperty(S3Writer.S3_ENDPOINT, endpoint)
    writerProperties.setProperty(S3Writer.S3_BUCKET, bucket)
    writerProperties.setProperty(S3Writer.S3_ROOT_PATH, "data/default/" + tableName + "/")

    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(2)
    environment.enableCheckpointing(20000)
    environment.setRestartStrategy(RestartStrategies.noRestart())

    // Build source stream.
    val source = new TestSource(
      """
        |{
        |	"stringField": "test",
        |	"intField": 26,
        |	"shortField": 26,
        |	"longField": 1234567,
        |	"doubleField": 23.3333,
        |	"boolField": false,
        |	"dateField": "2019-03-02",
        |	"timeField": "2019-02-12 03:03:34",
        |	"decimalField" : 55.35,
        |	"binaryField" : "abc"
        |}
      """.stripMargin)
    val stream = environment.addSource(source)
    val streamSink = StreamingFileSink.forBulkFormat(
      new Path(ProxyFileSystem.DEFAULT_URI),
      new CarbonWriterFactory[String](None, tableName, "{}", writerProperties)
    ).build()
    stream.addSink(streamSink)

    // Execute the flink environment
    environment.execute()
    streamSink.close()

    // Check result.
    checkAnswer(
      sql(s"select * from $tableName order by stringfield limit 1"),
      Seq(Row("test1", 26, 26, 1234567, 23.3333, false, java.sql.Date.valueOf("2019-03-02"), Timestamp.valueOf("2019-02-12 03:03:34"), 55.35))
    )
    checkAnswer(
      sql(s"select count(1) from $tableName"),
      Seq(Row(TestSource.DATA_COUNT.get()))
    )
  }

  def setSparkSessionConfiguration(accessKey: String, secretKey: String, endpoint: String) : Unit = {
    val sparkSessionConfiguration = Spark2TestQueryExecutor.spark.sessionState.conf
    sparkSessionConfiguration.setConfString("fs.s3a.access.key", accessKey)
    sparkSessionConfiguration.setConfString("fs.s3a.secret.key", secretKey)
    sparkSessionConfiguration.setConfString("fs.s3a.endpoint", endpoint)
    sparkSessionConfiguration.setConfString("fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
    sparkSessionConfiguration.setConfString("fs.obs.access.key", accessKey)
    sparkSessionConfiguration.setConfString("fs.obs.secret.key", secretKey)
    sparkSessionConfiguration.setConfString("fs.obs.endpoint", endpoint)
    sparkSessionConfiguration.setConfString("fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem")
  }

}