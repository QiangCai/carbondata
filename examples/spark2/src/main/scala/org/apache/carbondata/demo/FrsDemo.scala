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

package org.apache.carbondata.demo

import java.io.File

import org.apache.spark.CarbonInputMetrics
import org.apache.spark.sql.hive.{CarbonMetaData, CarbonRelation}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.{CarbonEnv, CarbonSession, SparkSession}
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual}

import org.apache.carbondata.ai.model.{Feature, Model}
import org.apache.carbondata.ai.model.impl.FeatureFloats
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.PushDownUdfRDD

object FrsDemo {

  def main(args: Array[String]): Unit = {

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE, "10000")

    val spark = ExampleUtils.createCarbonSession("FrsDemo", 4)
    // spark.sparkContext.setLogLevel("INFO")
    prepareTable(spark, createTable = true)

    queryBySQL(spark)

    queryByAPI(spark)

    Thread.sleep(300000)
    spark.close()
  }

  def queryByAPI(spark: SparkSession): Unit = {
    val startTime = System.currentTimeMillis()
    val searchFeature = new FeatureFloats()
    searchFeature.setValue(Array(301.0F, 302.0F, 303.0F))

    val model = new Model("KNNSearch", searchFeature, 50)

    val filter = GreaterThanOrEqual("size", 20)
    val scanRDD = buildScan(spark,
      Array("id", "feature"),
      // Array[Filter](filter),
      Array.empty,
      model)
    scanRDD.collect().sortBy(_.getFloat(2)).take(model.getLimit).foreach(println(_))
    // scanRDD.sortBy(row => row.getFloat(2)).take(model.getLimit).foreach(println(_))

    val endTime = System.currentTimeMillis()
    println(s"time token: ${ endTime - startTime } ms")
  }

  def buildScan(
      spark: SparkSession,
      requiredColumns: Array[String],
      filters: Array[Filter],
      model: Model
  ): PushDownUdfRDD = {
    val carbonTable = CarbonEnv.getCarbonTable(Option("default"), "frs_demo")(spark)
    val identifier = carbonTable.getAbsoluteTableIdentifier
    val metaData = CarbonMetaData(Seq.empty, Seq.empty, carbonTable, null, false)
    val relation = CarbonRelation(
      carbonTable.getDatabaseName,
      carbonTable.getTableName,
      metaData,
      carbonTable
    )

    val filterExpression: Option[Expression] = filters.flatMap { filter =>
      CarbonFilters.createCarbonFilter(relation.schema, filter)
    }.reduceOption(new AndExpression(_, _))

    val projection = new CarbonProjection
    requiredColumns.foreach(projection.addColumn)
    CarbonSession.threadUnset(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP)
    val inputMetricsStats: CarbonInputMetrics = new CarbonInputMetrics
    new PushDownUdfRDD(
      spark,
      projection,
      filterExpression.orNull,
      identifier,
      carbonTable.getTableInfo.serialize(),
      carbonTable.getTableInfo,
      inputMetricsStats,
      model
    )
  }

  def queryBySQL(spark: SparkSession): Unit = {

    spark.udf.register(
      "array_sum",
      (featrue: Seq[Int]) => featrue.sum
    )
    spark.sql(
      "select id, name, array_sum(feature) as feature_sum from frs_demo where size >= 20"
    ).show(100, false)

    val startTime = System.currentTimeMillis()
    spark.sql(
      "select id, name, array_sum(feature) as feature_sum from frs_demo where size >= 20"
    ).show(100, false)
    val endTime = System.currentTimeMillis()
    println(s"time token: ${ endTime - startTime } ms")

  }

  def prepareTable(spark: SparkSession, createTable: Boolean): Unit = if (createTable) {
    spark.sql("DROP TABLE IF EXISTS frs_demo")
    // create table
    spark.sql(
      s"""
         | CREATE TABLE frs_demo(
         |   id INT,
         |   name STRING,
         |   size INT,
         |   feature ARRAY<INT>
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

    // load table
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '${ rootPath }/examples/spark2/src/main/resources/demo_data.csv'
         | INTO TABLE frs_demo
         | OPTIONS('HEADER'='false', 'COMPLEX_DELIMITER_LEVEL_1'='$$')
      """.stripMargin
    )
  }

}
