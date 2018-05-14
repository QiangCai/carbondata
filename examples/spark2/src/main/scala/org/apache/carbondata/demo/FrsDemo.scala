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
import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.carbondata.hadoop.CarbonProjection
import org.apache.carbondata.spark.rdd.PushDownUdfRDD

object FrsDemo {

  def main(args: Array[String]): Unit = {
    val spark = ExampleUtils.createCarbonSession("FrsDemo")
    spark.sparkContext.setLogLevel("INFO")
    prepareTable(spark, createTable = false)
    // queryBySQL(spark)

    val searchFeature = new FeatureFloats()
    searchFeature.setValue(Array(3.0F, 4.0F, 5.0F))
    queryByAPI(spark, searchFeature)
    spark.close()
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

  def queryBySQL(spark: SparkSession): Unit = {

    spark.udf.register(
      "array_sum",
      (featrue: Seq[Int]) => featrue.sum
    )
    spark.sql(
      "select id, name, array_sum(feature) as feature_sum from frs_demo where size >= 20"
    ).show(100, false)
  }

  def queryByAPI(spark: SparkSession, searchFeature: Feature[Array[Float]]): Unit = {
    val filter = GreaterThanOrEqual("size", 20)
    val scanRDD = buildScan(spark,
      Array("id", "name", "feature"),
      Array[Filter](filter),
      searchFeature)
    scanRDD.collect().sortBy(row => row.getFloat(3)).take(50).foreach(println(_))
  }

  def buildScan(
      spark: SparkSession,
      requiredColumns: Array[String],
      filters: Array[Filter],
      searchFeature: Feature[Array[Float]]
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
      new Model("KNNSearch", searchFeature)
    )
  }

}
