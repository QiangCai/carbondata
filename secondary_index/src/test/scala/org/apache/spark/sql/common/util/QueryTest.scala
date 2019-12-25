/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.common.util

import java.util.{Locale, TimeZone}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import scala.collection.JavaConversions._

import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.test.TestQueryExecutor

class QueryTest extends PlanTest {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  CarbonProperties.getInstance().addProperty(
    CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
    CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_DEFAULT
  )
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)

  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

  /**
   * Runs the plan and makes sure the answer contains all of the keywords, or the
   * none of keywords are listed in the answer
   * @param df the [[DataFrame]] to be executed
   * @param exists true for make sure the keywords are listed in the output, otherwise
   *               to make sure none of the keyword are not listed in the output
   * @param keywords keyword in string array
   */
  def checkExistence(df: DataFrame, exists: Boolean, keywords: String*) {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      if (exists) {
        assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
      } else {
        assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
      }
    }
  }

  def sqlTest(sqlString: String, expectedAnswer: Seq[Row])(implicit sqlContext: SQLContext) {
    test(sqlString) {
      checkAnswer(sqlContext.sql(sqlString), expectedAnswer)
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  def sql(sqlText: String): DataFrame = TestQueryExecutor.INSTANCE.sql(sqlText)

  val sqlContext: SQLContext = TestQueryExecutor.INSTANCE.sqlContext

  val storeLocation = TestQueryExecutor.storeLocation.replaceAll("\\\\", "/")
  val warehouse = TestQueryExecutor.warehouse.replaceAll("\\\\", "/")
  val resourcesPath = TestQueryExecutor.resourcesPath.replaceAll("\\\\", "/")
  val pluginResourcesPath = TestQueryExecutor.pluginResourcesPath.replaceAll("\\\\", "/")
  val integrationPath = TestQueryExecutor.integrationPath.replaceAll("\\\\", "/")
  val location = s"$integrationPath/spark-common/target/store_location"
}

object QueryTest {

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.toSeq) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (!isSorted) converted.sortBy(_.toString()) else converted
    }
    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           |Results do not match for query:
           |${df.queryExecution}
           |== Results ==
           |${
          sideBySide(
            s"== Correct Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
            s"== Spark Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")
        }
      """.stripMargin
      return Some(errorMessage)
    }

    return None
  }
}
