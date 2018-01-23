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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object CarbonSessionExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("WARN")

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
      CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_MERGE_FILES)

    spark.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE carbon_table (
         | col1 int,
         | col2 string
         | )
         | STORED BY 'carbondata'
       """.stripMargin)

    (0 to 30).foreach(i =>
      spark.sql(s"insert into carbon_table select $i, 'a'")
    )

    spark.sql("select * from carbon_table").show(100, false)

    (0 to 600).foreach(_ => Thread.sleep(1000))

    spark.stop()
  }

}