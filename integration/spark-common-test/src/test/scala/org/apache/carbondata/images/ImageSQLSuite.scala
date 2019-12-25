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

package org.apache.carbondata.images

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class ImageSQLSuite extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
  }

  test("test imageinfo udf") {
    val dirPath = resourcesPath + "/image"
    sql("DROP TABLE IF EXISTS imagesourcetable")
    sql("DROP TABLE IF EXISTS carbontable")

    sql("CREATE TEMPORARY TABLE imagesourcetable USING binaryfile OPTIONS(path='" + dirPath + "')")
    val sqlOnImageSource = sql("SELECT length, " +
      "IMAGEINFO(content).height, IMAGEINFO(content).width," +
      "IMAGEINFO(content).format FROM imagesourcetable")

    val expectedRows = Seq(Row(11665, 200, 1112, "JPEG"),Row(11004, 98, 545, "png"),
     Row(5723, 98, 545, "JPEG"),Row(5723, 98, 545, "JPEG"))
    checkAnswer(sqlOnImageSource, expectedRows)

    sql(
      """
        | CREATE TABLE carbontable(path STRING, modificationTime TIMESTAMP, length LONG, height INT, width INT, format String, content BINARY)
        | STORED AS carbondata
      """.stripMargin)
    sql(s"INSERT INTO carbontable SELECT path, modificationTime, length, " +
    "IMAGEINFO(content).height, IMAGEINFO(content).width," +
      "IMAGEINFO(content).format, content FROM imagesourcetable").show()

    val sqlOnCarbonTable = sql("SELECT length, height, width, format FROM carbontable")

    checkAnswer(sqlOnCarbonTable, sqlOnImageSource)
    sql("DROP TABLE IF EXISTS carbontable")
    sql("DROP TABLE IF EXISTS imagesourcetable")
  }


  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS carbontable")
    sql("DROP TABLE IF EXISTS imagesourcetable")
  }
}


