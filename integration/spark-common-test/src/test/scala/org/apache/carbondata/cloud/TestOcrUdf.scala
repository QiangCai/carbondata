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

package org.apache.carbondata.cloud

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.sql.test.TestQueryExecutor
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties

class TestOcrUdf extends QueryTest with BeforeAndAfterAll {

  val dbName = "ocr_db"

  override protected def beforeAll(): Unit = {
    sql(s"DROP DATABASE IF EXISTS $dbName CASCADE")
    sql(s"CREATE DATABASE $dbName")
    sql(s"USE $dbName")
    prepareTable("base_table")
    val confFile = TestQueryExecutor.projectPath + "/cloud/conf/carbon.properties"
    val properties = new Properties()
    properties.load(new FileInputStream(confFile))
    val iterator = properties.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      CarbonProperties
        .getInstance()
        .addProperty(entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String])
    }
    CloudUdfRegister.register(sqlContext.sparkSession)
  }

  private def readFileToBytes(name: String): Array[Byte] = {
    FileUtils.readFileToByteArray(new File(s"$resourcesPath/ocr/$name"))
  }

  private def prepareTable(tableName: String): Unit = {
    loadImageIntoTable("customs-form-en-demo.jpg", "image_form")
    loadImageIntoTable("general-table-demo.png", "image_general_table")
    loadImageIntoTable("handwriting-demo.jpg", "image_handwriting")
    loadImageIntoTable("mvs-invoice-demo.jpg", "image_mvs_invoice")
    loadImageIntoTable("vehicle-license-demo.png", "image_vehicle_license")
    loadImageIntoTable("driver-license-demo.png", "image_driver_license")
    loadImageIntoTable("general-text-demo.jpg", "image_general_text")
    loadImageIntoTable("id-card-demo.png", "image_id_card")
    loadImageIntoTable("vat-invoice-demo.jpg", "image_vat_invoice")
    // loadImageIntoTable("plate-number-demo.jpg", "image_plate_number")
  }

  def loadImageIntoTable(image: String, table: String): Unit = {
    val rdd = sqlContext
      .sparkSession
      .sparkContext
      .parallelize(Seq(
        Record(image, readFileToBytes(image))
      ))
    val df = sqlContext.createDataFrame(rdd)
    df.createOrReplaceTempView(table)
  }

  override protected def afterAll(): Unit = {
    sql(s"use default")
    // sql(s"DROP DATABASE $dbName CASCADE")
  }

//  test("Test ocr: form") {
//    testOcrUdf("image_form", "table_form", "ocr_form")
//  }
//
//  test("Test ocr: general_table") {
//    testOcrUdf("image_general_table", "table_general_table", "ocr_general_table")
//  }
//
//  test("Test ocr: handwriting") {
//    testOcrUdf("image_handwriting", "table_handwriting", "ocr_handwriting")
//  }
//
//  test("Test ocr: mvs_invoice") {
//    testOcrUdf("image_mvs_invoice", "table_mvs_invoice", "ocr_mvs_invoice")
//  }
//
//  test("Test ocr: vehicle_license") {
//    testOcrUdf("image_vehicle_license", "table_vehicle_license", "ocr_vehicle_license")
//  }
//
//  test("Test ocr: driver_license") {
//    testOcrUdf("image_driver_license", "table_driver_license", "ocr_vehicle_license")
//  }
//
//  test("Test ocr: general_text") {
//    testOcrUdf("image_general_text", "table_general_text", "ocr_general_text")
//  }
//
//  test("Test ocr: id_card") {
//    testOcrUdf("image_id_card", "table_id_card", "ocr_id_card")
//  }
//
//  test("Test ocr: vat_invoice") {
//    testOcrUdf("image_vat_invoice", "table_vat_invoice", "ocr_vat_invoice")
//  }

//  test("Test ocr: plate_number") {
//    testOcrUdf("image_plate_number", "table_plate_number", "ocr_plate_number")
//  }

  def testOcrUdf(imageTable: String, labelTable: String, udf: String): Unit = {
    sql(s"drop table if exists $labelTable")
    sql(
      s"""create table $labelTable(
         | name string,
         | image binary
         | )
         | STORED AS carbondata
         | tblproperties('vector'='true')
      """.stripMargin)

    sql(s"insert into $labelTable select * from $imageTable")

    sqlContext.udf.register("size", (image: Array[Byte]) => image.length)

    sql(s"select name, size(image) image_byte_size from $labelTable").show(100, false)

    sql(
      s"""
         | insert columns(result)
         |  into table $labelTable
         | select $udf(image) from $labelTable
       """.stripMargin).show(100, false)

    sql(s"select name, result from $labelTable" ).show(100, false)

    sql(
      s"""
         | insert columns(result_struct json)
         |  into table $labelTable
         | select $udf(image) from $labelTable
       """.stripMargin).show(100, false)

    sql(s"describe formatted $labelTable").show(100, false)

    sql(s"select name, result_struct from $labelTable" ).show(100, false)
  }
}

case class Record(
    name: String,
    image: Array[Byte]
)
