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
package org.apache.carbondata.spark.testsuite.mergedata

import java.io.{File, PrintWriter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.util.Random

class CarbonDataFileMergeTestCaseOnSI
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 160000
    createFile(file2, n * 4, n)
    sql("drop database if exists dataFileMerge cascade")
    sql("create database dataFileMerge")
    sql("use dataFileMerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
  }

  override protected def afterAll(): Unit = {
    deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
    sql("use default")
    sql("drop database if exists dataFileMerge cascade")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Verify correctness of data file merge") {
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      "CREATE INDEX indexmerge_index1 on table indexmerge (name) AS 'carbondata' tblproperties" +
      "('table_blocksize'='1')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from indexmerge where name='n164419'""").collect()
    sql("clean files for table indexmerge_index1")
    checkAnswer(sql("""Select count(*) from indexmerge where name='n164419'"""), rows)
    assert(getDataFileCount("indexmerge_index1", "0") < 7)
  }

  test("Verify command of data file merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
      "CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata' " +
      "tblproperties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("REBUILD INDEX nonindexmerge_index1").collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    sql("clean files for table nonindexmerge_index1")
    assert(getDataFileCount("nonindexmerge_index1", "0") < 7)
    assert(getDataFileCount("nonindexmerge_index1", "1") < 7)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
  }

  test("Verify command of data file merge on segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata' " +
    "tblproperties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("REBUILD INDEX nonindexmerge_index2 WHERE SEGMENT.ID IN(0)").collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    sql("clean files for table nonindexmerge_index2")
    assert(getDataFileCount("nonindexmerge_index2", "0") < 7)
    assert(getDataFileCount("nonindexmerge_index2", "1") == 100)
    sql("REBUILD INDEX nonindexmerge_index2 WHERE SEGMENT.ID IN(1)").collect()
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    sql("clean files for table nonindexmerge_index2")
    assert(getDataFileCount("nonindexmerge_index2", "1") < 7)
    sql("clean files for table nonindexmerge_index2")
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
  }

  test("Verify command of REBUILD INDEX command with invalid segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(
      "CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata' " +
      "tblproperties('table_blocksize'='1')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    val exceptionMessage = intercept[RuntimeException] {
      sql("REBUILD INDEX nonindexmerge_index2 WHERE SEGMENT.ID IN(1,2)").collect()
    }.getMessage
    assert(exceptionMessage.contains("Rebuild index by segment id is failed. Invalid ID:"))
  }

  test("Verify index data file merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index3 on table nonindexmerge (name) AS 'carbondata' " +
    "tblproperties('table_blocksize'='1')")
    CarbonProperties.getInstance()
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("clean files for table nonindexmerge_index3")
    assert(getDataFileCount("nonindexmerge_index3", "0.1") < 11)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Verify index data file merge for compacted segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "true")
      .addProperty(CarbonInternalCommonConstants.CARBON_SI_SEGMENT_MERGE, "true")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    val rows = sql("""Select count(*) from nonindexmerge where name='n164419'""").collect()
    sql(
    "CREATE INDEX nonindexmerge_index4 on table nonindexmerge (name) AS 'carbondata' " +
    "tblproperties('table_blocksize'='1')")
    sql("clean files for table nonindexmerge_index4")
    assert(getDataFileCount("nonindexmerge_index4", "0.2") < 15)
    checkAnswer(sql("""Select count(*) from nonindexmerge where name='n164419'"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  private def getDataFileCount(tableName: String, segment: String): Int = {
    val table = CarbonEnv.getCarbonTable(None, tableName)(sqlContext.sparkSession)
    val path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    val carbonFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(CarbonTablePath
        .CARBON_DATA_EXT)
    })
    if (carbonFiles != null) {
      carbonFiles.length
    } else {
      0
    }
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(fileName);
      for (i <- start until (start + line)) {
        write
          .println(i + "," + "n" + i + "," + "c" + Random.nextInt(line) + "," + Random.nextInt(80))
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  private def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }

}
