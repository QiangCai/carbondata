package org.apache.spark.sql.execution.command.mutation

import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.spark.util.CarbonSparkUtil
import org.apache.spark.sql.common.util.Spark2QueryTest
import org.apache.spark.sql.hive.CarbonRelation
import org.scalatest.BeforeAndAfterAll

class CarbonTruncateCommandTest extends Spark2QueryTest with BeforeAndAfterAll {

  test("test truncate table") {
    val index: Long = System.currentTimeMillis()
    sql(s"create table dli_stored$index(id int, name string) stored as carbondata")
    sql(s"insert into table dli_stored$index select 2,'aa'")
    sql(s"truncate table dli_stored$index")
    assert(getTableSize("default", s"dli_stored$index") == 0)
  }

  test("test truncate partition table") {
    val index: Long = System.currentTimeMillis()
    sql(s"create table dli_stored$index(id int) stored as carbondata " +
      s"partitioned by (name string)")
    sql(s"insert into table dli_stored$index select 2,'aa'")
    sql(s"truncate table dli_stored$index")
    assert(getTableSize("default", s"dli_stored$index") == 0)
  }

  private def getTableSize(databaseName: String, tableName:String) :Long={
    val table = CarbonMetadata.getInstance.getCarbonTable(databaseName, tableName)
    val relation = CarbonRelation(databaseName, tableName,
      CarbonSparkUtil.createSparkMeta(table), table)
    relation.sizeInBytes
  }

}
