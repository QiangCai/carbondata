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
package org.apache.carbondata.spark.testsuite.secondaryindex

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.sdk.file.{CarbonWriter, Schema}

/**
 * test cases for IUD data retention on SI tables
 */
class TestSecondaryIndexWithIUD extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists dest")
    sql("drop table if exists source")
    sql("drop table if exists test")
    sql("drop table if exists sitestmain")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest_parquet1")
  }

  test("test index with IUD delete all_rows") {

    sql(
      "create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    sql("drop index if exists index_dest1 on dest")
    sql("create index index_dest1 on table dest (c3) AS 'carbondata'")
    sql("drop index if exists index_dest2 on dest")
    //create second index table , result should be same
    sql("create index index_dest2 on table dest (c3,c5) AS 'carbondata'")
    // delete all rows in the segment
    sql("delete from dest d where d.c2 not in (56)").show
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )
    sql("show segments for table index_dest1").show(false)
    assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
             .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
             .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))

    // execute clean files
    sql("clean files for table dest")

    sql("show segments for table index_dest2").show()
    val exception_index_dest1 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
        .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    }
    val exception_index_dest2 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
        .equals(SegmentStatus.MARKED_FOR_DELETE.getMessage))
    }

    //load again and check result
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )


  }

  test("test index with IUD delete all_rows-1") {
    sql(
      "create table source (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table source""")
    sql("drop index if exists index_source1 on source")
    sql("create index index_source1 on table source (c5) AS 'carbondata'")
    // delete (5-1)=4 rows
    try {
      sql("""delete from source d where d.c2 in (1,2,3,4)""").show
      assert(false)
    }
    catch {
      case ex: Exception => assert(true)
        // results should not be same
        val exception = intercept[Exception] {
          checkAnswer(
            sql("""select c5 from source"""),
            sql("""select c5 from index_source1""")
          )
        }
    }
    // crete second index table
    sql("drop index if exists index_source2 on source")
    sql("create index index_source2 on table source (c3) AS 'carbondata'")
    // result should be same
      checkAnswer(
        sql("""select c3 from source"""),
        sql("""select c3 from index_source2""")
      )
    sql("clean files for table source")
    sql("show segments for table index_source2").show()
    assert(sql("show segments for table index_source2").collect()(0).get(1).toString()
      .equals(SegmentStatus.SUCCESS.getMessage))
  }

  test("test index with IUD delete using Join") {
    sql(
      "create table test (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("drop index if exists index_test1 on test")
    sql("create index index_test1 on table test (c3) AS 'carbondata'")
    // delete all rows in the segment
    sql("delete from test d where d.c2 not in (56)").show
    checkAnswer(
      sql(
        "select test.c3, index_test1.c3 from test right join index_test1  on test.c3 =  " +
        "index_test1.c3"),
      Seq())
  }

  test("test if secondary index gives correct result on limit query after row deletion") {
    sql("drop table if exists t10")
    sql("create table t10(id int, country string) STORED AS carbondata TBLPROPERTIES" +
        "('DICTIONARY_INCLUDE'='id')").show()
    sql("create index si3 on table t10(country) AS 'carbondata'")
    sql(
      s" load data INPATH '$pluginResourcesPath/IUD/sample_1.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    sql(
      s" load data INPATH '$pluginResourcesPath/IUD/sample_2.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    try {
      sql("delete from t10 where id in (1,2)").show()
    assert(false)
    }
    catch {
      case ex: Exception => assert(true)
    }
    sql(" select *  from t10").show()
    checkAnswer(sql(" select country from t10 where country = 'china' order by id limit 1"), Row("china"))
  }

  test("test index with IUD delete and compaction") {
    sql("drop table if exists test")
    sql(
      "create table test (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("drop index if exists index_test1 on test")
    sql("create index index_test1 on table test (c3) AS 'carbondata'")
    sql("delete from test d where d.c2 = '1'").show
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("alter table test compact 'major'")
    // delete all rows in the segment
    sql("delete from test d where d.c2 not in (56)").show
    checkAnswer(
      sql(
        "select test.c3, index_test1.c3 from test right join index_test1  on test.c3 =  " +
        "index_test1.c3"),
      Seq())
  }

  // DTS2018100903355
  test("test if secondary index gives correct result after updation in maintable") {
    sql("drop table if exists sitestmain")
    sql("Create Table sitestmain (id int,dim1 string,name string,tech string,measure int,amount " +
        "int,dim2 string,M1 int,dim3 string,M2 int,dim4 string,dim5 string,M3 int,dim6 string," +
        "dim7 string,M4 int,dim8 string,dim9 string,M5 int,dim10 string,dim11 string,dim12 " +
        "string,M6 int,dim13 string,dim14 string,dim15 string,M7 int,dim16 string,dim17 string," +
        "dim18 string,dim19 string) STORED AS carbondata tblproperties" +
        "('dictionary_include'='dim1,name,tech,dim2,dim3,dim4,dim5,dim6,dim7,dim8,dim9,dim10," +
        "dim11,dim12,dim13,dim14,dim15,dim16,dim17,dim18,dim19')")

    sql(s"LOAD DATA INPATH '$pluginResourcesPath/IUD/hugedataSI_ful_new.csv' into table sitestmain " +
        "OPTIONS('DELIMITER'=',' , 'QUOTECHAR'='\"','FILEHEADER'='id,dim1,name,tech,measure," +
        "amount,dim2,M1,dim3,M2,dim4,dim5,M3,dim6,dim7,M4,dim8,dim9,M5,dim10,dim11,dim12,M6," +
        "dim13,dim14,dim15,M7,dim16,dim17,dim18,dim19')")
    val count = sql("select * from sitestmain where name='Cathy'").count()
    sql("update sitestmain set (name) = ('Revathi') where name='Cathy'").show()
    sql("create index siindex on table sitestmain(name) AS 'carbondata'")
    checkAnswer(sql("select name from sitestmain where name='Revathi' limit 1"),
      Seq(Row("Revathi")))
    assertResult(count)(sql("select * from sitestmain where name='Revathi'").count())
  }

  // DTS2019010208198
  test("test set segments with SI") {
    sql("drop table if exists dest")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    sql("drop index if exists index_dest1 on dest")
    sql("create index index_dest1 on table dest (c3) AS 'carbondata'")
    checkAnswer(sql("select count(*) from dest"), Seq(Row(10)))
    sql("set carbon.input.segments.default.dest=0")
    checkAnswer(sql("select count(*) from dest"), Seq(Row(5)))
    checkAnswer(sql("select count(*) from index_dest1"), Seq(Row(5)))
  }

  test("Test block secondary index creation on external table") {
    var writerPath = new File(this.getClass.getResource("/").getPath
                              +
                              "../." +
                              "./target/SparkCarbonFileFormat/WriterOutput/")
      .getCanonicalPath
    writerPath = writerPath.replace("\\", "/")
    val schema = new StringBuilder()
      .append("[ \n")
      .append("   {\"NaMe\":\"string\"},\n")
      .append("   {\"age\":\"int\"},\n")
      .append("   {\"height\":\"double\"}\n")
      .append("]")
      .toString()

    try {
      val builder = CarbonWriter.builder()
      val writer =
        builder.outputPath(writerPath)
          .uniqueIdentifier(
            System.currentTimeMillis).withBlockSize(2)
          .withCsvInput(Schema.parseJson(schema)).writtenBy("TestNonTransactionalCarbonTable")
          .build()
      var i = 0
      while (i < 2) {
        writer.write(Array[String]("robot" + i, String.valueOf(i), String.valueOf(i.toDouble / 2)))
        i = i + 1
      }
      writer.close()
    } catch {
      case ex: Throwable => throw new RuntimeException(ex)
    }
    sql("drop table if exists test")
    sql(s"create external table test STORED AS carbondata location '$writerPath'")
    val exception = intercept[MalformedCarbonCommandException] {
      sql("create index idx on table test(cert_no) AS 'carbondata'")
    }
    assert(exception.getMessage
      .contains("Unsupported operation on non transactional table"))
  }

  test("test SI with Union and Union All with same table") {
    sql("drop table if exists dest")
    sql("drop table if exists dest_parquet")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest  where c3 = 'abc'"),
      sql("select c3 from dest_parquet where c3 = 'abc' union select c3 from " +
          "dest_parquet where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest where c3 = 'abc' union all " +
                    "select c3 from dest where c3 = 'abc'"),
      sql("select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
          "dest_parquet  where c3 = 'abc'"))
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest")
  }

  test("test SI with Union and Union All with different table") {
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest_parquet1")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create table dest_parquet1 stored as parquet select * from dest")
    sql("create table dest1 STORED AS carbondata select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    sql("create index index_dest1 on table dest1 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest1  where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union select c3 from " +
        "dest_parquet1 where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest where c3 = 'abc' union all select c3 from dest1 " +
                    "where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union all select c3 " +
        "from dest_parquet1 where c3 = 'abc'"))
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet1")
    sql("drop table if exists dest_parquet")
  }

  test("test SI with more than 2 Union and Union All with different table") {
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create table dest_parquet1 stored as parquet select * from dest")
    sql("create table dest1 STORED AS carbondata select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    sql("create index index_dest1 on table dest1 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest1  " +
      "where c3 = 'abc' union select c3 from dest1  where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union select c3 from dest_parquet1" +
        " where c3 = 'abc' union select c3 from dest_parquet1  where c3 = 'abc'"))

    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union all select c3 from dest1 " +
      "where c3 = 'abc' union all select c3 from dest1  where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
        "dest_parquet1 where c3 = 'abc' union all select c3 from dest_parquet1 " +
        "where c3 = 'abc'"))
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
  }

  test("test SI with more than 2 Union and Union All with same table") {
    sql("drop table if exists dest")
    sql("drop table if exists dest_parquet")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest  where c3 = 'abc' " +
      "union select c3 from dest  where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union select c3 from dest_parquet " +
        "where c3 = 'abc'"))
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union all select c3 from dest  where c3 = 'abc' " +
      "union all select c3 from dest  where c3 = 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union all select c3 from dest_parquet  " +
        "where c3 = 'abc' union all select c3 from dest_parquet  where c3 = 'abc'"))
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest")
  }

  test("test SI with join") {
    sql("drop table if exists dest")
    sql("drop table if exists dest_parquet")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    checkAnswer(sql("select t1.c3,t2.c3 from dest t1, dest t2 where t1.c3=t2.c3 and t1.c3 = 'abc'"),
      sql("select t1.c3,t2.c3 from dest_parquet t1, dest t2 where t1.c3=t2.c3 and t1.c3 = 'abc'"))
    sql("drop table if exists dest")
    sql("drop table if exists dest_parquet")
  }

  test("test SI with Union and Union All with donotPushtoSI operations") {
    sql("drop table if exists dest")
    sql("drop table if exists dest_parquet")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest  where c3 != 'abc'"),
      sql("select c3 from dest_parquet where c3 = 'abc' union select c3 from " +
          "dest_parquet where c3 != 'abc'"))
    checkAnswer(sql("select c3 from dest where c3 = 'abc' union all " +
                    "select c3 from dest where c3 != 'abc'"),
      sql("select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
          "dest_parquet  where c3 != 'abc'"))
    checkAnswer(sql(
      "select c3 from dest where c3 like '%bc' union select c3 from dest  where c3 not like '%bc'"),
      sql("select c3 from dest_parquet where c3 like '%bc' union select c3 from " +
          "dest_parquet where c3 not like '%bc'"))
    checkAnswer(sql("select c3 from dest where c3 like '%bc' union all " +
                    "select c3 from dest where c3 not like '%bc'"),
      sql("select c3 from dest_parquet where c3 like '%bc' union all select c3 from " +
          "dest_parquet  where c3 not like '%bc'"))
    checkAnswer(sql(
      "select c3 from dest where c3 in ('abc') union select c3 from dest  where c3 not in ('abc')"),
      sql("select c3 from dest_parquet where c3 in ('abc') union select c3 from " +
          "dest_parquet where c3 not in ('abc')"))
    checkAnswer(sql("select c3 from dest where c3 in ('abc') union all " +
                    "select c3 from dest where c3 not in ('abc')"),
      sql("select c3 from dest_parquet where c3 in ('abc') union all select c3 from " +
          "dest_parquet  where c3 not in ('abc')"))
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest  where ni(c3 = 'abc')"),
      sql("select c3 from dest_parquet where c3 = 'abc' union select c3 from " +
          "dest_parquet where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest where c3 = 'abc' union all " +
                    "select c3 from dest where ni(c3 ='abc')"),
      sql("select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
          "dest_parquet  where c3 = 'abc'"))
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest")
  }

  test("test SI with more than 2 Union and Union All with different table donotPushtoSI operations") {
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest_parquet1")
    sql("create table dest (c1 string,c2 int,c3 string,c5 string) STORED AS " +
        "carbondata")
    sql("insert into dest values('a',1,'abc','b')")
    sql("create table dest_parquet stored as parquet select * from dest")
    sql("create table dest_parquet1 stored as parquet select * from dest")
    sql("create table dest1 STORED AS carbondata select * from dest")
    sql("create index index_dest on table dest (c3) AS 'carbondata'")
    sql("create index index_dest1 on table dest1 (c3) AS 'carbondata'")
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest1  " +
      "where c3 = 'abc' union select c3 from dest1  where c3 != 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union select c3 from dest_parquet1" +
        " where c3 = 'abc' union select c3 from dest_parquet1  where c3 != 'abc'"))

    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union all select c3 from dest1 " +
      "where c3 = 'abc' union all select c3 from dest1  where c3 != 'abc'"),
      sql(
        "select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
        "dest_parquet1 where c3 = 'abc' union all select c3 from dest_parquet1 " +
        "where c3 != 'abc'"))
    checkAnswer(sql(
      "select c3 from dest where c3 like '%bc' union select c3 from dest1  where c3 not like '%bc'"),
      sql("select c3 from dest_parquet where c3 like '%bc' union select c3 from " +
          "dest_parquet1 where c3 not like '%bc'"))
    checkAnswer(sql("select c3 from dest where c3 like '%bc' union all " +
                    "select c3 from dest1 where c3 not like '%bc'"),
      sql("select c3 from dest_parquet where c3 like '%bc' union all select c3 from " +
          "dest_parquet1  where c3 not like '%bc'"))
    checkAnswer(sql(
      "select c3 from dest where c3 in ('abc') union select c3 from dest1  where c3 not in ('abc')"),
      sql("select c3 from dest_parquet where c3 in ('abc') union select c3 from " +
          "dest_parquet1 where c3 not in ('abc')"))
    checkAnswer(sql("select c3 from dest where c3 in ('abc') union all " +
                    "select c3 from dest1 where c3 not in ('abc')"),
      sql("select c3 from dest_parquet where c3 in ('abc') union all select c3 from " +
          "dest_parquet1  where c3 not in ('abc')"))
    checkAnswer(sql(
      "select c3 from dest where c3 = 'abc' union select c3 from dest1  where ni(c3 = 'abc')"),
      sql("select c3 from dest_parquet where c3 = 'abc' union select c3 from " +
          "dest_parquet1 where c3 = 'abc'"))
    checkAnswer(sql("select c3 from dest where c3 = 'abc' union all " +
                    "select c3 from dest1 where ni(c3 ='abc')"),
      sql("select c3 from dest_parquet where c3 = 'abc' union all select c3 from " +
          "dest_parquet1  where c3 = 'abc'"))
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest")
    sql("drop table if exists dest1")
  }


  override def afterAll: Unit = {
    sql("drop table if exists dest")
    sql("drop table if exists source")
    sql("drop table if exists test")
    sql("drop table if exists sitestmain")
    sql("drop table if exists dest1")
    sql("drop table if exists dest_parquet")
    sql("drop table if exists dest_parquet1")
  }
}
