package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestPreAggStreamingSelection extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists mainTableStreamingOne")
    sql("CREATE TABLE mainTable(id int, name string, city string, age string) STORED AS carbondata tblproperties('streaming'='true')")
    sql(
      s"""
         | CREATE DATAMAP agg0 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg1 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, sum(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg2 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, avg(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg3 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, sum(CASE WHEN age=35 THEN id ELSE 0 END)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg4 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, count(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg5 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, max(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql(
      s"""
         | CREATE DATAMAP agg6 ON TABLE mainTable
         | USING 'preaggregate' AS
         | SELECT name, min(age)
         | FROM mainTable GROUP BY name
         | """.stripMargin
    )
    sql("CREATE TABLE mainTableStreamingOne(id int, name string, city string, age smallint) STORED AS carbondata tblproperties('streaming'='true')")
    sql(
      s"""
         | CREATE DATAMAP aggStreamingAvg ON TABLE mainTableStreamingOne
         | USING 'preaggregate' AS
         | SELECT name, avg(age)
         | FROM mainTableStreamingOne GROUP BY name
         | """.stripMargin
    )
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTable")
    sql(s"LOAD DATA LOCAL INPATH '$resourcesPath/measureinsertintotest.csv' into table mainTableStreamingOne")
  }

  test("Test Pre Agg Streaming with projection column and group by") {
    val df = sql("select name from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming with projection column wiht alias and group by") {
    val df = sql("select name as newname from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table with projection, aggregation and group by") {
    val df = sql("select name, sum(age) from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table with projection with alias, aggregation with alias and group by") {
    val df = sql("select name as newName, sum(age) as sum_age from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table with UDF and aggregation") {
    val df = sql("select substring(name,1,1), sum(age) from maintable group by substring(name,1,1)")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table with UDF in group by") {
    val df = sql("select sum(age) from maintable group by substring(name,1,1)")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With Sum Aggregation, group by and Order by") {
    val df = sql("select name, sum(age) from maintable group by name order by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With projection Avg Aggregation, group by and order by") {
    val df = sql("select name, avg(age) from maintable group by name order by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With projection, Expression Aggregation, group by and order y") {
    val df = sql("select name, sum(CASE WHEN age=35 THEN id ELSE 0 END) from maintable group by name order by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With only aggregate expression and group by") {
    val df = sql("select sum(age) from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With only count aggregate expression and group by") {
    val df = sql("select count(age) from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With only max aggregate expression and group by") {
    val df = sql("select max(age) from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With only min aggregate expression and group by") {
    val df = sql("select min(age) from maintable group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  test("Test Pre Agg Streaming table With small int and avg") {
    val df = sql("select name, avg(age) from mainTableStreamingOne group by name")
    df.collect()
    assert(validateStreamingTablePlan(df.queryExecution.optimizedPlan))
  }

  /**
   * Below method will be used validate whether plan is already updated in case of streaming table
   * In case of streaming table it will add UnionNode to get the data from fact and aggregate both
   * as aggregate table will be updated after each handoff.
   * So if plan is already updated no need to transform the plan again
   * @param logicalPlan
   * query plan
   * @return whether need to update the query plan or not
   */
  def validateStreamingTablePlan(logicalPlan: LogicalPlan) : Boolean = {
    var isChildTableExists: Boolean = false
    logicalPlan.transform {
      case union @ Union(Seq(plan1, plan2)) =>
        plan2.collect{
          case logicalRelation: LogicalRelation if
          logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .isChildDataMap =>
            isChildTableExists = true
        }
        union
    }
    isChildTableExists
  }

  override def afterAll: Unit = {
    sql("drop table if exists mainTable")
    sql("drop table if exists mainTableStreamingOne")
  }
}
