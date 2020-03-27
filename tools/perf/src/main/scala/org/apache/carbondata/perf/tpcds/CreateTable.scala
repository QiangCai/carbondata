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

package org.apache.carbondata.perf.tpcds

import org.apache.spark.sql.SparkSession

/**
 * CreateTable
 */
object CreateTable {

  def loadAllData(spark: SparkSession): Unit = {
    val carbonTime = loadCarbon(spark)
    val parquetTime = loadParquet(spark)
    System.out.println(s"load taken time: carbon vs parquet = ${carbonTime.toFloat / parquetTime} : 1 ")
  }

  def loadParquet(spark: SparkSession): Long = {
    System.out.println("start to load parquet...")
    spark.sql("drop database if exists parquet cascade")
    spark.sql("create database parquet")
    spark.sql("use parquet")
    val t1 = System.currentTimeMillis()
    spark.sql("drop table if exists web_sales")
    spark.sql(
      """
        | create table if not exists web_sales
        |(
        |    ws_sold_date_sk           int,
        |    ws_sold_time_sk           int,
        |    ws_ship_date_sk           int,
        |    ws_item_sk                int,
        |    ws_bill_customer_sk       int,
        |    ws_bill_cdemo_sk          int,
        |    ws_bill_hdemo_sk          int,
        |    ws_bill_addr_sk           int,
        |    ws_ship_customer_sk       int,
        |    ws_ship_cdemo_sk          int,
        |    ws_ship_hdemo_sk          int,
        |    ws_ship_addr_sk           int,
        |    ws_web_page_sk            int,
        |    ws_web_site_sk            int,
        |    ws_ship_mode_sk           int,
        |    ws_warehouse_sk           int,
        |    ws_promo_sk               int,
        |    ws_order_number           int,
        |    ws_quantity               int,
        |    ws_wholesale_cost         decimal(7,2),
        |    ws_list_price             decimal(7,2),
        |    ws_sales_price            decimal(7,2),
        |    ws_ext_discount_amt       decimal(7,2),
        |    ws_ext_sales_price        decimal(7,2),
        |    ws_ext_wholesale_cost     decimal(7,2),
        |    ws_ext_list_price         decimal(7,2),
        |    ws_ext_tax                decimal(7,2),
        |    ws_coupon_amt             decimal(7,2),
        |    ws_ext_ship_cost          decimal(7,2),
        |    ws_net_paid               decimal(7,2),
        |    ws_net_paid_inc_tax       decimal(7,2),
        |    ws_net_paid_inc_ship      decimal(7,2),
        |    ws_net_paid_inc_ship_tax  decimal(7,2),
        |    ws_net_profit             decimal(7,2)
        |) using parquet
        |""".stripMargin)
    spark.sql(
      """
        | insert into web_sales select * from carbon.web_sales
        |""".stripMargin)

    spark.sql("drop table if exists catalog_sales")
    spark.sql(
      """
        |create table if not exists catalog_sales
        |(
        |    cs_sold_date_sk           int,
        |    cs_sold_time_sk           int,
        |    cs_ship_date_sk           int,
        |    cs_bill_customer_sk       int,
        |    cs_bill_cdemo_sk          int,
        |    cs_bill_hdemo_sk          int,
        |    cs_bill_addr_sk           int,
        |    cs_ship_customer_sk       int,
        |    cs_ship_cdemo_sk          int,
        |    cs_ship_hdemo_sk          int,
        |    cs_ship_addr_sk           int,
        |    cs_call_center_sk         int,
        |    cs_catalog_page_sk        int,
        |    cs_ship_mode_sk           int,
        |    cs_warehouse_sk           int,
        |    cs_item_sk                int,
        |    cs_promo_sk               int,
        |    cs_order_number           int,
        |    cs_quantity               int,
        |    cs_wholesale_cost         decimal(7,2),
        |    cs_list_price             decimal(7,2),
        |    cs_sales_price            decimal(7,2),
        |    cs_ext_discount_amt       decimal(7,2),
        |    cs_ext_sales_price        decimal(7,2),
        |    cs_ext_wholesale_cost     decimal(7,2),
        |    cs_ext_list_price         decimal(7,2),
        |    cs_ext_tax                decimal(7,2),
        |    cs_coupon_amt             decimal(7,2),
        |    cs_ext_ship_cost          decimal(7,2),
        |    cs_net_paid               decimal(7,2),
        |    cs_net_paid_inc_tax       decimal(7,2),
        |    cs_net_paid_inc_ship      decimal(7,2),
        |    cs_net_paid_inc_ship_tax  decimal(7,2),
        |    cs_net_profit             decimal(7,2)
        |) using parquet
        |""".stripMargin)
    spark.sql(
      """
        | insert into catalog_sales select * from carbon.catalog_sales
        |""".stripMargin)

    spark.sql("drop table if exists store_sales")
    spark.sql(
      """
        | create table store_sales
        |(
        |    ss_sold_date_sk           int                       ,
        |    ss_sold_time_sk           int                       ,
        |    ss_item_sk                int               ,
        |    ss_customer_sk            int                       ,
        |    ss_cdemo_sk               int                       ,
        |    ss_hdemo_sk               int                       ,
        |    ss_addr_sk                int                       ,
        |    ss_store_sk               int                       ,
        |    ss_promo_sk               int                       ,
        |    ss_ticket_number          int               ,
        |    ss_quantity               int                       ,
        |    ss_wholesale_cost         decimal(7,2)                  ,
        |    ss_list_price             decimal(7,2)                  ,
        |    ss_sales_price            decimal(7,2)                  ,
        |    ss_ext_discount_amt       decimal(7,2)                  ,
        |    ss_ext_sales_price        decimal(7,2)                  ,
        |    ss_ext_wholesale_cost     decimal(7,2)                  ,
        |    ss_ext_list_price         decimal(7,2)                  ,
        |    ss_ext_tax                decimal(7,2)                  ,
        |    ss_coupon_amt             decimal(7,2)                  ,
        |    ss_net_paid               decimal(7,2)                  ,
        |    ss_net_paid_inc_tax       decimal(7,2)                  ,
        |    ss_net_profit             decimal(7,2)
        |) using parquet
        |""".stripMargin)
    spark.sql(
      """
        | insert into store_sales select * from carbon.store_sales
        |""".stripMargin)

    spark.sql("drop table if exists item")
    spark.sql(
      """
        |create table item
        |    (
        |      i_item_sk                 int               ,
        |      i_item_id                 string              ,
        |      i_rec_start_date          date                          ,
        |      i_rec_end_date            date                          ,
        |      i_item_desc               string                  ,
        |      i_current_price           decimal(7,2)                  ,
        |      i_wholesale_cost          decimal(7,2)                  ,
        |      i_brand_id                int                       ,
        |      i_brand                   string                      ,
        |      i_class_id                int                       ,
        |      i_class                   string                      ,
        |      i_category_id             int                       ,
        |      i_category                string                      ,
        |      i_manufact_id             int                       ,
        |      i_manufact                string                      ,
        |      i_size                    string                      ,
        |      i_formulation             string                      ,
        |      i_color                   string                      ,
        |      i_units                   string                      ,
        |      i_container               string                      ,
        |      i_manager_id              int                       ,
        |      i_product_name            string
        |    )
        | using parquet
        |""".stripMargin)
    spark.sql("insert into item select * from carbon.item")

    spark.sql("drop table if exists reason")
    spark.sql(
      """
        |create table reason
        |(
        |    r_reason_sk               int               ,
        |    r_reason_id               string              ,
        |    r_reason_desc             string
        |) using parquet
        |""".stripMargin)
    spark.sql(
      """
        | insert into reason select * from carbon.reason
        |""".stripMargin)

    spark.sql("drop table if exists date_dim")
    spark.sql(
      """
        |create table if not exists date_dim
        |(
        |    d_date_sk                 int,
        |    d_date_id                 string,
        |    d_date                    date,
        |    d_month_seq               int,
        |    d_week_seq                int,
        |    d_quarter_seq             int,
        |    d_year                    int,
        |    d_dow                     int,
        |    d_moy                     int,
        |    d_dom                     int,
        |    d_qoy                     int,
        |    d_fy_year                 int,
        |    d_fy_quarter_seq          int,
        |    d_fy_week_seq             int,
        |    d_day_name                string,
        |    d_quarter_name            string,
        |    d_holiday                 string,
        |    d_weekend                 string,
        |    d_following_holiday       string,
        |    d_first_dom               int,
        |    d_last_dom                int,
        |    d_same_day_ly             int,
        |    d_same_day_lq             int,
        |    d_current_day             string,
        |    d_current_week            string,
        |    d_current_month           string,
        |    d_current_quarter         string,
        |    d_current_year            string
        |) using parquet
        |""".stripMargin)
    spark.sql(
      """
        | insert into date_dim select * from carbon.date_dim
        |""".stripMargin)
    val t2 = System.currentTimeMillis()
    val time = t2 - t1
    System.out.println(s"finish to load parquet, taken time: ${time} ms"  )
    time
  }

  def loadCarbon(spark: SparkSession): Long = {
    System.out.println("start to load carbon...")
    spark.sql("drop database if exists carbon cascade")
    spark.sql("create database carbon")
    spark.sql("use carbon")
    val t1 = System.currentTimeMillis()
    spark.sql("drop table if exists web_sales")
    spark.sql(
      """
        | create table if not exists web_sales
        |(
        |    ws_sold_date_sk           int,
        |    ws_sold_time_sk           int,
        |    ws_ship_date_sk           int,
        |    ws_item_sk                int,
        |    ws_bill_customer_sk       int,
        |    ws_bill_cdemo_sk          int,
        |    ws_bill_hdemo_sk          int,
        |    ws_bill_addr_sk           int,
        |    ws_ship_customer_sk       int,
        |    ws_ship_cdemo_sk          int,
        |    ws_ship_hdemo_sk          int,
        |    ws_ship_addr_sk           int,
        |    ws_web_page_sk            int,
        |    ws_web_site_sk            int,
        |    ws_ship_mode_sk           int,
        |    ws_warehouse_sk           int,
        |    ws_promo_sk               int,
        |    ws_order_number           int,
        |    ws_quantity               int,
        |    ws_wholesale_cost         decimal(7,2),
        |    ws_list_price             decimal(7,2),
        |    ws_sales_price            decimal(7,2),
        |    ws_ext_discount_amt       decimal(7,2),
        |    ws_ext_sales_price        decimal(7,2),
        |    ws_ext_wholesale_cost     decimal(7,2),
        |    ws_ext_list_price         decimal(7,2),
        |    ws_ext_tax                decimal(7,2),
        |    ws_coupon_amt             decimal(7,2),
        |    ws_ext_ship_cost          decimal(7,2),
        |    ws_net_paid               decimal(7,2),
        |    ws_net_paid_inc_tax       decimal(7,2),
        |    ws_net_paid_inc_ship      decimal(7,2),
        |    ws_net_paid_inc_ship_tax  decimal(7,2),
        |    ws_net_profit             decimal(7,2)
        |) using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/web_sales.dat' into table web_sales options('delimiter'='|', 'header'='false')
        |""".stripMargin)

    spark.sql("drop table if exists catalog_sales")
    spark.sql(
      """
        |create table if not exists catalog_sales
        |(
        |    cs_sold_date_sk           int,
        |    cs_sold_time_sk           int,
        |    cs_ship_date_sk           int,
        |    cs_bill_customer_sk       int,
        |    cs_bill_cdemo_sk          int,
        |    cs_bill_hdemo_sk          int,
        |    cs_bill_addr_sk           int,
        |    cs_ship_customer_sk       int,
        |    cs_ship_cdemo_sk          int,
        |    cs_ship_hdemo_sk          int,
        |    cs_ship_addr_sk           int,
        |    cs_call_center_sk         int,
        |    cs_catalog_page_sk        int,
        |    cs_ship_mode_sk           int,
        |    cs_warehouse_sk           int,
        |    cs_item_sk                int,
        |    cs_promo_sk               int,
        |    cs_order_number           int,
        |    cs_quantity               int,
        |    cs_wholesale_cost         decimal(7,2),
        |    cs_list_price             decimal(7,2),
        |    cs_sales_price            decimal(7,2),
        |    cs_ext_discount_amt       decimal(7,2),
        |    cs_ext_sales_price        decimal(7,2),
        |    cs_ext_wholesale_cost     decimal(7,2),
        |    cs_ext_list_price         decimal(7,2),
        |    cs_ext_tax                decimal(7,2),
        |    cs_coupon_amt             decimal(7,2),
        |    cs_ext_ship_cost          decimal(7,2),
        |    cs_net_paid               decimal(7,2),
        |    cs_net_paid_inc_tax       decimal(7,2),
        |    cs_net_paid_inc_ship      decimal(7,2),
        |    cs_net_paid_inc_ship_tax  decimal(7,2),
        |    cs_net_profit             decimal(7,2)
        |) using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/catalog_sales.dat' into table catalog_sales options('delimiter'='|', 'header'='false')
        |""".stripMargin)

    spark.sql("drop table if exists store_sales")
    spark.sql(
      """
        | create table store_sales
        |(
        |    ss_sold_date_sk           int                       ,
        |    ss_sold_time_sk           int                       ,
        |    ss_item_sk                int               ,
        |    ss_customer_sk            int                       ,
        |    ss_cdemo_sk               int                       ,
        |    ss_hdemo_sk               int                       ,
        |    ss_addr_sk                int                       ,
        |    ss_store_sk               int                       ,
        |    ss_promo_sk               int                       ,
        |    ss_ticket_number          int               ,
        |    ss_quantity               int                       ,
        |    ss_wholesale_cost         decimal(7,2)                  ,
        |    ss_list_price             decimal(7,2)                  ,
        |    ss_sales_price            decimal(7,2)                  ,
        |    ss_ext_discount_amt       decimal(7,2)                  ,
        |    ss_ext_sales_price        decimal(7,2)                  ,
        |    ss_ext_wholesale_cost     decimal(7,2)                  ,
        |    ss_ext_list_price         decimal(7,2)                  ,
        |    ss_ext_tax                decimal(7,2)                  ,
        |    ss_coupon_amt             decimal(7,2)                  ,
        |    ss_net_paid               decimal(7,2)                  ,
        |    ss_net_paid_inc_tax       decimal(7,2)                  ,
        |    ss_net_profit             decimal(7,2)
        |) using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/store_sales.dat' into table store_sales options('delimiter'='|', 'header'='false','DATEFORMAT'='yyyy-MM-dd')
        |""".stripMargin)

    spark.sql("drop table if exists item")
    spark.sql(
      """
        |create table item
        |    (
        |      i_item_sk                 int               ,
        |      i_item_id                 string              ,
        |      i_rec_start_date          date                          ,
        |      i_rec_end_date            date                          ,
        |      i_item_desc               string                  ,
        |      i_current_price           decimal(7,2)                  ,
        |      i_wholesale_cost          decimal(7,2)                  ,
        |      i_brand_id                int                       ,
        |      i_brand                   string                      ,
        |      i_class_id                int                       ,
        |      i_class                   string                      ,
        |      i_category_id             int                       ,
        |      i_category                string                      ,
        |      i_manufact_id             int                       ,
        |      i_manufact                string                      ,
        |      i_size                    string                      ,
        |      i_formulation             string                      ,
        |      i_color                   string                      ,
        |      i_units                   string                      ,
        |      i_container               string                      ,
        |      i_manager_id              int                       ,
        |      i_product_name            string
        |    )
        | using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/item.dat' into table item options('delimiter'='|', 'header'='false','DATEFORMAT'='yyyy-MM-dd')
        |""".stripMargin)

    spark.sql("drop table if exists reason")
    spark.sql(
      """
        |create table reason
        |(
        |    r_reason_sk               int               ,
        |    r_reason_id               string              ,
        |    r_reason_desc             string
        |) using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/reason.dat' into table reason options('delimiter'='|', 'header'='false','DATEFORMAT'='yyyy-MM-dd')
        |""".stripMargin)

    spark.sql("drop table if exists date_dim")
    spark.sql(
      """
        |create table if not exists date_dim
        |(
        |    d_date_sk                 int,
        |    d_date_id                 string,
        |    d_date                    date,
        |    d_month_seq               int,
        |    d_week_seq                int,
        |    d_quarter_seq             int,
        |    d_year                    int,
        |    d_dow                     int,
        |    d_moy                     int,
        |    d_dom                     int,
        |    d_qoy                     int,
        |    d_fy_year                 int,
        |    d_fy_quarter_seq          int,
        |    d_fy_week_seq             int,
        |    d_day_name                string,
        |    d_quarter_name            string,
        |    d_holiday                 string,
        |    d_weekend                 string,
        |    d_following_holiday       string,
        |    d_first_dom               int,
        |    d_last_dom                int,
        |    d_same_day_ly             int,
        |    d_same_day_lq             int,
        |    d_current_day             string,
        |    d_current_week            string,
        |    d_current_month           string,
        |    d_current_quarter         string,
        |    d_current_year            string
        |) using carbondata
        |""".stripMargin)
    spark.sql(
      """
        |load data inpath '/opt/bigdata/data/tpcds/date_dim.dat' into table date_dim options('delimiter'='|', 'header'='false','DATEFORMAT'='yyyy-MM-dd')
        |""".stripMargin)
    val t2 = System.currentTimeMillis()
    val time = t2 - t1
    System.out.println(s"finish to load carbon, taken ${time} ms")
    time
  }
}
