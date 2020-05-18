create database if not exists ${DB};
use ${DB};

drop table if exists store_sales;
create table store_sales
(
  ss_sold_time_sk int,
  ss_item_sk int,
  ss_customer_sk int,
  ss_cdemo_sk int,
  ss_hdemo_sk int,
  ss_addr_sk int,
  ss_store_sk int,
  ss_promo_sk int,
  ss_ticket_number int,
  ss_quantity int,
  ss_wholesale_cost decimal(7,2),
  ss_list_price decimal(7,2),
  ss_sales_price decimal(7,2),
  ss_ext_discount_amt decimal(7,2),
  ss_ext_sales_price decimal(7,2),
  ss_ext_wholesale_cost decimal(7,2),
  ss_ext_list_price decimal(7,2),
  ss_ext_tax decimal(7,2),
  ss_coupon_amt decimal(7,2),
  ss_net_paid decimal(7,2),
  ss_net_paid_inc_tax decimal(7,2),
  ss_net_profit decimal(7,2)
)
partitioned by (ss_sold_date_sk int)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table store_sales select ss_sold_time_sk,ss_item_sk,ss_customer_sk,ss_cdemo_sk,ss_hdemo_sk,ss_addr_sk,ss_store_sk,ss_promo_sk,ss_ticket_number,ss_quantity,ss_wholesale_cost,ss_list_price,ss_sales_price,ss_ext_discount_amt,ss_ext_sales_price,ss_ext_wholesale_cost,ss_ext_list_price,ss_ext_tax,ss_coupon_amt,ss_net_paid,ss_net_paid_inc_tax,ss_net_profit,ss_sold_date_sk from ${SOURCE}.store_sales distribute by ss_sold_date_sk;

drop table if exists store_returns;
create table IF NOT EXISTS store_returns
(
    sr_return_time_sk         bigint,
    sr_item_sk                bigint,
    sr_customer_sk            bigint,
    sr_cdemo_sk               bigint,
    sr_hdemo_sk               bigint,
    sr_addr_sk                bigint,
    sr_store_sk               bigint,
    sr_reason_sk              bigint,
    sr_ticket_number          bigint,
    sr_return_quantity        bigint,
    sr_return_amt             decimal(7,2),
    sr_return_tax             decimal(7,2),
    sr_return_amt_inc_tax     decimal(7,2),
    sr_fee                    decimal(7,2),
    sr_return_ship_cost       decimal(7,2),
    sr_refunded_cash          decimal(7,2),
    sr_reversed_charge        decimal(7,2),
    sr_store_credit           decimal(7,2),
    sr_net_loss               decimal(7,2)
)
partitioned by (sr_returned_date_sk bigint)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table store_returns select sr_return_time_sk,sr_item_sk,sr_customer_sk,sr_cdemo_sk,sr_hdemo_sk,sr_addr_sk,sr_store_sk,sr_reason_sk,sr_ticket_number,sr_return_quantity,sr_return_amt,sr_return_tax,sr_return_amt_inc_tax,sr_fee,sr_return_ship_cost,sr_refunded_cash,sr_reversed_charge,sr_store_credit,sr_net_loss,sr_returned_date_sk from ${SOURCE}.store_returns distribute by sr_returned_date_sk;

drop table if exists catalog_sales;
create table IF NOT EXISTS catalog_sales
(
    cs_sold_time_sk           int,
    cs_ship_date_sk           int,
    cs_bill_customer_sk       int,
    cs_bill_cdemo_sk          int,
    cs_bill_hdemo_sk          int,
    cs_bill_addr_sk           int,
    cs_ship_customer_sk       int,
    cs_ship_cdemo_sk          int,
    cs_ship_hdemo_sk          int,
    cs_ship_addr_sk           int,
    cs_call_center_sk         int,
    cs_catalog_page_sk        int,
    cs_ship_mode_sk           int,
    cs_warehouse_sk           int,
    cs_item_sk                int,
    cs_promo_sk               int,
    cs_order_number           int,
    cs_quantity               int,
    cs_wholesale_cost         decimal(7,2),
    cs_list_price             decimal(7,2),
    cs_sales_price            decimal(7,2),
    cs_ext_discount_amt       decimal(7,2),
    cs_ext_sales_price        decimal(7,2),
    cs_ext_wholesale_cost     decimal(7,2),
    cs_ext_list_price         decimal(7,2),
    cs_ext_tax                decimal(7,2),
    cs_coupon_amt             decimal(7,2),
    cs_ext_ship_cost          decimal(7,2),
    cs_net_paid               decimal(7,2),
    cs_net_paid_inc_tax       decimal(7,2),
    cs_net_paid_inc_ship      decimal(7,2),
    cs_net_paid_inc_ship_tax  decimal(7,2),
    cs_net_profit             decimal(7,2)
)
partitioned by (cs_sold_date_sk int)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table catalog_sales select cs_sold_time_sk,cs_ship_date_sk,cs_bill_customer_sk,cs_bill_cdemo_sk,cs_bill_hdemo_sk,cs_bill_addr_sk,cs_ship_customer_sk,cs_ship_cdemo_sk,cs_ship_hdemo_sk,cs_ship_addr_sk,cs_call_center_sk,cs_catalog_page_sk,cs_ship_mode_sk,cs_warehouse_sk,cs_item_sk,cs_promo_sk,cs_order_number,cs_quantity,cs_wholesale_cost,cs_list_price,cs_sales_price,cs_ext_discount_amt,cs_ext_sales_price,cs_ext_wholesale_cost,cs_ext_list_price,cs_ext_tax,cs_coupon_amt,cs_ext_ship_cost,cs_net_paid,cs_net_paid_inc_tax,cs_net_paid_inc_ship,cs_net_paid_inc_ship_tax,cs_net_profit,cs_sold_date_sk from ${SOURCE}.catalog_sales distribute by cs_sold_date_sk;


drop table if exists catalog_returns;
create table IF NOT EXISTS catalog_returns
(
    cr_returned_time_sk       int,
    cr_item_sk                int,
    cr_refunded_customer_sk   int,
    cr_refunded_cdemo_sk      int,
    cr_refunded_hdemo_sk      int,
    cr_refunded_addr_sk       int,
    cr_returning_customer_sk  int,
    cr_returning_cdemo_sk     int,
    cr_returning_hdemo_sk     int,
    cr_returning_addr_sk      int,
    cr_call_center_sk         int,
    cr_catalog_page_sk        int,
    cr_ship_mode_sk           int,
    cr_warehouse_sk           int,
    cr_reason_sk              int,
    cr_order_number           int,
    cr_return_quantity        int,
    cr_return_amount          decimal(7,2),
    cr_return_tax             decimal(7,2),
    cr_return_amt_inc_tax     decimal(7,2),
    cr_fee                    decimal(7,2),
    cr_return_ship_cost       decimal(7,2),
    cr_refunded_cash          decimal(7,2),
    cr_reversed_charge        decimal(7,2),
    cr_store_credit           decimal(7,2),
    cr_net_loss               decimal(7,2)
)
partitioned by (cr_returned_date_sk int)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table catalog_returns select cr_returned_time_sk,cr_item_sk,cr_refunded_customer_sk,cr_refunded_cdemo_sk,cr_refunded_hdemo_sk,cr_refunded_addr_sk,cr_returning_customer_sk,cr_returning_cdemo_sk,cr_returning_hdemo_sk,cr_returning_addr_sk,cr_call_center_sk,cr_catalog_page_sk,cr_ship_mode_sk,cr_warehouse_sk,cr_reason_sk,cr_order_number,cr_return_quantity,cr_return_amount,cr_return_tax,cr_return_amt_inc_tax,cr_fee,cr_return_ship_cost,cr_refunded_cash,cr_reversed_charge,cr_store_credit,cr_net_loss,cr_returned_date_sk from ${SOURCE}.catalog_returns distribute by cr_returned_date_sk;



drop table if exists web_sales;
create table IF NOT EXISTS web_sales
(
    ws_sold_time_sk           bigint,
    ws_ship_date_sk           bigint,
    ws_item_sk                bigint,
    ws_bill_customer_sk       bigint,
    ws_bill_cdemo_sk          bigint,
    ws_bill_hdemo_sk          bigint,
    ws_bill_addr_sk           bigint,
    ws_ship_customer_sk       bigint,
    ws_ship_cdemo_sk          bigint,
    ws_ship_hdemo_sk          bigint,
    ws_ship_addr_sk           bigint,
    ws_web_page_sk            bigint,
    ws_web_site_sk            bigint,
    ws_ship_mode_sk           bigint,
    ws_warehouse_sk           bigint,
    ws_promo_sk               bigint,
    ws_order_number           bigint,
    ws_quantity               bigint,
    ws_wholesale_cost         decimal(7,2),
    ws_list_price             decimal(7,2),
    ws_sales_price            decimal(7,2),
    ws_ext_discount_amt       decimal(7,2),
    ws_ext_sales_price        decimal(7,2),
    ws_ext_wholesale_cost     decimal(7,2),
    ws_ext_list_price         decimal(7,2),
    ws_ext_tax                decimal(7,2),
    ws_coupon_amt             decimal(7,2),
    ws_ext_ship_cost          decimal(7,2),
    ws_net_paid               decimal(7,2),
    ws_net_paid_inc_tax       decimal(7,2),
    ws_net_paid_inc_ship      decimal(7,2),
    ws_net_paid_inc_ship_tax  decimal(7,2),
    ws_net_profit             decimal(7,2)
)
partitioned by (ws_sold_date_sk bigint)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table web_sales select ws_sold_time_sk,ws_ship_date_sk,ws_item_sk,ws_bill_customer_sk,ws_bill_cdemo_sk,ws_bill_hdemo_sk,ws_bill_addr_sk,ws_ship_customer_sk,ws_ship_cdemo_sk,ws_ship_hdemo_sk,ws_ship_addr_sk,ws_web_page_sk,ws_web_site_sk,ws_ship_mode_sk,ws_warehouse_sk,ws_promo_sk,ws_order_number,ws_quantity,ws_wholesale_cost,ws_list_price,ws_sales_price,ws_ext_discount_amt,ws_ext_sales_price,ws_ext_wholesale_cost,ws_ext_list_price,ws_ext_tax,ws_coupon_amt,ws_ext_ship_cost,ws_net_paid,ws_net_paid_inc_tax,ws_net_paid_inc_ship,ws_net_paid_inc_ship_tax,ws_net_profit,ws_sold_date_sk from ${SOURCE}.web_sales distribute by ws_sold_date_sk;


drop table if exists web_returns;
create table IF NOT EXISTS web_returns
(
    wr_returned_time_sk       bigint,
    wr_item_sk                bigint,
    wr_refunded_customer_sk   bigint,
    wr_refunded_cdemo_sk      bigint,
    wr_refunded_hdemo_sk      bigint,
    wr_refunded_addr_sk       bigint,
    wr_returning_customer_sk  bigint,
    wr_returning_cdemo_sk     bigint,
    wr_returning_hdemo_sk     bigint,
    wr_returning_addr_sk      bigint,
    wr_web_page_sk            bigint,
    wr_reason_sk              bigint,
    wr_order_number           bigint,
    wr_return_quantity        bigint,
    wr_return_amt             decimal(7,2),
    wr_return_tax             decimal(7,2),
    wr_return_amt_inc_tax     decimal(7,2),
    wr_fee                    decimal(7,2),
    wr_return_ship_cost       decimal(7,2),
    wr_refunded_cash          decimal(7,2),
    wr_reversed_charge        decimal(7,2),
    wr_account_credit         decimal(7,2),
    wr_net_loss               decimal(7,2)
)
partitioned by (wr_returned_date_sk bigint)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table web_returns select wr_returned_time_sk,wr_item_sk,wr_refunded_customer_sk,wr_refunded_cdemo_sk,wr_refunded_hdemo_sk,wr_refunded_addr_sk,wr_returning_customer_sk,wr_returning_cdemo_sk,wr_returning_hdemo_sk,wr_returning_addr_sk,wr_web_page_sk,wr_reason_sk,wr_order_number,wr_return_quantity,wr_return_amt,wr_return_tax,wr_return_amt_inc_tax,wr_fee,wr_return_ship_cost,wr_refunded_cash,wr_reversed_charge,wr_account_credit,wr_net_loss,wr_returned_date_sk from ${SOURCE}.web_returns distribute by wr_returned_date_sk;


drop table if exists inventory;
create table IF NOT EXISTS inventory
(
    inv_item_sk               bigint,
    inv_warehouse_sk          bigint,
    inv_quantity_on_hand      bigint
)
partitioned by (inv_date_sk bigint)
stored as ${FILE}
tblproperties(
'cache_level'='blocklet',
'carbon.column.compressor'='zstd',
'carbon.local.dictionary.enable'='false',
'table_blocksize'='1024',
'table_blocklet_size'='256');

insert overwrite table inventory select inv_item_sk,inv_warehouse_sk,inv_quantity_on_hand,inv_date_sk from ${SOURCE}.inventory distribute by inv_date_sk;

drop table if exists customer_address;
create table customer_address
stored as ${FILE}
tblproperties('sort_columns'='ca_address_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.customer_address;

drop table if exists customer_demographics;
create table customer_demographics
stored as ${FILE}
tblproperties('sort_columns'='cd_demo_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.customer_demographics;

drop table if exists date_dim;
create table date_dim
stored as ${FILE}
tblproperties('sort_columns'='d_date_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.date_dim;

drop table if exists warehouse;
create table warehouse
stored as ${FILE}
tblproperties('sort_columns'='w_warehouse_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.warehouse;

drop table if exists ship_mode;
create table ship_mode
stored as ${FILE}
tblproperties('sort_columns'='sm_ship_mode_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.ship_mode;

drop table if exists time_dim;
create table time_dim
stored as ${FILE}
tblproperties('sort_columns'='t_time_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.time_dim;

drop table if exists reason;
create table reason
stored as ${FILE}
tblproperties('sort_columns'='r_reason_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.reason;

drop table if exists income_band;
create table income_band
stored as ${FILE}
tblproperties('sort_columns'='ib_income_band_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.income_band;

drop table if exists item;
create table item
stored as ${FILE}
tblproperties('sort_columns'='i_item_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.item;

drop table if exists store;
create table store
stored as ${FILE}
tblproperties('sort_columns'='s_store_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.store;

drop table if exists call_center;
create table call_center
stored as ${FILE}
tblproperties('sort_columns'='cc_call_center_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.call_center;

drop table if exists customer;
create table customer
stored as ${FILE}
tblproperties('sort_columns'='c_customer_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.customer;

drop table if exists web_site;
create table web_site
stored as ${FILE}
tblproperties('sort_columns'='web_site_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.web_site;

drop table if exists household_demographics;
create table household_demographics
stored as ${FILE}
tblproperties('sort_columns'='hd_demo_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.household_demographics;

drop table if exists web_page;
create table web_page
stored as ${FILE}
tblproperties('sort_columns'='wp_web_page_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.web_page;

drop table if exists promotion;
create table promotion
stored as ${FILE}
tblproperties('sort_columns'='p_promo_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.promotion;

drop table if exists catalog_page;
create table catalog_page
stored as ${FILE}
tblproperties('sort_columns'='cp_catalog_page_sk','sort_scope'='global_sort')
as select * from ${SOURCE}.catalog_page;
