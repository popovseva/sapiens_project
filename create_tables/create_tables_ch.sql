/* Loading mart from Greenplum to Clickhouse. 
Fill some fields related to Greenplum connection: 
your_greenplum_host:port, your_database, your_username, your_password
*/
CREATE database std6_116;
CREATE TABLE std6_116.ch_mart_proj_ext
(
    `plant` String,
    `txt` String,
    `date` Date,
    `month` String,
    `daily_turnover` Float64,
    `daily_coupon_discounts` Float64,
    `daily_turnover_with_discounts` Float64,
    `daily_qty` Int32,
    `daily_bills` Int32,
    `daily_traffic` Int32,
    `daily_items_on_discount` Int32,
    `daily_share_discount_items` Float64,
    `daily_avg_sold_qty` Float64,
    `daily_conversion_coeff` Float64,
    `daily_avg_bill` Float64,
    `daily_avg_turnover_per_client` Float64
)
ENGINE = PostgreSQL('your_greenplum_host:port',
 'your_database',
 'mart_20210101_20210228',
 'your_username',
 'your_password,
 'std6_116');


-- replicated table
CREATE TABLE std6_116.ch_mart_proj_repl
(
    `plant` String,
    `txt` String,
    `date` Date,
    `month` String,
    `daily_turnover` Float64,
    `daily_coupon_discounts` Float64,
    `daily_turnover_with_discounts` Float64,
    `daily_qty` Int32,
    `daily_bills` Int32,
    `daily_traffic` Int32,
    `daily_items_on_discount` Int32,
    `daily_share_discount_items` Float64,
    `daily_avg_sold_qty` Float64,
    `daily_conversion_coeff` Float64,
    `daily_avg_bill` Float64,
    `daily_avg_turnover_per_client` Float64
)
ENGINE = ReplicatedMergeTree('/click/std6_116/ch_mart_proj_repl/{shard}',
 '{replica}')
ORDER BY plant
SETTINGS index_granularity = 8192;


-- distributed table
CREATE TABLE std6_116.ch_mart_proj_distr
(
    `plant` String,
    `txt` String,
    `date` Date,
    `month` String,
    `daily_turnover` Float64,
    `daily_coupon_discounts` Float64,
    `daily_turnover_with_discounts` Float64,
    `daily_qty` Int32,
    `daily_bills` Int32,
    `daily_traffic` Int32,
    `daily_items_on_discount` Int32,
    `daily_share_discount_items` Float64,
    `daily_avg_sold_qty` Float64,
    `daily_conversion_coeff` Float64,
    `daily_avg_bill` Float64,
    `daily_avg_turnover_per_client` Float64
)
ENGINE = Distributed('default_cluster',
 'std6_116',
 'ch_mart_proj_repl',
 cityHash64(plant));