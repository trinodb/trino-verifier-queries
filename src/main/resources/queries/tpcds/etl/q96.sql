CREATE TABLE q96
AS
SELECT
"t_hour"
, "t_minute"
, "hd_dep_count"
, "s_store_name"
, count(*) c
FROM
  store_sales
, household_demographics
, time_dim
, store
WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
   AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
   AND ("ss_store_sk" = "s_store_sk")
GROUP BY "t_hour", "t_minute", "hd_dep_count", "s_store_name"
