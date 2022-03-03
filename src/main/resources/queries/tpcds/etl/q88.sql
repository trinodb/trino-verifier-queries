CREATE TABLE q88
AS
SELECT
    "s1"."hd_dep_count"
     ,"s1"."hd_vehicle_count"
     ,"s1"."s_store_name"
     , "h8_30_to_9"
     , "h9_to_9_30"
     , "h9_30_to_10"
     , "h10_to_10_30"
     , "h10_30_to_11"
     , "h11_to_11_30"
     , "h12_to_12_30"
FROM
  (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h8_30_to_9"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 8)
      AND ("time_dim"."t_minute" >= 30)
    GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s1
, (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h9_to_9_30"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 9)
      AND ("time_dim"."t_minute" < 30)
   GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s2
, (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h9_30_to_10"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 9)
      AND ("time_dim"."t_minute" >= 30)
   GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s3
, (
   SELECT
      "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h10_to_10_30"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 10)
      AND ("time_dim"."t_minute" < 30)
   GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s4
, (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h10_30_to_11"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 10)
      AND ("time_dim"."t_minute" >= 30)
    GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s5
, (
   SELECT
        "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
    ,"count"(*) "h11_to_11_30"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 11)
      AND ("time_dim"."t_minute" < 30)
    GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s6
, (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h11_30_to_12"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 11)
      AND ("time_dim"."t_minute" >= 30)
    GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s7
, (
   SELECT
     "hd_dep_count"
     ,"hd_vehicle_count"
     ,"s_store_name"
     ,"count"(*) "h12_to_12_30"
   FROM
     store_sales
   , household_demographics
   , time_dim
   , store
   WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
      AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("time_dim"."t_hour" = 12)
      AND ("time_dim"."t_minute" < 30)
    GROUP BY "hd_dep_count", "hd_vehicle_count", "s_store_name"
)  s8
WHERE
 (s1.hd_dep_count = s2.hd_dep_count AND s1.hd_vehicle_count = s2.hd_vehicle_count AND s1.s_store_name = s2.s_store_name)
AND  (s1.hd_dep_count = s3.hd_dep_count AND s1.hd_vehicle_count = s3.hd_vehicle_count AND s1.s_store_name = s3.s_store_name)
AND  (s1.hd_dep_count = s4.hd_dep_count AND s1.hd_vehicle_count = s4.hd_vehicle_count AND s1.s_store_name = s4.s_store_name)
AND  (s1.hd_dep_count = s5.hd_dep_count AND s1.hd_vehicle_count = s5.hd_vehicle_count AND s1.s_store_name = s5.s_store_name)
AND  (s1.hd_dep_count = s6.hd_dep_count AND s1.hd_vehicle_count = s6.hd_vehicle_count AND s1.s_store_name = s6.s_store_name)
AND  (s1.hd_dep_count = s7.hd_dep_count AND s1.hd_vehicle_count = s7.hd_vehicle_count AND s1.s_store_name = s7.s_store_name)
