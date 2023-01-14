CREATE TABLE q99
--WITH--
AS
SELECT
  "w_warehouse_name"
, "sm_type"
, "cc_name"
, "d_month_seq"
, "sum"((CASE WHEN (("cs_ship_date_sk" - "cs_sold_date_sk") <= 30) THEN 1 ELSE 0 END)) "30 days"
, "sum"((CASE WHEN (("cs_ship_date_sk" - "cs_sold_date_sk") > 30)
   AND (("cs_ship_date_sk" - "cs_sold_date_sk") <= 60) THEN 1 ELSE 0 END)) "31-60 days"
, "sum"((CASE WHEN (("cs_ship_date_sk" - "cs_sold_date_sk") > 60)
   AND (("cs_ship_date_sk" - "cs_sold_date_sk") <= 90) THEN 1 ELSE 0 END)) "61-90 days"
, "sum"((CASE WHEN (("cs_ship_date_sk" - "cs_sold_date_sk") > 90)
   AND (("cs_ship_date_sk" - "cs_sold_date_sk") <= 120) THEN 1 ELSE 0 END)) "91-120 days"
, "sum"((CASE WHEN (("cs_ship_date_sk" - "cs_sold_date_sk") > 120) THEN 1 ELSE 0 END)) ">120 days"
FROM
  catalog_sales
, warehouse
, ship_mode
, call_center
, date_dim
WHERE ("cs_ship_date_sk" = "d_date_sk")
   AND ("cs_warehouse_sk" = "w_warehouse_sk")
   AND ("cs_ship_mode_sk" = "sm_ship_mode_sk")
   AND ("cs_call_center_sk" = "cc_call_center_sk")
GROUP BY "w_warehouse_name", "sm_type", "cc_name", "d_month_seq"
