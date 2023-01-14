CREATE TABLE q73
--WITH--
AS
SELECT
  "c_last_name"
, "c_first_name"
, "c_salutation"
, "c_preferred_cust_flag"
, "ss_ticket_number"
, "cnt"
FROM
  (
   SELECT
     "ss_ticket_number"
   , "ss_customer_sk"
   , "hd_buy_potential"
   , "hd_vehicle_count"
   , "d_dom"
   , "s_county"
   , "count"(*) "cnt"
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE ("store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("store_sales"."ss_store_sk" = "store"."s_store_sk")
      AND ("store_sales"."ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
   GROUP BY "ss_ticket_number", "ss_customer_sk", "hd_buy_potential", "hd_vehicle_count", "d_dom", "s_county"
)  dj
, customer
WHERE ("ss_customer_sk" = "c_customer_sk")
