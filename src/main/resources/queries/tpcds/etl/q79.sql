CREATE TABLE q79
AS
SELECT
  "c_last_name"
, "c_first_name"
, "substr"("s_city", 1, 30) s_city
, "ss_ticket_number"
, "amt"
, "profit"
FROM
  (
   SELECT
     "ss_ticket_number"
   , "ss_customer_sk"
   , "store"."s_city"
   , "date_dim"."d_year"
   , "date_dim"."d_dow"
   , "sum"("ss_coupon_amt") "amt"
   , "sum"("ss_net_profit") "profit"
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   WHERE ("store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("store_sales"."ss_store_sk" = "store"."s_store_sk")
      AND ("store_sales"."ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
   GROUP BY "ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "store"."s_city", "date_dim"."d_year", "date_dim"."d_dow"
)  ms
, customer
WHERE ("ss_customer_sk" = "c_customer_sk")
