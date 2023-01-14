CREATE TABLE q68
--WITH--
AS
SELECT
  *
FROM
  (
   SELECT
     "ss_ticket_number"
   , "ss_customer_sk"
   , "ca_city" "bought_city"
   , "d_dom"
   , "hd_dep_count"
   , "hd_vehicle_count"
   , "d_year"
   , "s_city"
   , "sum"("ss_ext_sales_price") "extended_price"
   , "sum"("ss_ext_list_price") "list_price"
   , "sum"("ss_ext_tax") "extended_tax"
   FROM
     store_sales
   , date_dim
   , store
   , household_demographics
   , customer_address
   WHERE ("store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("store_sales"."ss_store_sk" = "store"."s_store_sk")
      AND ("store_sales"."ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
      AND ("store_sales"."ss_addr_sk" = "customer_address"."ca_address_sk")
   GROUP BY "ss_ticket_number", "ss_customer_sk", "ss_addr_sk", "ca_city", "d_dom", "hd_dep_count", "hd_vehicle_count", "d_year", "s_city"
)  dn
, customer
, customer_address current_addr
WHERE ("ss_customer_sk" = "c_customer_sk")
   AND ("customer"."c_current_addr_sk" = "current_addr"."ca_address_sk")
   AND ("current_addr"."ca_city" <> "bought_city")
