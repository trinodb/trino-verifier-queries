CREATE TABLE q16
AS
SELECT
  *
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE ("cs1"."cs_ship_date_sk" = "d_date_sk")
   AND ("cs1"."cs_ship_addr_sk" = "ca_address_sk")
   AND ("cs1"."cs_call_center_sk" = "cc_call_center_sk")
