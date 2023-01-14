CREATE TABLE q16
--WITH--
AS
SELECT
  cs_quantity,
  cs_wholesale_cost,
  cs_list_price,
  cs_sales_price,
  cs_ext_discount_amt,
  cs_ext_sales_price,
  cs_ext_wholesale_cost,
  cs_ext_list_price,
  cs_ext_tax,
  cs_coupon_amt,
  cs_net_paid,
  cs_net_paid_inc_tax,
  cs_net_profit,
  d_date,
  d_fy_year,
  d_holiday,
  d_weekend,
  d_following_holiday,
  ca_street_number,
  ca_street_name,
  ca_city,
  ca_state,
  ca_zip,
  cc_name,
  cc_rec_start_date,
  cc_rec_end_date
FROM
  catalog_sales cs1
, date_dim
, customer_address
, call_center
WHERE ("cs1"."cs_ship_date_sk" = "d_date_sk")
   AND ("cs1"."cs_ship_addr_sk" = "ca_address_sk")
   AND ("cs1"."cs_call_center_sk" = "cc_call_center_sk")
   AND (EXISTS (
       SELECT *
       FROM
         catalog_sales cs2
       WHERE ("cs1"."cs_order_number" = "cs2"."cs_order_number")
          AND ("cs1"."cs_warehouse_sk" <> "cs2"."cs_warehouse_sk")
    ))
   AND (NOT (EXISTS (
       SELECT *
       FROM
         catalog_returns cr1
       WHERE ("cs1"."cs_order_number" = "cr1"."cr_order_number")
    )))
