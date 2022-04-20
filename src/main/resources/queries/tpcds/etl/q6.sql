CREATE TABLE q6
AS
SELECT
 ca_address_id,
 ca_street_number,
 ca_street_name,
 ca_street_type,
 ca_suite_number,
 ca_city,
 ca_county,
 ca_state,
 ca_zip,
 ca_country,
 c_customer_id,
 c_email_address,
 c_login,
 ss_quantity,
 ss_wholesale_cost,
 ss_list_price,
 ss_sales_price,
 ss_ext_discount_amt,
 ss_ext_sales_price,
 ss_ext_wholesale_cost,
 ss_ext_list_price,
 ss_ext_tax,
 ss_coupon_amt,
 ss_net_paid,
 ss_net_paid_inc_tax,
 ss_net_profit,
 d_date,
 d_fy_year,
 d_holiday,
 d_weekend,
 d_following_holiday,
 i_current_price,
 i_wholesale_cost,
 i_class,
 i_category,
 i_manufact,
 i_size,
 i_color
FROM
  customer_address a
, customer c
, store_sales s
, date_dim d
, item i
WHERE ("a"."ca_address_sk" = "c"."c_current_addr_sk")
   AND ("c"."c_customer_sk" = "s"."ss_customer_sk")
   AND ("s"."ss_sold_date_sk" = "d"."d_date_sk")
   AND ("s"."ss_item_sk" = "i"."i_item_sk")
   AND ("d"."d_month_seq" NOT IN (
      SELECT DISTINCT "d_month_seq"
      FROM
        date_dim
      WHERE "d_year" = 2001
   ))
   AND ("i"."i_current_price" < (DECIMAL '0.25' * (
      SELECT "avg"("j"."i_current_price")
      FROM
        item j
      WHERE ("j"."i_category" = "i"."i_category")
   )))
