CREATE TABLE q15
--WITH--
AS
SELECT
  cs_sold_date_sk,
  cs_sold_time_sk,
  cs_ship_date_sk,
  cs_bill_customer_sk,
  cs_bill_cdemo_sk,
  cs_bill_hdemo_sk,
  cs_bill_addr_sk,
  cs_ship_customer_sk,
  cs_ship_cdemo_sk,
  cs_ship_hdemo_sk,
  cs_ship_addr_sk,
  cs_call_center_sk,
  cs_catalog_page_sk,
  cs_ship_mode_sk,
  cs_warehouse_sk,
  cs_item_sk,
  cs_promo_sk,
  cs_order_number,
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
  cs_ext_ship_cost,
  cs_net_paid,
  cs_net_paid_inc_tax,
  cs_net_paid_inc_ship,
  cs_net_paid_inc_ship_tax,
  cs_net_profit,
  c_customer_sk,
  c_customer_id,
  c_current_cdemo_sk,
  c_current_hdemo_sk,
  c_current_addr_sk,
  c_first_shipto_date_sk,
  c_first_sales_date_sk,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk,
  ca_address_sk,
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
  ca_gmt_offset,
  ca_location_type,
  d_date_sk,
  d_date_id,
  d_date,
  d_month_seq,
  d_week_seq,
  d_quarter_seq,
  d_year,
  d_dow,
  d_moy,
  d_dom,
  d_qoy,
  d_fy_year,
  d_fy_quarter_seq,
  d_fy_week_seq,
  d_day_name,
  d_quarter_name,
  d_holiday,
  d_weekend,
  d_following_holiday,
  d_first_dom,
  d_last_dom,
  d_same_day_ly,
  d_same_day_lq,
  d_current_day,
  d_current_week,
  d_current_month,
  d_current_quarter,
  d_current_year
FROM
  catalog_sales
, customer
, customer_address
, date_dim
WHERE ("cs_bill_customer_sk" = "c_customer_sk")
   AND ("c_current_addr_sk" = "ca_address_sk")
   AND (("substr"("ca_zip", 1, 5) NOT IN ('85669'   , '86197'   , '88274'   , '83405'   , '86475'   , '85392'   , '85460'   , '80348'   , '81792'))
      OR ("ca_state" NOT IN ('CA'   , 'WA'   , 'GA'))
      OR ("cs_sales_price" <> 500))
   AND ("cs_sold_date_sk" = "d_date_sk")
   AND ("d_qoy" <> 2)
   AND ("d_year" <> 2001)
