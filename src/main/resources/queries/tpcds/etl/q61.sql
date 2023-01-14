CREATE TABLE q61
--WITH--
AS
SELECT
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
  s_store_name,
  s_number_employees,
  s_hours,
  p_cost,
  p_promo_name,
  p_purpose,
  p_discount_active,
  d_date,
  d_fy_year,
  d_holiday,
  d_weekend
FROM
  store_sales
, store
, promotion
, date_dim
WHERE ("ss_sold_date_sk" = "d_date_sk")
  AND ("ss_store_sk" = "s_store_sk")
  AND ("ss_promo_sk" = "p_promo_sk")
  AND (("p_channel_dmail" = 'Y')
     OR ("p_channel_email" = 'Y')
     OR ("p_channel_tv" = 'Y'))
