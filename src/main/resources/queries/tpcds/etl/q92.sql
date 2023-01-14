CREATE TABLE q92
--WITH--
AS
SELECT
  ws_sold_date_sk,
  ws_sold_time_sk,
  ws_ship_date_sk,
  ws_item_sk,
  ws_bill_customer_sk,
  ws_bill_cdemo_sk,
  ws_bill_hdemo_sk,
  ws_bill_addr_sk,
  ws_ship_customer_sk,
  ws_ship_cdemo_sk,
  ws_ship_hdemo_sk,
  ws_ship_addr_sk,
  ws_web_page_sk,
  ws_web_site_sk,
  ws_ship_mode_sk,
  ws_warehouse_sk,
  ws_promo_sk,
  ws_order_number,
  ws_quantity,
  ws_wholesale_cost,
  ws_list_price,
  ws_sales_price,
  ws_ext_discount_amt,
  ws_ext_sales_price,
  ws_ext_wholesale_cost,
  ws_ext_list_price,
  ws_ext_tax,
  ws_coupon_amt,
  ws_ext_ship_cost,
  ws_net_paid,
  ws_net_paid_inc_tax,
  ws_net_paid_inc_ship,
  ws_net_paid_inc_ship_tax,
  ws_net_profit,
  i_item_sk,
  i_item_id,
  i_rec_start_date,
  i_rec_end_date,
  i_item_desc,
  i_current_price,
  i_wholesale_cost,
  i_brand_id,
  i_brand,
  i_class_id,
  i_class,
  i_category_id,
  i_category,
  i_manufact_id,
  i_manufact,
  i_size,
  i_formulation,
  i_color,
  i_units,
  i_container,
  i_manager_id,
  i_product_name,
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
  web_sales
, item
, date_dim
WHERE ("i_item_sk" = "ws_item_sk")
   AND ("d_date_sk" = "ws_sold_date_sk")
   AND ("ws_ext_discount_amt" <> (
      SELECT (DECIMAL '1.3' * "avg"("ws_ext_discount_amt"))
      FROM
        web_sales
      , date_dim
      WHERE ("ws_item_sk" = "i_item_sk")
         AND ("d_date" BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND ("d_date_sk" = "ws_sold_date_sk")
   ))
