CREATE TABLE q32
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
  catalog_sales
, item
, date_dim
WHERE
   ("i_item_sk" = "cs_item_sk")
   AND ("d_date_sk" = "cs_sold_date_sk")
   AND ("cs_ext_discount_amt" <> (
      SELECT (DECIMAL '1.3' * "avg"("cs_ext_discount_amt"))
      FROM
        catalog_sales
      , date_dim
      WHERE ("cs_item_sk" = "i_item_sk")
         AND ("d_date" BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND ("d_date_sk" = "cs_sold_date_sk")
   ))
