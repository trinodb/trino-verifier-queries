CREATE TABLE q7
AS
SELECT
  "i_item_id"
, "cd_gender"
, "cd_marital_status"
, "cd_education_status"
, "p_channel_email"
, "p_channel_event"
, "d_year"
, "avg"("ss_quantity") "agg1"
, "avg"("ss_list_price") "agg2"
, "avg"("ss_coupon_amt") "agg3"
, "avg"("ss_sales_price") "agg4"
FROM
  store_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE ("ss_sold_date_sk" = "d_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
   AND ("ss_cdemo_sk" = "cd_demo_sk")
   AND ("ss_promo_sk" = "p_promo_sk")
GROUP BY 1, 2, 3, 4, 5, 6, 7
