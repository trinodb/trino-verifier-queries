CREATE TABLE q26
--WITH--
AS
SELECT
  "i_item_id"
, "avg"("cs_quantity") "agg1"
, "avg"("cs_list_price") "agg2"
, "avg"("cs_coupon_amt") "agg3"
, "avg"("cs_sales_price") "agg4"
FROM
  catalog_sales
, customer_demographics
, date_dim
, item
, promotion
WHERE ("cs_sold_date_sk" = "d_date_sk")
   AND ("cs_item_sk" = "i_item_sk")
   AND ("cs_bill_cdemo_sk" = "cd_demo_sk")
   AND ("cs_promo_sk" = "p_promo_sk")
GROUP BY "i_item_id"
