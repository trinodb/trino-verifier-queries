CREATE TABLE q3
AS
SELECT
  "dt"."d_year"
, "item"."i_brand_id" "brand_id"
, "item"."i_brand" "brand"
, "store_sales"."ss_customer_sk"
, "sum"("ss_ext_sales_price") "sum_agg"
, "count"(DISTINCT "ss_promo_sk") "ss_promo_sk_distinct"
FROM
  date_dim dt
, store_sales
, item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
GROUP BY "dt"."d_year", "item"."i_brand", "item"."i_brand_id", "store_sales"."ss_customer_sk"
