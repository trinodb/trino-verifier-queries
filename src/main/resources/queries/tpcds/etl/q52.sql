CREATE TABLE q52
--WITH--
AS
SELECT
  "dt"."d_year"
, "item"."i_brand_id" "brand_id"
, "item"."i_brand" "brand"
, "item"."i_manager_id"
, "sum"("ss_ext_sales_price") "ext_price"
FROM
  date_dim dt
, store_sales
, item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
GROUP BY "dt"."d_year", "item"."i_brand", "item"."i_brand_id", "item"."i_manager_id"
