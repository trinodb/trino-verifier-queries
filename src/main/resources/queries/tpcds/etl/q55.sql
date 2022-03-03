CREATE TABLE q55
AS
SELECT
  "i_brand_id" "brand_id"
, "i_brand" "brand"
, "i_manager_id"
, "d_moy"
, "d_year"
, "sum"("ss_ext_sales_price") "ext_price"
FROM
  date_dim
, store_sales
, item
WHERE ("d_date_sk" = "ss_sold_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
GROUP BY "i_brand", "i_brand_id", "i_manager_id", "d_moy", "d_year"
