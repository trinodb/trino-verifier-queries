CREATE TABLE q42
AS
SELECT
  "dt"."d_year"
, "item"."i_category_id"
, "item"."i_category"
, "item"."i_manager_id"
, "dt"."d_moy"
, "sum"("ss_ext_sales_price") sum_ss_ext_sales_price
FROM
  date_dim dt
, store_sales
, item
WHERE ("dt"."d_date_sk" = "store_sales"."ss_sold_date_sk")
   AND ("store_sales"."ss_item_sk" = "item"."i_item_sk")
GROUP BY "dt"."d_year", "item"."i_category_id", "item"."i_category", "item"."i_manager_id", "dt"."d_moy", "dt"."d_year"
