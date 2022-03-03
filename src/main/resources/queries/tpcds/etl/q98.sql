CREATE TABLE q98
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_category"
, "i_class"
, "i_current_price"
, "d_date"
, "sum"("ss_ext_sales_price") "itemrevenue"
, (("sum"("ss_ext_sales_price") * 100) / "sum"("sum"("ss_ext_sales_price")) OVER (PARTITION BY "i_class")) "revenueratio"
FROM
  store_sales
, item
, date_dim
WHERE ("ss_item_sk" = "i_item_sk")
   AND ("ss_sold_date_sk" = "d_date_sk")
GROUP BY "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price", "d_date"
