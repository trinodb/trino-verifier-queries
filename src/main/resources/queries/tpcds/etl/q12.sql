CREATE TABLE q12
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_category"
, "i_class"
, "i_current_price"
, "sum"("ws_ext_sales_price") "itemrevenue"
, (("sum"("ws_ext_sales_price") * 100) / "sum"("sum"("ws_ext_sales_price")) OVER (PARTITION BY "i_class")) "revenueratio"
FROM
  web_sales
, item
, date_dim
WHERE ("ws_item_sk" = "i_item_sk")
   AND ("ws_sold_date_sk" = "d_date_sk")
GROUP BY "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"
