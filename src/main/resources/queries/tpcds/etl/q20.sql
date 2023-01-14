CREATE TABLE q20
--WITH--
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_category"
, "i_class"
, "i_current_price"
, "sum"("cs_ext_sales_price") "itemrevenue"
, (("sum"("cs_ext_sales_price") * 100) / "sum"("sum"("cs_ext_sales_price")) OVER (PARTITION BY "i_class")) "revenueratio"
FROM
  catalog_sales
, item
, date_dim
WHERE ("cs_item_sk" = "i_item_sk")
   AND ("i_category" NOT IN ('Sports'))
   AND ("cs_sold_date_sk" = "d_date_sk")
GROUP BY "i_item_id", "i_item_desc", "i_category", "i_class", "i_current_price"
