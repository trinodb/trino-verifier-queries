CREATE TABLE q67
--WITH--
AS
SELECT
 "i_category"
, "i_class"
, "i_brand"
, "i_product_name"
, "d_year"
, "s_store_id"
, "sumsales"
, "rank"() OVER (PARTITION BY "i_category" ORDER BY "sumsales" DESC) "rk"
FROM
 (
  SELECT
    "i_category"
  , "i_class"
  , "i_brand"
  , "i_product_name"
  , "d_year"
  , "s_store_id"
  , "sum"(COALESCE(("ss_sales_price" * "ss_quantity"), 0)) "sumsales"
  FROM
    store_sales
  , date_dim
  , store
  , item
  WHERE ("ss_sold_date_sk" = "d_date_sk")
     AND ("ss_item_sk" = "i_item_sk")
     AND ("ss_store_sk" = "s_store_sk")
     AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
  GROUP BY i_category, i_class, i_brand, i_product_name, d_year, s_store_id
)  dw1
