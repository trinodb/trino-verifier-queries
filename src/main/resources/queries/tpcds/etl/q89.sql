CREATE TABLE q89
--WITH--
AS
SELECT *
FROM
  (
   SELECT
     "i_category"
   , "i_class"
   , "i_brand"
   , "s_store_name"
   , "s_company_name"
   , "d_moy"
   , "d_year"
   , "sum"("ss_sales_price") "sum_sales"
   , "avg"("sum"("ss_sales_price")) OVER (PARTITION BY "i_category", "i_brand", "s_store_name", "s_company_name") "avg_monthly_sales"
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE ("ss_item_sk" = "i_item_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_store_sk" = "s_store_sk")
   GROUP BY "i_category", "i_class", "i_brand", "s_store_name", "s_company_name", "d_moy", "d_year"
)
