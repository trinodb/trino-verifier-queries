CREATE TABLE q63
--WITH--
AS
SELECT *
FROM
  (
   SELECT
     "i_manager_id"
   , "i_category"
   , "i_class"
   , "i_brand"
   , "d_month_seq"
   , "sum"("ss_sales_price") "sum_sales"
   , "avg"("sum"("ss_sales_price")) OVER (PARTITION BY "i_manager_id") "avg_monthly_sales"
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE ("ss_item_sk" = "i_item_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_store_sk" = "s_store_sk")
   GROUP BY "i_manager_id", "d_moy", "i_category", "i_class", "i_brand", "d_month_seq"
)  tmp1
WHERE ((CASE WHEN ("avg_monthly_sales" > 0) THEN ("abs"(("sum_sales" - "avg_monthly_sales")) / "avg_monthly_sales") ELSE null END) > DECIMAL '0.1')
