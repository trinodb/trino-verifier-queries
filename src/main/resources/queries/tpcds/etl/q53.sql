CREATE TABLE q53
AS
SELECT *
FROM
  (
   SELECT
     "i_manufact_id"
   , "i_category"
   , "i_class"
   , "i_brand"
   , "sum"("ss_sales_price") "sum_sales"
   , "avg"("sum"("ss_sales_price")) OVER (PARTITION BY "i_manufact_id") "avg_quarterly_sales"
   FROM
     item
   , store_sales
   , date_dim
   , store
   WHERE ("ss_item_sk" = "i_item_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_store_sk" = "s_store_sk")
      AND ("d_month_seq" IN (1200   , (1200 + 1)   , (1200 + 2)   , (1200 + 3)   , (1200 + 4)   , (1200 + 5)   , (1200 + 6)   , (1200 + 7)   , (1200 + 8)   , (1200 + 9)   , (1200 + 10)   , (1200 + 11)))
   GROUP BY "i_manufact_id", "d_qoy", "i_category", "i_class", "i_brand"
)  tmp1
WHERE ((CASE WHEN ("avg_quarterly_sales" > 0) THEN ("abs"((CAST("sum_sales" AS DECIMAL(38,4)) - "avg_quarterly_sales")) / "avg_quarterly_sales") ELSE null END) > DECIMAL '0.1')
