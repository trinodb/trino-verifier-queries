CREATE TABLE q77
AS
WITH
  ss AS (
   SELECT
     "s_store_sk"
   , "d_date"
   , "sum"("ss_ext_sales_price") "sales"
   , "sum"("ss_net_profit") "profit"
   FROM
     store_sales
   , date_dim
   , store
   WHERE ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_store_sk" = "s_store_sk")
   GROUP BY "s_store_sk", "d_date"
)
, ws AS (
   SELECT
     "wp_web_page_sk"
   , "d_date"
   , "sum"("ws_ext_sales_price") "sales"
   , "sum"("ws_net_profit") "profit"
   FROM
     web_sales
   , date_dim
   , web_page
   WHERE ("ws_sold_date_sk" = "d_date_sk")
      AND ("ws_web_page_sk" = "wp_web_page_sk")
   GROUP BY "wp_web_page_sk", "d_date"
)
SELECT
  *
FROM
  (
   SELECT
     'store channel' "channel"
   , "ss"."s_store_sk" "id"
   , "ss"."d_date"
   , "sales"
   FROM
     ss
UNION ALL    SELECT
     'web channel' "channel"
   , "ws"."wp_web_page_sk" "id"
   , "ws"."d_date"
   , "sales"
   FROM
     ws
)  x
