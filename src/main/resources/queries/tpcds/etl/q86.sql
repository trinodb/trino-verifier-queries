CREATE TABLE q86
--WITH--
AS
SELECT
  "sum"("ws_net_paid") "total_sum"
, "i_category"
, "i_class"
, "d_month_seq"
, (GROUPING ("i_category") + GROUPING ("i_class")) "lochierarchy"
, "rank"() OVER (PARTITION BY (GROUPING ("i_category") + GROUPING ("i_class")), (CASE WHEN (GROUPING ("i_class") = 0) THEN "i_category" END) ORDER BY "sum"("ws_net_paid") DESC) "rank_within_parent"
FROM
  web_sales
, date_dim d1
, item
WHERE ("d1"."d_date_sk" = "ws_sold_date_sk")
   AND ("i_item_sk" = "ws_item_sk")
GROUP BY ROLLUP (i_category, i_class, d_month_seq)
