CREATE TABLE q94
--WITH--
AS
SELECT
"web_company_name"
, "ca_state"
,  "count"(DISTINCT "ws_order_number") "order_count"
, "sum"("ws_ext_ship_cost") "total_shipping_cost"
, "sum"("ws_net_profit") "total_net_profit"
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE ("d_date" BETWEEN CAST('1999-2-01' AS DATE) AND (CAST('1999-2-01' AS DATE) + INTERVAL  '60' DAY))
   AND ("ws1"."ws_ship_date_sk" = "d_date_sk")
   AND ("ws1"."ws_ship_addr_sk" = "ca_address_sk")
   AND ("ws1"."ws_web_site_sk" = "web_site_sk")
   AND (EXISTS (
   SELECT *
   FROM
     web_sales ws2
   WHERE ("ws1"."ws_order_number" = "ws2"."ws_order_number")
      AND ("ws1"."ws_warehouse_sk" <> "ws2"."ws_warehouse_sk")
))
   AND (NOT (EXISTS (
   SELECT *
   FROM
     web_returns wr1
   WHERE ("ws1"."ws_order_number" = "wr1"."wr_order_number")
)))
GROUP BY 1, 2
