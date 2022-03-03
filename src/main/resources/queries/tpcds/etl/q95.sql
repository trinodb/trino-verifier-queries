CREATE TABLE q95
AS
WITH
  ws_wh AS (
   SELECT
     "ws1"."ws_order_number"
   , "ws1"."ws_warehouse_sk" "wh1"
   , "ws2"."ws_warehouse_sk" "wh2"
   FROM
     web_sales ws1
   , web_sales ws2
   WHERE ("ws1"."ws_order_number" = "ws2"."ws_order_number")
      AND ("ws1"."ws_warehouse_sk" <> "ws2"."ws_warehouse_sk")
)
SELECT
"ca_state"
, "web_company_name"
, "ws_order_number"
,  "count"(DISTINCT "ws_order_number") "order count"
, "sum"("ws_ext_ship_cost") "total shipping cost"
, "sum"("ws_net_profit") "total net profit"
FROM
  web_sales ws1
, date_dim
, customer_address
, web_site
WHERE ("ws1"."ws_ship_date_sk" = "d_date_sk")
   AND ("ws1"."ws_ship_addr_sk" = "ca_address_sk")
   AND ("ws1"."ws_web_site_sk" = "web_site_sk")
GROUP BY "ca_state", "web_company_name", "ws_order_number"
