CREATE TABLE q62
AS
SELECT
*,
  (CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") <= 30) THEN 1 ELSE 0 END) "30 days"
, (CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 30)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 60) THEN 1 ELSE 0 END) "31-60 days"
, (CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 60)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 90) THEN 1 ELSE 0 END) "61-90 days"
, (CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 90)
   AND (("ws_ship_date_sk" - "ws_sold_date_sk") <= 120) THEN 1 ELSE 0 END) "91-120 days"
, (CASE WHEN (("ws_ship_date_sk" - "ws_sold_date_sk") > 120) THEN 1 ELSE 0 END) ">120 days"
FROM
  web_sales
