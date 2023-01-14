CREATE TABLE q85
--WITH--
AS
SELECT
  "substr"("r_reason_desc", 1, 20) r_reason_desc
, "cd1"."cd_marital_status"
, "cd1"."cd_education_status"
, "ca_country"
, "ca_state"
, "avg"("ws_quantity") avg_ws_quantity
, "avg"("wr_refunded_cash") avg_wr_refunded_cash
, "avg"("wr_fee") avg_wr_fee
FROM
  web_sales
, web_returns
, web_page
, customer_demographics cd1
, customer_demographics cd2
, customer_address
, date_dim
, reason
WHERE ("ws_web_page_sk" = "wp_web_page_sk")
   AND ("ws_item_sk" = "wr_item_sk")
   AND ("ws_order_number" = "wr_order_number")
   AND ("ws_sold_date_sk" = "d_date_sk")
   AND ("cd1"."cd_demo_sk" = "wr_refunded_cdemo_sk")
   AND ("cd2"."cd_demo_sk" = "wr_returning_cdemo_sk")
   AND ("ca_address_sk" = "wr_refunded_addr_sk")
   AND ("r_reason_sk" = "wr_reason_sk")
   AND ("cd1"."cd_marital_status" = "cd2"."cd_marital_status")
GROUP BY "r_reason_desc", "cd1"."cd_marital_status", "cd1"."cd_education_status", "ca_country", "ca_state"
