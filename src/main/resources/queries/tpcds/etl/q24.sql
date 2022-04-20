CREATE TABLE q24
AS
SELECT
 "s_store_name"
, "s_state"
, "i_color"
, "i_current_price"
, "i_manager_id"
, "i_units"
, "i_size"
, "sum"("ss_net_paid") "netpaid"
FROM
 store_sales
, store
, item
WHERE ("ss_item_sk" = "i_item_sk")
  AND ("ss_store_sk" = "s_store_sk")
GROUP BY "s_store_name", "s_state", "i_color", "i_current_price", "i_manager_id", "i_units", "i_size"
