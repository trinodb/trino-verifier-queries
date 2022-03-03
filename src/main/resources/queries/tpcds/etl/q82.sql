CREATE TABLE q82
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_current_price"
FROM
  item
, inventory
, date_dim
, store_sales
WHERE ("inv_item_sk" = "i_item_sk")
   AND ("d_date_sk" = "inv_date_sk")
   AND ("ss_item_sk" = "i_item_sk")
GROUP BY "i_item_id", "i_item_desc", "i_current_price"
