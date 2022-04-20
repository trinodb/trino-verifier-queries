CREATE TABLE q82
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_current_price"
, "i_manufact_id"
, sum(inv_quantity_on_hand) "quantity"
FROM
  item
, inventory
, date_dim
WHERE ("inv_item_sk" = "i_item_sk")
   AND ("d_date_sk" = "inv_date_sk")
GROUP BY "i_item_id", "i_item_desc", "i_current_price", "i_manufact_id"
