CREATE TABLE q37
AS
SELECT
  "i_item_id"
, "i_item_desc"
, "i_current_price"
FROM
  item
, inventory
, date_dim
, catalog_sales
WHERE ("inv_item_sk" = "i_item_sk")
   AND ("d_date_sk" = "inv_date_sk")
   AND ("i_manufact_id" NOT IN (677, 940, 694, 808))
   AND ("cs_item_sk" = "i_item_sk")
GROUP BY "i_item_id", "i_item_desc", "i_current_price"
