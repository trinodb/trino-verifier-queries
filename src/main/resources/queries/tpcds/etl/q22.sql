CREATE TABLE q22
AS
SELECT
  "i_product_name"
, "i_brand"
, "i_class"
, "i_category"
, "avg"("inv_quantity_on_hand") "qoh"
FROM
  inventory
, date_dim
, item
WHERE ("inv_date_sk" = "d_date_sk")
   AND ("inv_item_sk" = "i_item_sk")
GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category)
