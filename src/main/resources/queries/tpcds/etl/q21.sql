CREATE TABLE q21
--WITH--
AS
SELECT
     "w_warehouse_name"
    , "i_item_id"
    , "sum"((CASE WHEN (CAST("d_date" AS DATE) < CAST('2000-03-11' AS DATE)) THEN "inv_quantity_on_hand" ELSE 0 END)) "inv_before"
    , "sum"((CASE WHEN (CAST("d_date" AS DATE) >= CAST('2000-03-11' AS DATE)) THEN "inv_quantity_on_hand" ELSE 0 END)) "inv_after"
FROM
     inventory
    , warehouse
    , item
    , date_dim
WHERE (("i_item_sk" = "inv_item_sk")
  AND ("inv_warehouse_sk" = "w_warehouse_sk")
  AND ("inv_date_sk" = "d_date_sk"))
GROUP BY "w_warehouse_name", "i_item_id"
