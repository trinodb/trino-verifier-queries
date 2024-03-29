CREATE TABLE q83
--WITH--
AS
WITH
  sr_items AS (
   SELECT
     "i_item_id" "item_id"
   , "sum"("sr_return_quantity") "sr_item_qty"
   FROM
     store_returns
   , item
   , date_dim
   WHERE ("sr_item_sk" = "i_item_sk")
      AND ("sr_returned_date_sk" = "d_date_sk")
   GROUP BY "i_item_id"
)
, cr_items AS (
   SELECT
     "i_item_id" "item_id"
   , "sum"("cr_return_quantity") "cr_item_qty"
   FROM
     catalog_returns
   , item
   , date_dim
   WHERE ("cr_item_sk" = "i_item_sk")
      AND ("cr_returned_date_sk" = "d_date_sk")
   GROUP BY "i_item_id"
)
, wr_items AS (
   SELECT
     "i_item_id" "item_id"
   , "sum"("wr_return_quantity") "wr_item_qty"
   FROM
     web_returns
   , item
   , date_dim
   WHERE ("wr_item_sk" = "i_item_sk")
      AND ("wr_returned_date_sk" = "d_date_sk")
   GROUP BY "i_item_id"
)
SELECT
  "sr_items"."item_id"
, "sr_item_qty"
, CAST(((("sr_item_qty" / ((CAST("sr_item_qty" AS DECIMAL(19,4)) + "cr_item_qty") + "wr_item_qty")) / DECIMAL '3.0') * 100) AS DECIMAL(17,2)) "sr_dev"
, "cr_item_qty"
, CAST(((("cr_item_qty" / ((CAST("sr_item_qty" AS DECIMAL(19,4)) + "cr_item_qty") + "wr_item_qty")) / DECIMAL '3.0') * 100) AS DECIMAL(17,2)) "cr_dev"
, "wr_item_qty"
, CAST(((("wr_item_qty" / ((CAST("sr_item_qty" AS DECIMAL(19,4)) + "cr_item_qty") + "wr_item_qty")) / DECIMAL '3.0') * 100) AS DECIMAL(17,2)) "wr_dev"
, ((("sr_item_qty" + "cr_item_qty") + "wr_item_qty") / DECIMAL '3.00') "average"
FROM
  sr_items
, cr_items
, wr_items
WHERE ("sr_items"."item_id" = "cr_items"."item_id")
   AND ("sr_items"."item_id" = "wr_items"."item_id")
