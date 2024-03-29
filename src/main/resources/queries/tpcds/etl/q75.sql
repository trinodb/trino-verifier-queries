CREATE TABLE q75
--WITH--
AS
SELECT
 "d_year"
, "i_brand_id"
, "i_class_id"
, "i_category_id"
, "i_manufact_id"
, "sum"("sales_cnt") "sales_cnt"
, "sum"("sales_amt") "sales_amt"
FROM
 (
  SELECT
    "d_year"
  , "i_brand_id"
  , "i_class_id"
  , "i_category_id"
  , "i_manufact_id"
  , ("cs_quantity" - COALESCE("cr_return_quantity", 0)) "sales_cnt"
  , ("cs_ext_sales_price" - COALESCE("cr_return_amount", DECIMAL '0.0')) "sales_amt"
  FROM
    (((catalog_sales
  INNER JOIN item ON ("i_item_sk" = "cs_item_sk"))
  INNER JOIN date_dim ON ("d_date_sk" = "cs_sold_date_sk"))
  LEFT JOIN catalog_returns ON ("cs_order_number" = "cr_order_number")
     AND ("cs_item_sk" = "cr_item_sk"))
UNION ALL
SELECT
    "d_year"
  , "i_brand_id"
  , "i_class_id"
  , "i_category_id"
  , "i_manufact_id"
  , ("ss_quantity" - COALESCE("sr_return_quantity", 0)) "sales_cnt"
  , ("ss_ext_sales_price" - COALESCE("sr_return_amt", DECIMAL '0.0')) "sales_amt"
  FROM
    (((store_sales
  INNER JOIN item ON ("i_item_sk" = "ss_item_sk"))
  INNER JOIN date_dim ON ("d_date_sk" = "ss_sold_date_sk"))
  LEFT JOIN store_returns ON ("ss_ticket_number" = "sr_ticket_number")
     AND ("ss_item_sk" = "sr_item_sk"))
UNION ALL
SELECT
    "d_year"
  , "i_brand_id"
  , "i_class_id"
  , "i_category_id"
  , "i_manufact_id"
  , ("ws_quantity" - COALESCE("wr_return_quantity", 0)) "sales_cnt"
  , ("ws_ext_sales_price" - COALESCE("wr_return_amt", DECIMAL '0.0')) "sales_amt"
  FROM
    (((web_sales
  INNER JOIN item ON ("i_item_sk" = "ws_item_sk"))
  INNER JOIN date_dim ON ("d_date_sk" = "ws_sold_date_sk"))
  LEFT JOIN web_returns ON ("ws_order_number" = "wr_order_number")
     AND ("ws_item_sk" = "wr_item_sk"))
)  sales_detail
GROUP BY "d_year", "i_brand_id", "i_class_id", "i_category_id", "i_manufact_id"
