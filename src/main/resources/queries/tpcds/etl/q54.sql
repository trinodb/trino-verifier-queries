CREATE TABLE q54
--WITH--
AS
SELECT DISTINCT
     "c_customer_sk"
   , "c_current_addr_sk"
   FROM
     (
      SELECT
        "cs_sold_date_sk" "sold_date_sk"
      , "cs_bill_customer_sk" "customer_sk"
      , "cs_item_sk" "item_sk"
      FROM
        catalog_sales
UNION ALL       SELECT
        "ws_sold_date_sk" "sold_date_sk"
      , "ws_bill_customer_sk" "customer_sk"
      , "ws_item_sk" "item_sk"
      FROM
        web_sales
   )  cs_or_ws_sales
   , item
   , date_dim
   , customer
   WHERE ("sold_date_sk" = "d_date_sk")
      AND ("item_sk" = "i_item_sk")
      AND ("c_customer_sk" = "cs_or_ws_sales"."customer_sk")
