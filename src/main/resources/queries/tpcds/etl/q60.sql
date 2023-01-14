CREATE TABLE q60
--WITH--
AS
WITH
  ss AS (
   SELECT
     "i_item_id"
   , "d_year"
   , "d_moy"
   , "ca_gmt_offset"
   , "sum"("ss_ext_sales_price") "total_sales"
   FROM
     store_sales
   , date_dim
   , customer_address
   , item
   WHERE ("i_item_id" IN (
      SELECT "i_item_id"
      FROM
        item
      WHERE ("i_category" IN ('Music'))
   ))
      AND ("ss_item_sk" = "i_item_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_addr_sk" = "ca_address_sk")
   GROUP BY "i_item_id", "d_year", "d_moy", "ca_gmt_offset"
)
, cs AS (
   SELECT
     "i_item_id"
   , "d_year"
   , "d_moy"
   , "ca_gmt_offset"
   , "sum"("cs_ext_sales_price") "total_sales"
   FROM
     catalog_sales
   , date_dim
   , customer_address
   , item
   WHERE ("i_item_id" NOT IN (
      SELECT "i_item_id"
      FROM
        item
      WHERE ("i_category" IN ('Music'))
   ))
      AND ("cs_item_sk" = "i_item_sk")
      AND ("cs_sold_date_sk" = "d_date_sk")
      AND ("cs_bill_addr_sk" = "ca_address_sk")
   GROUP BY "i_item_id", "d_year", "d_moy", "ca_gmt_offset"
)
, ws AS (
   SELECT
     "i_item_id"
   , "d_year"
   , "d_moy"
   , "ca_gmt_offset"
   , "sum"("ws_ext_sales_price") "total_sales"
   FROM
     web_sales
   , date_dim
   , customer_address
   , item
   WHERE ("i_item_id" NOT IN (
      SELECT "i_item_id"
      FROM
        item
      WHERE ("i_category" IN ('Music'))
   ))
      AND ("ws_item_sk" = "i_item_sk")
      AND ("ws_sold_date_sk" = "d_date_sk")
      AND ("ws_bill_addr_sk" = "ca_address_sk")
   GROUP BY "i_item_id", "d_year", "d_moy", "ca_gmt_offset"
)
SELECT
  *
FROM
  (
   SELECT *
   FROM
     ss
UNION ALL    SELECT *
   FROM
     cs
UNION ALL    SELECT *
   FROM
     ws
)  tmp1
