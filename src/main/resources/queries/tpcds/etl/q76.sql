CREATE TABLE q76
--WITH--
AS
SELECT
     'store' "channel"
   , 'ss_store_sk' "col_name"
   , "d_year"
   , "d_qoy"
   , "i_category"
   , "ss_ext_sales_price" "ext_sales_price"
   FROM
     store_sales
   , item
   , date_dim
   WHERE ("ss_store_sk" IS NULL)
      AND ("ss_sold_date_sk" = "d_date_sk")
      AND ("ss_item_sk" = "i_item_sk")
UNION ALL    SELECT
     'web' "channel"
   , 'ws_ship_customer_sk' "col_name"
   , "d_year"
   , "d_qoy"
   , "i_category"
   , "ws_ext_sales_price" "ext_sales_price"
   FROM
     web_sales
   , item
   , date_dim
   WHERE ("ws_ship_customer_sk" IS NULL)
      AND ("ws_sold_date_sk" = "d_date_sk")
      AND ("ws_item_sk" = "i_item_sk")
UNION ALL    SELECT
     'catalog' "channel"
   , 'cs_ship_addr_sk' "col_name"
   , "d_year"
   , "d_qoy"
   , "i_category"
   , "cs_ext_sales_price" "ext_sales_price"
   FROM
     catalog_sales
   , item
   , date_dim
   WHERE ("cs_ship_addr_sk" IS NULL)
      AND ("cs_sold_date_sk" = "d_date_sk")
      AND ("cs_item_sk" = "i_item_sk")
