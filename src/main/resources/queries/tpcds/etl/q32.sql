CREATE TABLE q32
AS
SELECT
 *
FROM
  catalog_sales
, item
, date_dim
WHERE
   ("i_item_sk" = "cs_item_sk")
   AND ("d_date_sk" = "cs_sold_date_sk")
   AND ("cs_ext_discount_amt" <> (
      SELECT (DECIMAL '1.3' * "avg"("cs_ext_discount_amt"))
      FROM
        catalog_sales
      , date_dim
      WHERE ("cs_item_sk" = "i_item_sk")
         AND ("d_date" BETWEEN CAST('2000-01-27' AS DATE) AND (CAST('2000-01-27' AS DATE) + INTERVAL  '90' DAY))
         AND ("d_date_sk" = "cs_sold_date_sk")
   ))
