CREATE TABLE q97
AS
WITH
  ssci AS (
   SELECT
     "ss_customer_sk" "customer_sk"
   , "ss_item_sk" "item_sk"
   , "d_month_seq"
   FROM
     store_sales
   , date_dim
   WHERE ("ss_sold_date_sk" = "d_date_sk")
   GROUP BY "ss_customer_sk", "ss_item_sk", "d_month_seq"
)
, csci AS (
   SELECT
     "cs_bill_customer_sk" "customer_sk"
   , "cs_item_sk" "item_sk"
   , "d_month_seq"
   FROM
     catalog_sales
   , date_dim
   WHERE ("cs_sold_date_sk" = "d_date_sk")
   GROUP BY "cs_bill_customer_sk", "cs_item_sk", "d_month_seq"
)
SELECT
"ssci"."customer_sk"
, "ssci"."item_sk"
, "ssci"."d_month_seq"
,  "sum"((CASE WHEN ("ssci"."customer_sk" IS NOT NULL)
   AND ("csci"."customer_sk" IS NULL) THEN 1 ELSE 0 END)) "store_only"
, "sum"((CASE WHEN ("ssci"."customer_sk" IS NULL)
   AND ("csci"."customer_sk" IS NOT NULL) THEN 1 ELSE 0 END)) "catalog_only"
, "sum"((CASE WHEN ("ssci"."customer_sk" IS NOT NULL)
   AND ("csci"."customer_sk" IS NOT NULL) THEN 1 ELSE 0 END)) "store_and_catalog"
FROM
  (ssci
FULL JOIN csci ON ("ssci"."customer_sk" = "csci"."customer_sk")
   AND ("ssci"."item_sk" = "csci"."item_sk"))
GROUP BY 1, 2, 3
