CREATE TABLE q93
--WITH--
AS
SELECT
  "ss_customer_sk"
, "r_reason_desc"
, "sum"("act_sales") "sumsales"
FROM
  (
   SELECT
     "ss_item_sk"
   , "r_reason_desc"
   , "ss_ticket_number"
   , "ss_customer_sk"
   , (CASE WHEN ("sr_return_quantity" IS NOT NULL) THEN (("ss_quantity" - "sr_return_quantity") * "ss_sales_price") ELSE ("ss_quantity" * "ss_sales_price") END) "act_sales"
   FROM
     (store_sales
   LEFT JOIN store_returns ON ("sr_item_sk" = "ss_item_sk")
      AND ("sr_ticket_number" = "ss_ticket_number"))
   , reason
   WHERE ("sr_reason_sk" = "r_reason_sk")
)  t
GROUP BY "ss_customer_sk","r_reason_desc"
