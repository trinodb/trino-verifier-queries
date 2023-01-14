CREATE TABLE q35
--WITH--
AS
SELECT
  "ca_state"
, "cd_gender"
, "cd_marital_status"
, "cd_dep_count"
, "count"(*) "cnt1"
, "min"("cd_dep_count") min_cd_dep_count
, "max"("cd_dep_count") max_cd_dep_count
, "avg"("cd_dep_count") avg_cd_dep_count
, "cd_dep_employed_count"
, "count"(*) "cnt2"
, "min"("cd_dep_employed_count") min_cd_dep_employed_count
, "max"("cd_dep_employed_count") max_cd_dep_employed_count
, "avg"("cd_dep_employed_count") avg_cd_dep_employed_count
, "cd_dep_college_count"
, "count"(*) "cnt3"
, "min"("cd_dep_college_count") min_cd_dep_college_count
, "max"("cd_dep_college_count") max_cd_dep_college_count
, "avg"("cd_dep_college_count") avg_cd_dep_college_count
FROM
  customer c
, customer_address ca
, customer_demographics
WHERE ("c"."c_current_addr_sk" = "ca"."ca_address_sk")
   AND ("cd_demo_sk" = "c"."c_current_cdemo_sk")
   AND (EXISTS (
   SELECT *
   FROM
     store_sales
   , date_dim
   WHERE ("c"."c_customer_sk" = "ss_customer_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
))
   AND ((EXISTS (
      SELECT *
      FROM
        web_sales
      , date_dim
      WHERE ("c"."c_customer_sk" = "ws_bill_customer_sk")
         AND ("ws_sold_date_sk" = "d_date_sk")
   ))
      OR (EXISTS (
      SELECT *
      FROM
        catalog_sales
      , date_dim
      WHERE ("c"."c_customer_sk" = "cs_ship_customer_sk")
         AND ("cs_sold_date_sk" = "d_date_sk")
   )))
GROUP BY "ca_state", "cd_gender", "cd_marital_status", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"
