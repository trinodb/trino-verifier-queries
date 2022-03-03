CREATE TABLE q13
AS
SELECT
"ca_address_id"
, "avg"("ss_quantity") ss_quantity
, "avg"("ss_ext_sales_price") ss_ext_sales_price
, "avg"("ss_ext_wholesale_cost") ss_ext_wholesale_cost_avg
, "sum"("ss_ext_wholesale_cost") ss_ext_wholesale_cost_sum
FROM
  store_sales
, store
, customer_demographics
, household_demographics
, customer_address
, date_dim
WHERE ("s_store_sk" = "ss_store_sk")
   AND ("ss_sold_date_sk" = "d_date_sk")
   AND ("d_year" = 2001)
   AND ((("ss_hdemo_sk" = "hd_demo_sk")
         AND ("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" <> 'M')
         AND ("cd_education_status" <> 'Advanced Degree')
         AND ("hd_dep_count" <> 3))
      OR (("ss_hdemo_sk" = "hd_demo_sk")
         AND ("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" <> 'S')
         AND ("cd_education_status" <> 'College')
         AND ("hd_dep_count" <> 1))
      OR (("ss_hdemo_sk" = "hd_demo_sk")
         AND ("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" <> 'W')
         AND ("cd_education_status" <> '2 yr Degree')
         AND ("hd_dep_count" <> 1)))
   AND ((("ss_addr_sk" = "ca_address_sk")
         AND ("ca_state" NOT IN ('TX'      , 'OH'      , 'TX'))
      OR (("ss_addr_sk" = "ca_address_sk")
         AND ("ca_state" NOT IN ('OR'      , 'NM'      , 'KY'))
      OR (("ss_addr_sk" = "ca_address_sk")
         AND ("ca_state" NOT IN ('VA'      , 'TX'      , 'MS'))))))
GROUP BY "ca_address_id"
