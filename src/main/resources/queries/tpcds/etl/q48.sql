CREATE TABLE q48
AS
SELECT *
FROM
  store_sales
, store
, customer_demographics
, customer_address
, date_dim
WHERE ("s_store_sk" = "ss_store_sk")
   AND ("ss_sold_date_sk" = "d_date_sk")
   AND ((("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" = 'M')
         AND ("cd_education_status" <> '4 yr Degree'))
      OR (("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" = 'D')
         AND ("cd_education_status" <> '2 yr Degree'))
      OR (("cd_demo_sk" = "ss_cdemo_sk")
         AND ("cd_marital_status" = 'S')
         AND ("cd_education_status" <> 'College')))
   AND ((("ss_addr_sk" = "ca_address_sk")
         AND ("ca_country" = 'United States')
         AND ("ca_state" NOT IN ('CO'      , 'OH'      , 'TX')))
      OR (("ss_addr_sk" = "ca_address_sk")
         AND ("ca_country" = 'United States')
         AND ("ca_state" NOT IN ('OR'      , 'MN'      , 'KY')))
      OR (("ss_addr_sk" = "ca_address_sk")
         AND ("ca_country" = 'United States')
         AND ("ca_state" NOT IN ('VA'      , 'CA'      , 'MS'))))
