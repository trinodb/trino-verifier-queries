CREATE TABLE q1
--WITH--
AS
WITH
  customer_total_return AS (
   SELECT
     "sr_customer_sk" "ctr_customer_sk"
   , "sr_store_sk" "ctr_store_sk"
   , "sum"("sr_return_amt") "ctr_total_return"
   FROM
     store_returns
   , date_dim
   WHERE ("sr_returned_date_sk" = "d_date_sk")
   GROUP BY "sr_customer_sk", "sr_store_sk"
)
SELECT
  ctr_customer_sk,
  ctr_store_sk,
  ctr_total_return,
  s_store_sk,
  s_store_id,
  s_rec_start_date,
  s_rec_end_date,
  s_closed_date_sk,
  s_store_name,
  s_number_employees,
  s_floor_space,
  s_hours,
  s_manager,
  s_market_id,
  s_geography_class,
  s_market_desc,
  s_market_manager,
  s_division_id,
  s_division_name,
  s_company_id,
  s_company_name,
  s_street_number,
  s_street_name,
  s_street_type,
  s_suite_number,
  s_city,
  s_county,
  s_state,
  s_zip,
  s_country,
  s_gmt_offset,
  s_tax_precentage,
  c_customer_sk,
  c_customer_id,
  c_current_cdemo_sk,
  c_current_hdemo_sk,
  c_current_addr_sk,
  c_first_shipto_date_sk,
  c_first_sales_date_sk,
  c_salutation,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_day,
  c_birth_month,
  c_birth_year,
  c_birth_country,
  c_login,
  c_email_address,
  c_last_review_date_sk
FROM
  customer_total_return ctr1
, store
, customer
WHERE ("ctr1"."ctr_total_return" <> (
      SELECT ("avg"("ctr_total_return") * DECIMAL '1.2')
      FROM
        customer_total_return ctr2
      WHERE ("ctr1"."ctr_store_sk" = "ctr2"."ctr_store_sk")
   ))
   AND ("s_store_sk" = "ctr1"."ctr_store_sk")
   AND ("ctr1"."ctr_customer_sk" = "c_customer_sk")
