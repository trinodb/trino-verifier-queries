CREATE TABLE q48
AS
SELECT
  ss_sold_date_sk,
  ss_sold_time_sk,
  ss_item_sk,
  ss_customer_sk,
  ss_cdemo_sk,
  ss_hdemo_sk,
  ss_addr_sk,
  ss_store_sk,
  ss_promo_sk,
  ss_ticket_number,
  ss_quantity,
  ss_wholesale_cost,
  ss_list_price,
  ss_sales_price,
  ss_ext_discount_amt,
  ss_ext_sales_price,
  ss_ext_wholesale_cost,
  ss_ext_list_price,
  ss_ext_tax,
  ss_coupon_amt,
  ss_net_paid,
  ss_net_paid_inc_tax,
  ss_net_profit,
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
  cd_demo_sk,
  cd_gender,
  cd_marital_status,
  cd_education_status,
  cd_purchase_estimate,
  cd_credit_rating,
  cd_dep_count,
  cd_dep_employed_count,
  cd_dep_college_count,
  ca_address_sk,
  ca_address_id,
  ca_street_number,
  ca_street_name,
  ca_street_type,
  ca_suite_number,
  ca_city,
  ca_county,
  ca_state,
  ca_zip,
  ca_country,
  ca_gmt_offset,
  ca_location_type,
  d_date_sk,
  d_date_id,
  d_date,
  d_month_seq,
  d_week_seq,
  d_quarter_seq,
  d_year,
  d_dow,
  d_moy,
  d_dom,
  d_qoy,
  d_fy_year,
  d_fy_quarter_seq,
  d_fy_week_seq,
  d_day_name,
  d_quarter_name,
  d_holiday,
  d_weekend,
  d_following_holiday,
  d_first_dom,
  d_last_dom,
  d_same_day_ly,
  d_same_day_lq,
  d_current_day,
  d_current_week,
  d_current_month,
  d_current_quarter,
  d_current_year
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
