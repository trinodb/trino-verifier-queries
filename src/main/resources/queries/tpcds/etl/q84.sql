CREATE TABLE q84
--WITH--
AS
SELECT
  "c_customer_id" "customer_id"
, "concat"("concat"("c_last_name", ', '), "c_first_name") "customername"
FROM
  customer
, customer_address
, customer_demographics
, household_demographics
, income_band
, store_returns
WHERE ("c_current_addr_sk" = "ca_address_sk")
   AND ("ib_income_band_sk" = "hd_income_band_sk")
   AND ("cd_demo_sk" = "c_current_cdemo_sk")
   AND ("hd_demo_sk" = "c_current_hdemo_sk")
   AND ("sr_cdemo_sk" = "cd_demo_sk")
   AND ("ib_lower_bound" >= 38128)
   AND ("ib_upper_bound" <= (38128 + 50000))
