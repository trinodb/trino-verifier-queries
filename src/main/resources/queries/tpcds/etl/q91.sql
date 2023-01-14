CREATE TABLE q91
--WITH--
AS
SELECT
  "cc_call_center_id" "Call_Center"
, "cc_name" "Call_Center_Name"
, "cc_manager" "Manager"
, "cd_marital_status"
, "cd_education_status"
, "hd_buy_potential"
, "ca_gmt_offset"
, "sum"("cr_net_loss") "Returns_Loss"
FROM
  call_center
, catalog_returns
, date_dim
, customer
, customer_address
, customer_demographics
, household_demographics
WHERE ("cr_call_center_sk" = "cc_call_center_sk")
   AND ("cr_returned_date_sk" = "d_date_sk")
   AND ("cr_returning_customer_sk" = "c_customer_sk")
   AND ("cd_demo_sk" = "c_current_cdemo_sk")
   AND ("hd_demo_sk" = "c_current_hdemo_sk")
   AND ("ca_address_sk" = "c_current_addr_sk")
GROUP BY "cc_call_center_id", "cc_name", "cc_manager", "cd_marital_status", "cd_education_status", "hd_buy_potential", "ca_gmt_offset"
