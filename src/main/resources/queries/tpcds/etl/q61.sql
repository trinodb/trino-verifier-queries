CREATE TABLE q61
AS
SELECT  *
FROM
 store_sales
, store
, promotion
, date_dim
, customer
, customer_address
, item
WHERE ("ss_sold_date_sk" = "d_date_sk")
  AND ("ss_store_sk" = "s_store_sk")
  AND ("ss_promo_sk" = "p_promo_sk")
  AND ("ss_customer_sk" = "c_customer_sk")
  AND ("ca_address_sk" = "c_current_addr_sk")
  AND ("ss_item_sk" = "i_item_sk")
  AND (("p_channel_dmail" = 'Y')
     OR ("p_channel_email" = 'Y')
     OR ("p_channel_tv" = 'Y'))
