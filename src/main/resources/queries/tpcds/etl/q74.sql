CREATE TABLE q74
--WITH--
AS
WITH
  year_total AS (
   SELECT
     "c_customer_id" "customer_id"
   , "c_first_name" "customer_first_name"
   , "c_last_name" "customer_last_name"
   , "d_year" "YEAR"
   , "sum"("ss_net_paid") "year_total"
   , 's' "sale_type"
   FROM
     customer
   , store_sales
   , date_dim
   WHERE ("c_customer_sk" = "ss_customer_sk")
      AND ("ss_sold_date_sk" = "d_date_sk")
   GROUP BY "c_customer_id", "c_first_name", "c_last_name", "d_year"
UNION ALL    SELECT
     "c_customer_id" "customer_id"
   , "c_first_name" "customer_first_name"
   , "c_last_name" "customer_last_name"
   , "d_year" "YEAR"
   , "sum"("ws_net_paid") "year_total"
   , 'w' "sale_type"
   FROM
     customer
   , web_sales
   , date_dim
   WHERE ("c_customer_sk" = "ws_bill_customer_sk")
      AND ("ws_sold_date_sk" = "d_date_sk")
   GROUP BY "c_customer_id", "c_first_name", "c_last_name", "d_year"
)
SELECT
  "t_s_secdecade"."customer_id"
, "t_s_secdecade"."customer_first_name"
, "t_s_secdecade"."customer_last_name"
FROM
  year_total t_s_firstdecade
, year_total t_s_secdecade
, year_total t_w_firstdecade
, year_total t_w_secdecade
WHERE ("t_s_secdecade"."customer_id" = "t_s_firstdecade"."customer_id")
   AND ("t_s_firstdecade"."customer_id" = "t_w_secdecade"."customer_id")
   AND ("t_s_firstdecade"."customer_id" = "t_w_firstdecade"."customer_id")
   AND ("t_s_firstdecade"."sale_type" = 's')
   AND ("t_w_firstdecade"."sale_type" = 'w')
   AND ("t_s_secdecade"."sale_type" = 's')
   AND ("t_w_secdecade"."sale_type" = 'w')
   AND ("t_s_firstdecade"."year" BETWEEN 1990 AND 2000)
   AND ("t_s_secdecade"."year" BETWEEN 2000 AND 2010)
   AND ("t_w_firstdecade"."year" BETWEEN 1990 AND 2000)
   AND ("t_w_secdecade"."year" BETWEEN 2000 AND 2010)
   AND ("t_s_firstdecade"."year_total" > 0)
   AND ("t_w_firstdecade"."year_total" > 0)
