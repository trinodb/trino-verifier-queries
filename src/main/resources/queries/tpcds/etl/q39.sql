CREATE TABLE q39_1
AS
WITH
  inv AS (
   SELECT
     "w_warehouse_name"
   , "w_warehouse_sk"
   , "i_item_sk"
   , "d_moy"
   , "stdev"
   , "mean"
   , (CASE "mean" WHEN 0 THEN null ELSE ("stdev" / "mean") END) "cov"
   FROM
     (
      SELECT
        "w_warehouse_name"
      , "w_warehouse_sk"
      , "i_item_sk"
      , "d_moy"
      , "stddev_samp"("inv_quantity_on_hand") "stdev"
      , "avg"("inv_quantity_on_hand") "mean"
      FROM
        inventory
      , item
      , warehouse
      , date_dim
      WHERE ("inv_item_sk" = "i_item_sk")
         AND ("inv_warehouse_sk" = "w_warehouse_sk")
         AND ("inv_date_sk" = "d_date_sk")
      GROUP BY "w_warehouse_name", "w_warehouse_sk", "i_item_sk", "d_moy"
   )  foo
)
SELECT
  "inv1"."w_warehouse_sk"
, "inv1"."i_item_sk"
, "inv1"."d_moy" d_moy_1
, "inv1"."mean" mean_1
, "inv1"."cov" cov_1
, "inv2"."d_moy" d_moy_2
, "inv2"."mean" mean_2
, "inv2"."cov" cov_2
FROM
  inv inv1
, inv inv2
WHERE ("inv1"."i_item_sk" = "inv2"."i_item_sk")
   AND ("inv1"."w_warehouse_sk" = "inv2"."w_warehouse_sk")
   AND ("inv1"."d_moy" = 1)
   AND ("inv2"."d_moy" = (1 + 1))
