CREATE TABLE q3
AS
SELECT
  l_orderkey,
  o_orderdate,
  o_shippriority,
  sum(l_extendedprice * (1 - l_discount)) 		        AS revenue,
  sum(l_quantity)                                       AS sum_qty,
  sum(l_extendedprice)                                  AS sum_base_price,
  sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
  avg(l_quantity)                                       AS avg_qty,
  avg(l_extendedprice)                                  AS avg_price,
  avg(l_discount)                                       AS avg_disc,
  count(*)                                              AS c
FROM
  customer,
  orders,
  lineitem
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
GROUP BY
  l_orderkey,
  o_orderdate,
  o_shippriority
