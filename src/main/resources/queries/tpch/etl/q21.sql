CREATE TABLE q21
AS
SELECT
  l_quantity,
  l_extendedprice,
  l_discount,
  l_tax,
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_commitdate,
  l_receiptdate,
  o_orderkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  s_suppkey,
  s_name,
  s_phone,
  s_acctbal,
  n_name
FROM
  supplier,
  lineitem l1,
  orders,
  nation
WHERE
  s_suppkey = l1.l_suppkey
  AND o_orderkey = l1.l_orderkey
  AND o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND exists(
    SELECT *
    FROM
      lineitem l2
    WHERE
      l2.l_orderkey = l1.l_orderkey
      AND l2.l_suppkey = l1.l_suppkey
      AND l_returnflag = 'A'
  )
  AND NOT exists(
    SELECT *
    FROM
      lineitem l3
    WHERE
      l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey = l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
      AND l_returnflag = 'R'
  )
  AND s_nationkey = n_nationkey
