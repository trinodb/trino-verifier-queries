CREATE TABLE q5
AS
SELECT
  c_custkey,
  c_name,
  c_phone,
  c_mktsegment,
  o_orderkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  l_quantity,
  l_extendedprice,
  l_discount,
  l_tax,
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_commitdate,
  l_receiptdate,
  s_suppkey,
  s_name,
  s_phone,
  s_acctbal,
  n_name,
  r_name
FROM
  customer,
  orders,
  lineitem,
  supplier,
  nation,
  region
WHERE
  c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name IN ('AMERICA', 'ASIA', 'AFRICA')
  AND l_returnflag = 'A'
