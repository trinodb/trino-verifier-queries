CREATE TABLE q12
AS
SELECT
  o_orderkey,
  o_custkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment,
  l_orderkey,
  l_partkey,
  l_suppkey,
  l_linenumber,
  l_quantity,
  l_extendedprice,
  l_discount,
  l_tax,
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_commitdate,
  l_receiptdate,
  l_shipinstruct,
  l_shipmode,
  l_comment,
  CASE
      WHEN o_orderpriority = '1-URGENT'
           OR o_orderpriority = '2-HIGH'
        THEN 1
      ELSE 0
      END AS high_line,
  CASE
      WHEN o_orderpriority <> '1-URGENT'
           AND o_orderpriority <> '2-HIGH'
        THEN 1
      ELSE 0
      END AS low_line
FROM
  orders,
  lineitem
WHERE
  o_orderkey = l_orderkey
  AND l_shipmode IN ('MAIL', 'SHIP')
  AND l_commitdate < l_receiptdate
  AND l_shipdate < l_commitdate
  AND l_receiptdate >= DATE '1994-01-01'
