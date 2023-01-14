CREATE TABLE q4
--WITH--
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
  o_comment
FROM orders
WHERE
  o_orderdate >= DATE '1993-07-01'
AND EXISTS (
    SELECT *
    FROM lineitem
    WHERE
    l_orderkey = o_orderkey
    AND l_commitdate < l_receiptdate
)
