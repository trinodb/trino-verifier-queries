CREATE TABLE q12
AS
SELECT
  *,
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
