CREATE TABLE q4
AS
SELECT
  *
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
