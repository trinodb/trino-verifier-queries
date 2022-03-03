CREATE TABLE q18
AS
SELECT
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity) quantity,
  count(distinct l_linenumber) distinct_linenumbers,
  count(distinct l_orderkey) distinct_orderkeys
FROM
  customer,
  orders,
  lineitem
WHERE
  o_orderkey NOT IN (
    SELECT l_orderkey
    FROM
      lineitem
    GROUP BY
      l_orderkey
    HAVING
      sum(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
