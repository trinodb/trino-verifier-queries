CREATE TABLE q18
--WITH--
AS
SELECT
  c_custkey,
  o_orderkey,
  sum(o_totalprice) totalprice,
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
      sum(l_quantity) > 70
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY
  c_custkey,
  o_orderkey
