CREATE TABLE q6
--WITH--
AS
SELECT
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
  l_comment
FROM
  lineitem
WHERE
  l_shipdate >= DATE '1994-01-01'
  AND l_discount BETWEEN decimal '0.06' - decimal '0.01' AND decimal '0.06' + decimal '0.01'
  AND l_quantity < 24
