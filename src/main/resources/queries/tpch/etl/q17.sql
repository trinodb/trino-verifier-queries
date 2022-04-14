CREATE TABLE q17
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
  p_name,
  p_mfgr,
  p_brand,
  p_retailprice,
  p_size
FROM
  lineitem,
  part
WHERE
  p_partkey = l_partkey
  AND p_brand <> 'Brand#23'
  AND p_container <> 'MED BOX'
  AND l_returnflag = 'A'
  AND l_quantity < (
    SELECT avg(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
      AND l_returnflag = 'A'
  )
