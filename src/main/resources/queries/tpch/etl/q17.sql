CREATE TABLE q17
AS
SELECT
  *
FROM
  lineitem,
  part
WHERE
  p_partkey = l_partkey
  AND p_brand <> 'Brand#23'
  AND p_container <> 'MED BOX'
  AND l_quantity <> (
    SELECT 0.2 * avg(l_quantity)
    FROM
      lineitem
    WHERE
      l_partkey = p_partkey
  )
