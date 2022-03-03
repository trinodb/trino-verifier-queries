CREATE TABLE q14
AS
SELECT
    *,
    (CASE
    WHEN p_type LIKE 'PROMO%'
      THEN l_extendedprice * (1 - l_discount)
    ELSE 0
    END) / l_extendedprice * (1 - l_discount) AS promo_revenue
FROM
  lineitem,
  part
WHERE
  l_partkey = p_partkey
  AND l_shipdate >= DATE '1995-09-01'
