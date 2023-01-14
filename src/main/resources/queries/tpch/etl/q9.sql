CREATE TABLE q9
--WITH--
AS
SELECT
    o_orderkey,
    max(nation) nation,
    max(o_year) o_year,
    sum(o_year) total_amount
FROM (
    SELECT
        o_orderkey,
        n_name                                                          AS nation,
        extract(YEAR FROM o_orderdate)                                  AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        s_suppkey = l_suppkey
        AND ps_suppkey = l_suppkey
        AND ps_partkey = l_partkey
        AND p_partkey = l_partkey
        AND o_orderkey = l_orderkey
        AND s_nationkey = n_nationkey
        AND p_name LIKE '%green%'
 )
 GROUP BY o_orderkey
