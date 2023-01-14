CREATE TABLE q13
--WITH--
AS
SELECT
    c_custkey,
    count(distinct o_orderkey) distinct_orders,
    count(distinct o_orderdate) dates,
    count(distinct o_clerk) distinct_clerks,
    array_sort(array_agg(distinct o_comment)) comments,
    sum(o_totalprice) total_price
FROM
    customer
    LEFT OUTER JOIN orders ON
        c_custkey = o_custkey
        AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey
