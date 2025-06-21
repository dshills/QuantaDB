package tpch

// Query3 - Customer Market Segment Query
// This query retrieves the 10 unshipped orders with the highest value.
const Query3 = `
SELECT l_orderkey, 
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       o_orderdate, 
       o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < date '1995-03-15'
  AND l_shipdate > date '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue desc, o_orderdate
LIMIT 10;`

// Query5 - Local Supplier Volume Query
// This query lists the revenue volume done through local suppliers.
const Query5 = `
SELECT n_name, 
       sum(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey
  AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey
  AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= date '1994-01-01'
  AND o_orderdate < date '1995-01-01'
GROUP BY n_name
ORDER BY revenue desc;`

// Query8 - National Market Share Query
// This query determines how the market share of a given nation within a given region
// has changed over two years for a given part type.
const Query8 = `
SELECT o_year,
       sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
FROM (
  SELECT extract(year from o_orderdate) as o_year,
         l_extendedprice * (1 - l_discount) as volume,
         n2.n_name as nation
  FROM part, supplier, lineitem, orders, customer, nation n1, nation n2, region
  WHERE p_partkey = l_partkey
    AND s_suppkey = l_suppkey
    AND l_orderkey = o_orderkey
    AND o_custkey = c_custkey
    AND c_nationkey = n1.n_nationkey
    AND n1.n_regionkey = r_regionkey
    AND r_name = 'AMERICA'
    AND s_nationkey = n2.n_nationkey
    AND o_orderdate between date '1995-01-01' and date '1996-12-31'
    AND p_type = 'ECONOMY ANODIZED STEEL'
) as all_nations
GROUP BY o_year
ORDER BY o_year;`

// Query10 - Returned Item Reporting Query
// This query identifies customers who might be having problems with the parts
// that are shipped to them.
const Query10 = `
SELECT c_custkey, 
       c_name,
       sum(l_extendedprice * (1 - l_discount)) as revenue,
       c_acctbal, 
       n_name, 
       c_address, 
       c_phone, 
       c_comment
FROM customer, orders, lineitem, nation
WHERE c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate >= date '1993-10-01'
  AND o_orderdate < date '1994-01-01'
  AND l_returnflag = 'R'
  AND c_nationkey = n_nationkey
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue desc
LIMIT 20;`

// GetQueries returns all TPC-H queries for benchmarking
func GetQueries() map[string]string {
	return map[string]string{
		"Q3":  Query3,
		"Q5":  Query5,
		"Q8":  Query8,
		"Q10": Query10,
	}
}

// GetQueryDescriptions returns descriptions of each query
func GetQueryDescriptions() map[string]string {
	return map[string]string{
		"Q3":  "Customer Market Segment - Find high-value unshipped orders",
		"Q5":  "Local Supplier Volume - Revenue through local suppliers by nation",
		"Q8":  "National Market Share - Market share changes over time",
		"Q10": "Returned Item Reporting - Customers with returned items",
	}
}