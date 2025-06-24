-- Create tables needed for Q14
CREATE TABLE part (
    p_partkey INTEGER PRIMARY KEY,
    p_name TEXT NOT NULL,
    p_mfgr TEXT NOT NULL,
    p_brand TEXT NOT NULL,
    p_type TEXT NOT NULL,
    p_size INTEGER NOT NULL,
    p_container TEXT NOT NULL,
    p_retailprice DECIMAL(10,2) NOT NULL,
    p_comment TEXT
);

CREATE TABLE lineitem (
    l_orderkey INTEGER NOT NULL,
    l_partkey INTEGER NOT NULL,
    l_suppkey INTEGER NOT NULL,
    l_linenumber INTEGER NOT NULL,
    l_quantity DECIMAL(10,2) NOT NULL,
    l_extendedprice DECIMAL(10,2) NOT NULL,
    l_discount DECIMAL(3,2) NOT NULL,
    l_tax DECIMAL(3,2) NOT NULL,
    l_returnflag CHAR(1) NOT NULL,
    l_linestatus CHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct TEXT NOT NULL,
    l_shipmode TEXT NOT NULL,
    l_comment TEXT
);