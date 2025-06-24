-- Create missing tables for Q5

CREATE TABLE supplier (
    s_suppkey INTEGER PRIMARY KEY,
    s_name TEXT NOT NULL,
    s_address TEXT NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone TEXT NOT NULL,
    s_acctbal DECIMAL(10,2) NOT NULL,
    s_comment TEXT
);

CREATE TABLE nation (
    n_nationkey INTEGER PRIMARY KEY,
    n_name TEXT NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment TEXT
);

CREATE TABLE region (
    r_regionkey INTEGER PRIMARY KEY,
    r_name TEXT NOT NULL,
    r_comment TEXT
);