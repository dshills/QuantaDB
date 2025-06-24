CREATE TABLE supplier (
    s_suppkey INTEGER PRIMARY KEY,
    s_name TEXT NOT NULL,
    s_address TEXT NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone TEXT NOT NULL,
    s_acctbal DECIMAL(10,2) NOT NULL,
    s_comment TEXT
);
