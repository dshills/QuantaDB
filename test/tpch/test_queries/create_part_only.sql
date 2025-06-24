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