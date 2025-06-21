-- Simple SQL test for QuantaDB
-- No parameters, no subqueries, just basic SQL

-- Create a simple table
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(50),
    value INTEGER
);

-- Insert some data
INSERT INTO test_table (id, name, value) VALUES (1, 'Alice', 100);
INSERT INTO test_table (id, name, value) VALUES (2, 'Bob', 200);
INSERT INTO test_table (id, name, value) VALUES (3, 'Charlie', 300);
INSERT INTO test_table (id, name, value) VALUES (4, 'David', 400);
INSERT INTO test_table (id, name, value) VALUES (5, 'Eve', 500);

-- Simple queries
SELECT * FROM test_table;
SELECT COUNT(*) FROM test_table;
SELECT * FROM test_table WHERE value > 250;
SELECT name, value FROM test_table ORDER BY value DESC;

-- Update
UPDATE test_table SET value = 600 WHERE id = 5;

-- Delete
DELETE FROM test_table WHERE id = 1;

-- Final check
SELECT * FROM test_table ORDER BY id;

-- Cleanup
DROP TABLE test_table;