-- Test SUBSTRING function
SELECT 
    c_phone,
    SUBSTRING(c_phone FROM 1 FOR 3) AS country_code
FROM customer
LIMIT 5;

-- Test SUBSTRING without length
SELECT 
    c_phone,
    SUBSTRING(c_phone FROM 4) AS phone_without_code
FROM customer
LIMIT 5;