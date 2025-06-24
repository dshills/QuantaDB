-- Test LIKE operator
SELECT p_type FROM part WHERE p_type LIKE 'PROMO%' LIMIT 5;
SELECT COUNT(*) FROM part WHERE p_type LIKE 'PROMO%';
SELECT p_type FROM part WHERE p_type LIKE '%PROMO%' LIMIT 5;