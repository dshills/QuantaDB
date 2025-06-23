-- Small test dataset for TPC-H tables
-- This is for testing queries without full data generation

-- Regions (5 rows)
INSERT INTO region VALUES (0, 'AFRICA', 'lar deposits');
INSERT INTO region VALUES (1, 'AMERICA', 'hs use ironic requests');
INSERT INTO region VALUES (2, 'ASIA', 'ges. thinly even beans');
INSERT INTO region VALUES (3, 'EUROPE', 'ly final courts cajole');
INSERT INTO region VALUES (4, 'MIDDLE EAST', 'uickly special accounts');

-- Nations (subset - 10 rows)
INSERT INTO nation VALUES (0, 'ALGERIA', 0, 'furiously regular requests');
INSERT INTO nation VALUES (1, 'ARGENTINA', 1, 'al foxes promise slyly');
INSERT INTO nation VALUES (2, 'BRAZIL', 1, 'y alongside of the pending deposits');
INSERT INTO nation VALUES (3, 'CANADA', 1, 'eas hang ironic, silent packages');
INSERT INTO nation VALUES (7, 'GERMANY', 3, 'l platelets. regular accounts');
INSERT INTO nation VALUES (8, 'INDIA', 2, 'ss excuses cajole slyly');
INSERT INTO nation VALUES (9, 'INDONESIA', 2, 'slyly express asymptotes');
INSERT INTO nation VALUES (12, 'JAPAN', 2, 'ously. final, express gifts');
INSERT INTO nation VALUES (23, 'UNITED KINGDOM', 3, 'eans boost carefully');
INSERT INTO nation VALUES (24, 'UNITED STATES', 1, 'y final packages. slow foxes');

-- Suppliers (10 rows)
INSERT INTO supplier VALUES (1, 'Supplier#000000001', '17 Cherry St', 0, '10-123-456-7890', 5755.94, 'quickly final');
INSERT INTO supplier VALUES (2, 'Supplier#000000002', '89 Oak Ave', 1, '11-234-567-8901', 3846.12, 'slyly regular');
INSERT INTO supplier VALUES (3, 'Supplier#000000003', '42 Maple Dr', 2, '12-345-678-9012', 4192.40, 'blithely bold');
INSERT INTO supplier VALUES (4, 'Supplier#000000004', '55 Pine Ln', 3, '13-456-789-0123', 9234.55, 'carefully ironic');
INSERT INTO supplier VALUES (5, 'Supplier#000000005', '66 Elm St', 7, '17-567-890-1234', 2156.78, 'furiously even');
INSERT INTO supplier VALUES (6, 'Supplier#000000006', '78 Birch Rd', 8, '18-678-901-2345', 7823.99, 'pending regular');
INSERT INTO supplier VALUES (7, 'Supplier#000000007', '91 Cedar Ct', 9, '19-789-012-3456', 5643.21, 'express deposits');
INSERT INTO supplier VALUES (8, 'Supplier#000000008', '23 Walnut Way', 12, '22-890-123-4567', 8912.44, 'silent foxes');
INSERT INTO supplier VALUES (9, 'Supplier#000000009', '34 Ash Blvd', 23, '33-901-234-5678', 1234.56, 'ironic packages');
INSERT INTO supplier VALUES (10, 'Supplier#000000010', '45 Spruce Pl', 24, '34-012-345-6789', 6789.01, 'final accounts');

-- Customers (20 rows)
INSERT INTO customer VALUES (1, 'Customer#000000001', '123 Main St', 0, '10-111-222-3333', 823.56, 'BUILDING', 'regular deposits');
INSERT INTO customer VALUES (2, 'Customer#000000002', '456 First Ave', 1, '11-222-333-4444', 1234.78, 'AUTOMOBILE', 'express packages');
INSERT INTO customer VALUES (3, 'Customer#000000003', '789 Second St', 2, '12-333-444-5555', 5678.90, 'BUILDING', 'pending accounts');
INSERT INTO customer VALUES (4, 'Customer#000000004', '321 Third Ave', 3, '13-444-555-6666', 3456.12, 'MACHINERY', 'furiously final');
INSERT INTO customer VALUES (5, 'Customer#000000005', '654 Fourth St', 7, '17-555-666-7777', 7890.34, 'HOUSEHOLD', 'slyly regular');
INSERT INTO customer VALUES (6, 'Customer#000000006', '987 Fifth Ave', 8, '18-666-777-8888', 2345.67, 'FURNITURE', 'carefully ironic');
INSERT INTO customer VALUES (7, 'Customer#000000007', '159 Sixth St', 9, '19-777-888-9999', 4567.89, 'BUILDING', 'blithely bold');
INSERT INTO customer VALUES (8, 'Customer#000000008', '753 Seventh Ave', 12, '22-888-999-0000', 6789.01, 'AUTOMOBILE', 'quickly final');
INSERT INTO customer VALUES (9, 'Customer#000000009', '951 Eighth St', 23, '33-999-000-1111', 8901.23, 'MACHINERY', 'express ideas');
INSERT INTO customer VALUES (10, 'Customer#000000010', '357 Ninth Ave', 24, '34-000-111-2222', 1234.56, 'BUILDING', 'pending foxes');
INSERT INTO customer VALUES (11, 'Customer#000000011', '246 Tenth St', 0, '10-123-234-3456', 3456.78, 'HOUSEHOLD', 'regular requests');
INSERT INTO customer VALUES (12, 'Customer#000000012', '135 Main Blvd', 1, '11-234-345-4567', 5678.90, 'FURNITURE', 'final deposits');
INSERT INTO customer VALUES (13, 'Customer#000000013', '864 Oak Dr', 2, '12-345-456-5678', 7890.12, 'BUILDING', 'ironic accounts');
INSERT INTO customer VALUES (14, 'Customer#000000014', '975 Pine Ln', 3, '13-456-567-6789', 9012.34, 'AUTOMOBILE', 'silent packages');
INSERT INTO customer VALUES (15, 'Customer#000000015', '286 Elm St', 7, '17-567-678-7890', 2345.67, 'MACHINERY', 'furiously even');
INSERT INTO customer VALUES (16, 'Customer#000000016', '397 Maple Ave', 8, '18-678-789-8901', 4567.89, 'BUILDING', 'slyly bold');
INSERT INTO customer VALUES (17, 'Customer#000000017', '408 Cedar Rd', 9, '19-789-890-9012', 6789.01, 'HOUSEHOLD', 'carefully final');
INSERT INTO customer VALUES (18, 'Customer#000000018', '519 Birch St', 12, '22-890-901-0123', 8901.23, 'FURNITURE', 'blithely regular');
INSERT INTO customer VALUES (19, 'Customer#000000019', '620 Walnut Dr', 23, '33-901-012-1234', 1234.56, 'BUILDING', 'express foxes');
INSERT INTO customer VALUES (20, 'Customer#000000020', '731 Ash Ln', 24, '34-012-123-2345', 3456.78, 'AUTOMOBILE', 'pending ideas');

-- Parts (10 rows)
INSERT INTO part VALUES (1, 'Part Alpha', 'Manufacturer#1', 'Brand#11', 'ECONOMY ANODIZED STEEL', 10, 'SM CASE', 901.00, 'final foxes');
INSERT INTO part VALUES (2, 'Part Beta', 'Manufacturer#2', 'Brand#22', 'STANDARD COPPER', 20, 'MD BOX', 902.00, 'express deposits');
INSERT INTO part VALUES (3, 'Part Gamma', 'Manufacturer#3', 'Brand#33', 'PROMO BRASS', 30, 'LG BAG', 903.00, 'regular accounts');
INSERT INTO part VALUES (4, 'Part Delta', 'Manufacturer#4', 'Brand#44', 'ECONOMY STEEL', 15, 'SM JAR', 904.00, 'silent packages');
INSERT INTO part VALUES (5, 'Part Epsilon', 'Manufacturer#5', 'Brand#55', 'STANDARD ALUMINUM', 25, 'MD CASE', 905.00, 'pending ideas');
INSERT INTO part VALUES (6, 'Part Zeta', 'Manufacturer#1', 'Brand#12', 'ECONOMY ANODIZED COPPER', 35, 'LG BOX', 906.00, 'furiously final');
INSERT INTO part VALUES (7, 'Part Eta', 'Manufacturer#2', 'Brand#23', 'PROMO STEEL', 12, 'SM BAG', 907.00, 'carefully ironic');
INSERT INTO part VALUES (8, 'Part Theta', 'Manufacturer#3', 'Brand#34', 'STANDARD BRASS', 22, 'MD JAR', 908.00, 'slyly regular');
INSERT INTO part VALUES (9, 'Part Iota', 'Manufacturer#4', 'Brand#45', 'ECONOMY ALUMINUM', 32, 'LG CASE', 909.00, 'blithely bold');
INSERT INTO part VALUES (10, 'Part Kappa', 'Manufacturer#5', 'Brand#51', 'PROMO COPPER', 18, 'SM BOX', 910.00, 'quickly express');

-- Orders (30 rows with dates)
INSERT INTO orders VALUES (1, 1, 'O', 15234.56, '1995-01-15', '1-URGENT', 'Clerk#000000001', 0, 'final deposits');
INSERT INTO orders VALUES (2, 2, 'F', 23456.78, '1994-12-01', '2-HIGH', 'Clerk#000000002', 0, 'express foxes');
INSERT INTO orders VALUES (3, 3, 'O', 34567.89, '1995-03-20', '3-MEDIUM', 'Clerk#000000003', 0, 'pending accounts');
INSERT INTO orders VALUES (4, 4, 'F', 12345.67, '1994-06-15', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'regular ideas');
INSERT INTO orders VALUES (5, 5, 'O', 45678.90, '1995-02-28', '5-LOW', 'Clerk#000000005', 0, 'silent packages');
INSERT INTO orders VALUES (6, 6, 'F', 23456.78, '1994-11-10', '1-URGENT', 'Clerk#000000001', 0, 'furiously final');
INSERT INTO orders VALUES (7, 7, 'O', 34567.89, '1995-04-05', '2-HIGH', 'Clerk#000000002', 0, 'carefully regular');
INSERT INTO orders VALUES (8, 8, 'F', 45678.90, '1993-10-15', '3-MEDIUM', 'Clerk#000000003', 0, 'slyly express');
INSERT INTO orders VALUES (9, 9, 'O', 56789.01, '1993-11-20', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'blithely bold');
INSERT INTO orders VALUES (10, 10, 'F', 12345.67, '1993-12-25', '5-LOW', 'Clerk#000000005', 0, 'quickly ironic');
INSERT INTO orders VALUES (11, 11, 'O', 23456.78, '1995-01-30', '1-URGENT', 'Clerk#000000001', 0, 'express deposits');
INSERT INTO orders VALUES (12, 12, 'F', 34567.89, '1994-07-20', '2-HIGH', 'Clerk#000000002', 0, 'pending foxes');
INSERT INTO orders VALUES (13, 13, 'O', 45678.90, '1995-03-10', '3-MEDIUM', 'Clerk#000000003', 0, 'regular accounts');
INSERT INTO orders VALUES (14, 14, 'F', 56789.01, '1994-09-05', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'silent ideas');
INSERT INTO orders VALUES (15, 15, 'O', 67890.12, '1995-02-14', '5-LOW', 'Clerk#000000005', 0, 'furiously even');
INSERT INTO orders VALUES (16, 16, 'F', 12345.67, '1994-08-30', '1-URGENT', 'Clerk#000000001', 0, 'carefully final');
INSERT INTO orders VALUES (17, 17, 'O', 23456.78, '1995-04-15', '2-HIGH', 'Clerk#000000002', 0, 'slyly regular');
INSERT INTO orders VALUES (18, 18, 'F', 34567.89, '1993-11-01', '3-MEDIUM', 'Clerk#000000003', 0, 'blithely express');
INSERT INTO orders VALUES (19, 19, 'O', 45678.90, '1993-10-20', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'quickly bold');
INSERT INTO orders VALUES (20, 20, 'F', 56789.01, '1993-12-10', '5-LOW', 'Clerk#000000005', 0, 'express packages');
INSERT INTO orders VALUES (21, 1, 'O', 12345.67, '1994-02-15', '1-URGENT', 'Clerk#000000001', 0, 'pending deposits');
INSERT INTO orders VALUES (22, 2, 'F', 23456.78, '1994-05-20', '2-HIGH', 'Clerk#000000002', 0, 'regular foxes');
INSERT INTO orders VALUES (23, 3, 'O', 34567.89, '1994-08-25', '3-MEDIUM', 'Clerk#000000003', 0, 'silent accounts');
INSERT INTO orders VALUES (24, 4, 'F', 45678.90, '1994-11-30', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'furiously ironic');
INSERT INTO orders VALUES (25, 5, 'O', 56789.01, '1995-01-05', '5-LOW', 'Clerk#000000005', 0, 'carefully express');
INSERT INTO orders VALUES (26, 7, 'F', 12345.67, '1994-03-10', '1-URGENT', 'Clerk#000000001', 0, 'slyly final');
INSERT INTO orders VALUES (27, 8, 'O', 23456.78, '1994-06-15', '2-HIGH', 'Clerk#000000002', 0, 'blithely regular');
INSERT INTO orders VALUES (28, 9, 'F', 34567.89, '1994-09-20', '3-MEDIUM', 'Clerk#000000003', 0, 'quickly even');
INSERT INTO orders VALUES (29, 10, 'O', 45678.90, '1994-12-25', '4-NOT SPECIFIED', 'Clerk#000000004', 0, 'express ideas');
INSERT INTO orders VALUES (30, 1, 'F', 56789.01, '1995-03-30', '5-LOW', 'Clerk#000000005', 0, 'pending packages');

-- Lineitem (sample rows - normally 4-7 per order)
-- Order 1
INSERT INTO lineitem VALUES (1, 1, 1, 1, 17.00, 15234.56, 0.04, 0.02, 'N', 'O', '1995-01-20', '1995-02-15', '1995-01-25', 'NONE', 'TRUCK', 'express foxes');
INSERT INTO lineitem VALUES (1, 2, 2, 2, 10.00, 9020.00, 0.02, 0.01, 'N', 'O', '1995-01-22', '1995-02-17', '1995-01-27', 'COLLECT COD', 'RAIL', 'pending deposits');

-- Order 2
INSERT INTO lineitem VALUES (2, 3, 3, 1, 25.00, 22575.00, 0.05, 0.03, 'N', 'F', '1994-12-05', '1995-01-01', '1994-12-10', 'DELIVER IN PERSON', 'AIR', 'regular accounts');
INSERT INTO lineitem VALUES (2, 4, 4, 2, 15.00, 13560.00, 0.03, 0.02, 'N', 'F', '1994-12-07', '1995-01-03', '1994-12-12', 'TAKE BACK RETURN', 'SHIP', 'silent packages');

-- Order 8 (with returned items)
INSERT INTO lineitem VALUES (8, 5, 5, 1, 30.00, 27150.00, 0.06, 0.04, 'R', 'F', '1993-10-20', '1993-11-15', '1993-10-25', 'NONE', 'MAIL', 'furiously final');
INSERT INTO lineitem VALUES (8, 6, 6, 2, 20.00, 18120.00, 0.04, 0.02, 'R', 'F', '1993-10-22', '1993-11-17', '1993-10-27', 'COLLECT COD', 'FOB', 'carefully ironic');

-- Order 9 (with returned items)
INSERT INTO lineitem VALUES (9, 7, 7, 1, 12.00, 10884.00, 0.02, 0.01, 'R', 'O', '1993-11-25', '1993-12-20', '1993-11-30', 'DELIVER IN PERSON', 'REG AIR', 'slyly regular');

-- Order 10 (with returned items)
INSERT INTO lineitem VALUES (10, 8, 8, 1, 22.00, 19976.00, 0.03, 0.02, 'R', 'F', '1993-12-30', '1994-01-25', '1994-01-05', 'TAKE BACK RETURN', 'TRUCK', 'blithely bold');

-- More sample rows for other orders
INSERT INTO lineitem VALUES (3, 9, 9, 1, 18.00, 16362.00, 0.05, 0.03, 'N', 'O', '1995-03-25', '1995-04-20', '1995-03-30', 'NONE', 'RAIL', 'quickly express');
INSERT INTO lineitem VALUES (4, 10, 10, 1, 14.00, 12740.00, 0.02, 0.01, 'N', 'F', '1994-06-20', '1994-07-15', '1994-06-25', 'COLLECT COD', 'AIR', 'express ideas');
INSERT INTO lineitem VALUES (5, 1, 2, 1, 28.00, 25228.00, 0.04, 0.02, 'N', 'O', '1995-03-05', '1995-03-30', '1995-03-10', 'DELIVER IN PERSON', 'SHIP', 'pending foxes');
INSERT INTO lineitem VALUES (6, 2, 3, 1, 16.00, 14432.00, 0.03, 0.02, 'N', 'F', '1994-11-15', '1994-12-10', '1994-11-20', 'TAKE BACK RETURN', 'MAIL', 'regular deposits');
INSERT INTO lineitem VALUES (7, 3, 4, 1, 20.00, 18060.00, 0.05, 0.03, 'N', 'O', '1995-04-10', '1995-05-05', '1995-04-15', 'NONE', 'FOB', 'silent accounts');

-- Partsupp (4 suppliers per part)
INSERT INTO partsupp VALUES (1, 1, 100, 50.00, 'quickly express');
INSERT INTO partsupp VALUES (1, 3, 200, 52.00, 'express foxes');
INSERT INTO partsupp VALUES (1, 5, 150, 48.00, 'pending deposits');
INSERT INTO partsupp VALUES (1, 7, 175, 51.00, 'regular accounts');
INSERT INTO partsupp VALUES (2, 2, 125, 60.00, 'silent packages');
INSERT INTO partsupp VALUES (2, 4, 225, 62.00, 'furiously final');
INSERT INTO partsupp VALUES (2, 6, 175, 58.00, 'carefully ironic');
INSERT INTO partsupp VALUES (2, 8, 200, 61.00, 'slyly regular');
INSERT INTO partsupp VALUES (3, 3, 150, 70.00, 'blithely bold');
INSERT INTO partsupp VALUES (3, 5, 250, 72.00, 'quickly even');