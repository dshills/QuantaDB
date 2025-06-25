# Constraints Implementation in QuantaDB

## Overview

QuantaDB implements a comprehensive constraint system supporting foreign keys and CHECK constraints with full SQL standard compliance.

## Foreign Key Constraints

### Supported Features

1. **Referential Actions**:
   - `CASCADE`: Automatically deletes referencing rows when the referenced row is deleted
   - `SET NULL`: Sets foreign key columns to NULL when the referenced row is deleted
   - `SET DEFAULT`: Sets foreign key columns to their default values when the referenced row is deleted
   - `RESTRICT`/`NO ACTION`: Prevents deletion of rows that are referenced (default behavior)

2. **Multi-column Foreign Keys**: Supports composite foreign keys across multiple columns

3. **NULL Handling**: NULL values in foreign key columns are allowed and don't require a matching row

### Implementation Details

- **CascadeDeleteHandler** (`internal/sql/executor/cascade_handler.go`): Handles all cascade operations
- **SimpleConstraintValidator** (`internal/sql/executor/constraints.go`): Validates foreign key constraints during DML operations
- Cascade operations are performed recursively to handle multi-level relationships
- Circular reference detection prevents infinite loops

### Example Usage

```sql
-- Create parent table
CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Create child table with CASCADE DELETE
CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    dept_id INTEGER,
    FOREIGN KEY (dept_id) REFERENCES departments (id) ON DELETE CASCADE
);

-- When a department is deleted, all its employees are automatically deleted
DELETE FROM departments WHERE id = 1;
```

## CHECK Constraints

### Supported Features

1. **Expression Types**:
   - Comparison operators: `>`, `<`, `>=`, `<=`, `=`, `!=`
   - Logical operators: `AND`, `OR`, `NOT`
   - `IS NULL` / `IS NOT NULL`
   - `BETWEEN` / `NOT BETWEEN`
   - `IN` / `NOT IN`
   - Parentheses for grouping

2. **String Functions**:
   - `LENGTH(str)`: Returns string length
   - `UPPER(str)`: Converts to uppercase
   - `LOWER(str)`: Converts to lowercase
   - `SUBSTRING(str, start, length)`: Extracts substring
   - `TRIM(str)`, `LTRIM(str)`, `RTRIM(str)`: Removes whitespace
   - `CONCAT(str1, str2, ...)`: Concatenates strings

3. **Numeric Functions**:
   - `ABS(n)`: Absolute value
   - `ROUND(n, precision)`: Round to precision
   - `FLOOR(n)`, `CEIL(n)`: Floor and ceiling
   - `MOD(a, b)`: Modulo operation
   - `POWER(base, exp)`: Exponentiation
   - `SQRT(n)`: Square root

4. **Conditional Functions**:
   - `COALESCE(val1, val2, ...)`: Returns first non-NULL value
   - `NULLIF(val1, val2)`: Returns NULL if values are equal

### Implementation Details

- **Expression Caching**: Parsed expressions are cached to avoid re-parsing on every validation
- **Type Coercion**: Automatic type conversion between numeric types (int64 ↔ float64)
- **Parser Integration**: CHECK expressions are parsed during CREATE TABLE and stored as strings
- **Evaluation**: Expressions are evaluated against row data during INSERT/UPDATE operations

### Example Usage

```sql
-- Create table with CHECK constraints
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2),
    discount DECIMAL(3,2),
    status VARCHAR(20),
    CHECK (price > 0),
    CHECK (discount >= 0 AND discount < 1),
    CHECK (LENGTH(name) > 0),
    CHECK (status IN ('active', 'inactive', 'pending'))
);

-- Valid insert
INSERT INTO products (id, name, price, discount, status) 
VALUES (1, 'Widget', 19.99, 0.10, 'active');

-- Invalid insert (price <= 0)
INSERT INTO products (id, name, price, discount, status) 
VALUES (2, 'Gadget', -5.00, 0.10, 'active');  -- ERROR: check constraint violated
```

## Performance Considerations

1. **Expression Caching**: CHECK constraint expressions are parsed once and cached, significantly improving performance for repeated validations

2. **Type Coercion**: The system automatically handles mixed numeric type comparisons without requiring explicit casts

3. **Cascade Operations**: Currently use table scans; future optimization could use indexes on foreign key columns

4. **Constraint Validation Order**: Constraints are validated in the order they're defined, allowing early exit on first violation

## Testing

Comprehensive test suites ensure constraint reliability:

- `foreign_key_test.go`: Basic foreign key validation tests
- `cascade_delete_test.go`: Tests for all referential actions
- `check_constraint_test.go`: Comprehensive CHECK constraint expression tests

## Future Enhancements

1. **Index Usage**: Optimize foreign key lookups using indexes on referenced columns
2. **Deferred Constraints**: Support for deferring constraint checking until transaction commit
3. **Column-level CHECK Constraints**: Currently only table-level constraints are supported
4. **Constraint Names**: Better error messages using constraint names
5. **ALTER TABLE**: Support for adding/dropping constraints on existing tables