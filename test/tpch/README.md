# TPC-H Test Suite for QuantaDB

This directory contains the TPC-H benchmark implementation for QuantaDB.

## Directory Structure

```
tpch/
├── data/           # TPC-H table data (scale factor 0.01)
├── queries/        # TPC-H query implementations (Q1, Q6)
├── schema/         # SQL scripts to create TPC-H tables
├── test_programs/  # Go programs for testing
├── test_queries/   # SQL test queries for validation
├── tools/          # Data loading and generation tools
└── utilities/      # Helper scripts and programs
```

## Quick Start

1. **Start QuantaDB server**:
   ```bash
   cd ../.. && make run
   ```

2. **Create tables**:
   ```bash
   cd test/tpch
   go run tools/load_sql.go -file schema/create_all_tables.sql
   ```

3. **Load data**:
   ```bash
   cd utilities && ./load_all_data.sh
   ```

4. **Run a query**:
   ```bash
   go run ../tools/load_sql.go -file ../queries/q6.sql
   ```

## TPC-H Query Status

| Query | Status | Notes |
|-------|--------|-------|
| Q1 | ⚠️ Partial | Type coercion issues |
| Q6 | ✅ Working | Fully functional |
| Others | 🚧 In Progress | See docs/tpch-feature-matrix.md |

## Data Generation

To regenerate data with different scale factors:
```bash
cd utilities
go run regenerate_data.go  # Uses scale factor 0.01
```

## Testing

Various test queries are available in `test_queries/` to validate functionality:
- `check_all_counts.sql` - Verify row counts
- `test_alias.sql` - Test table aliases
- `test_interval.sql` - Test date arithmetic
- etc.