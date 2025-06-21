# TPC-H Benchmarks for QuantaDB

This directory contains TPC-H benchmark implementations for testing QuantaDB's query optimization and performance.

## Overview

TPC-H is an industry-standard decision support benchmark that consists of a suite of business-oriented ad-hoc queries. We've implemented queries 3, 5, 8, and 10 which test:

- **Query 3**: Customer Market Segment - Join performance, aggregation, sorting
- **Query 5**: Local Supplier Volume - Multi-way joins, aggregation
- **Query 8**: National Market Share - Complex joins, subqueries, date functions
- **Query 10**: Returned Item Reporting - Outer joins, aggregation

## Directory Structure

```
test/tpch/
├── schema.sql          # TPC-H table definitions
├── generator.go        # Data generation logic
├── queries.go          # TPC-H query definitions
├── benchmark.go        # Benchmark runner
├── cmd/
│   ├── generate/       # Data generation tool
│   └── benchmark/      # Benchmark execution tool
└── data/              # Generated data files (gitignored)
```

## Usage

### 1. Generate Test Data

Generate TPC-H data at scale factor 0.01 (1% of full size):

```bash
go run cmd/generate/main.go -sf 0.01 -output ./data
```

Options:
- `-sf`: Scale factor (0.01 = ~60MB, 0.1 = ~600MB, 1.0 = ~6GB)
- `-output`: Output directory for SQL files
- `-combined`: Generate single combined file instead of per-table files

### 2. Load Schema and Data

First, create the schema:

```bash
psql -h localhost -p 5432 -U postgres -d quantadb -f schema.sql
```

Then load the generated data:

```bash
psql -h localhost -p 5432 -U postgres -d quantadb -f data/region.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/nation.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/supplier.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/customer.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/part.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/partsupp.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/orders.sql
psql -h localhost -p 5432 -U postgres -d quantadb -f data/lineitem.sql
```

### 3. Run Benchmarks

Run the TPC-H benchmark suite:

```bash
go run cmd/benchmark/main.go -host localhost -port 5432 -dbname quantadb
```

Options:
- `-host`: Database host (default: localhost)
- `-port`: Database port (default: 5432)
- `-user`: Database user (default: postgres)
- `-password`: Database password
- `-dbname`: Database name (default: quantadb)
- `-warmup`: Run warmup queries first (default: true)
- `-report`: Output report to file instead of stdout

## Data Sizes

Approximate data sizes by scale factor:

| Scale Factor | Database Size | Row Count     |
|-------------|--------------|---------------|
| 0.01        | ~60 MB       | ~86K rows     |
| 0.1         | ~600 MB      | ~860K rows    |
| 1.0         | ~6 GB        | ~8.6M rows    |

## Expected Results

The benchmarks measure:
- Query execution time
- Row counts returned
- Optimizer effectiveness

Results are compared before and after optimizer improvements to measure performance gains.

## Notes

- The data generator creates TPC-H compliant data with appropriate distributions
- Indexes are created on common join columns to test index usage
- Statistics should be updated (ANALYZE) after data loading
- QuantaDB currently doesn't support all SQL features (e.g., EXTRACT, CASE), so some queries may need modification