#!/bin/bash

echo "=== TPC-H Benchmark for QuantaDB ==="
echo ""

# Clean existing data
echo "0. Cleaning existing data..."
rm -rf data/*.sql

# Generate TPC-H data
echo "1. Generating TPC-H data (scale factor 0.01)..."
go run cmd/generate/main.go -sf 0.01

# Load schema
echo ""
echo "2. Loading TPC-H schema..."
go run tools/load_sql.go -file schema.sql -host 127.0.0.1

# Load each data file in order
echo ""
echo "3. Loading TPC-H data..."
echo "  Loading region..."
go run tools/load_sql.go -file data/region.sql -host 127.0.0.1
echo "  Loading nation..."
go run tools/load_sql.go -file data/nation.sql -host 127.0.0.1
echo "  Loading supplier..."
go run tools/load_sql.go -file data/supplier.sql -host 127.0.0.1
echo "  Loading customer..."
go run tools/load_sql.go -file data/customer.sql -host 127.0.0.1
echo "  Loading part..."
go run tools/load_sql.go -file data/part.sql -host 127.0.0.1
echo "  Loading partsupp..."
go run tools/load_sql.go -file data/partsupp.sql -host 127.0.0.1
echo "  Loading orders..."
go run tools/load_sql.go -file data/orders.sql -host 127.0.0.1
echo "  Loading lineitem..."
go run tools/load_sql.go -file data/lineitem.sql -host 127.0.0.1

# Run benchmark
echo ""
echo "4. Running TPC-H benchmark..."
go run cmd/benchmark/main.go -sf 0.01 -host 127.0.0.1

echo ""
echo "=== Benchmark Complete ==="