#!/bin/bash

echo "=== TPC-H Benchmark for QuantaDB ==="
echo ""

# Generate TPC-H data
echo "1. Generating TPC-H data (scale factor 0.01)..."
go run cmd/generate/main.go -sf 0.01 -combined -output ./data

# Load schema
echo ""
echo "2. Loading TPC-H schema..."
go run cmd/load/main.go -schema schema.sql -data /dev/null -host 127.0.0.1

# Load data  
echo ""
echo "3. Loading TPC-H data..."
go run cmd/load/main.go -schema /dev/null -data data/tpch_data.sql -host 127.0.0.1

# Run benchmark
echo ""
echo "4. Running TPC-H benchmark..."
go run cmd/benchmark/main.go -sf 0.01 -host 127.0.0.1

echo ""
echo "=== Benchmark Complete ==="