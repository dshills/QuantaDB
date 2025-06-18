#!/bin/bash

# Start server
echo "Starting QuantaDB server..."
./build/quantadb --data ./test-data --port 5433 --log-level debug &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test with psql
echo "Testing CREATE TABLE..."
/opt/homebrew/opt/postgresql@16/bin/psql -h localhost -p 5433 -U user -d database -c "CREATE TABLE test (id INTEGER);" 2>&1

# Kill server
kill $SERVER_PID 2>/dev/null

echo "Test complete"