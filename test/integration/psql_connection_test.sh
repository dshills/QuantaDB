#!/bin/bash
# Integration test for PostgreSQL client connection

# Start QuantaDB server in background
echo "Starting QuantaDB server..."
./build/quantadb --data ./data --port 5433 &
SERVER_PID=$!

# Wait for server to start
sleep 2

# Test psql connection
echo "Testing psql connection..."
psql -h localhost -p 5433 -U postgres -d quantadb -c "SELECT 1;" 2>&1

RESULT=$?

# Kill the server
kill $SERVER_PID 2>/dev/null

if [ $RESULT -eq 0 ]; then
    echo "✓ psql connection successful!"
    exit 0
else
    echo "✗ psql connection failed!"
    exit 1
fi