#!/bin/bash

# Start QuantaDB cluster test
echo "Starting QuantaDB cluster test..."

# Clean up previous data
rm -rf ./data/primary ./data/replica

# Build the server
echo "Building server..."
make build

# Start primary node
echo "Starting primary node..."
./build/quantadb -config examples/cluster-primary.json &
PRIMARY_PID=$!
echo "Primary node started with PID: $PRIMARY_PID"

# Wait for primary to start
sleep 5

# Start replica node
echo "Starting replica node..."
./build/quantadb -config examples/cluster-replica.json &
REPLICA_PID=$!
echo "Replica node started with PID: $REPLICA_PID"

echo ""
echo "Cluster started!"
echo "Primary: localhost:5432 (PID: $PRIMARY_PID)"
echo "Replica: localhost:5433 (PID: $REPLICA_PID)"
echo ""
echo "To stop the cluster, run:"
echo "  kill $PRIMARY_PID $REPLICA_PID"
echo ""
echo "To connect:"
echo "  psql -h localhost -p 5432 -U postgres -d quantadb  # Primary"
echo "  psql -h localhost -p 5433 -U postgres -d quantadb  # Replica"

# Wait for interrupt
wait