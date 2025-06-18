#!/bin/bash

# Test PostgreSQL client connection to QuantaDB

PORT=5432
HOST=localhost

echo "Testing connection to QuantaDB at $HOST:$PORT"

# Simple query test
echo "SELECT 1;" | psql -h $HOST -p $PORT -U test -d test -t -A

# Test basic table operations
cat << EOF | psql -h $HOST -p $PORT -U test -d test
\dt
SELECT * FROM users;
EOF