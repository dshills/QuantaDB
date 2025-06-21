#!/bin/bash

# Simple script to load test data into QuantaDB
# Since QuantaDB doesn't have a standard client yet, we'll use netcat to send SQL

HOST=localhost
PORT=5432

# Function to send SQL command
send_sql() {
    echo "Executing: $1"
    # For now, just echo the commands - we need a proper client
    echo "$1"
}

echo "Loading TPC-H test data..."

# First, let's just try the schema
send_sql "CREATE TABLE region (
    r_regionkey INTEGER NOT NULL,
    r_name TEXT NOT NULL,
    r_comment TEXT
);"

send_sql "CREATE TABLE nation (
    n_nationkey INTEGER NOT NULL,
    n_name TEXT NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment TEXT
);"

send_sql "INSERT INTO region VALUES (0, 'AFRICA', 'lar deposits');"
send_sql "INSERT INTO region VALUES (1, 'AMERICA', 'hs use ironic requests');"

echo "Test data commands generated."