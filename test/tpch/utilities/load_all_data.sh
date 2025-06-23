#!/bin/bash

# Load all TPC-H data in correct order
echo "Loading TPC-H data..."

# Load dimension tables first
echo "Loading region..."
go run ../tools/load_sql.go -file ../data/region.sql

echo "Loading nation..."
go run ../tools/load_sql.go -file ../data/nation.sql

echo "Loading supplier..."
go run ../tools/load_sql.go -file ../data/supplier.sql

echo "Loading customer..."
go run ../tools/load_sql.go -file ../data/customer.sql

echo "Loading part..."
go run ../tools/load_sql.go -file ../data/part.sql

# Load fact tables
echo "Loading orders..."
go run ../tools/load_sql.go -file ../data/orders.sql

echo "Loading lineitem..."
go run ../tools/load_sql.go -file ../data/lineitem.sql

echo "Loading partsupp..."
go run ../tools/load_sql.go -file ../data/partsupp.sql

echo "Done loading all TPC-H data!"