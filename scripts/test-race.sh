#!/bin/bash

# Script to run concurrency tests with race detector
# This is part of Phase 5 Production Readiness

set -e

echo "Running QuantaDB concurrency tests with race detector..."
echo "=============================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test packages with concurrency-sensitive code
PACKAGES=(
    "./internal/sql/executor"
    "./internal/txn"
    "./internal/storage"
    "./internal/index"
    "./internal/wal"
    "./internal/cluster"
)

# Track results
FAILED_PACKAGES=()
PASSED_COUNT=0

# Run tests for each package
for pkg in "${PACKAGES[@]}"; do
    echo -e "\n${YELLOW}Testing package: $pkg${NC}"
    
    if go test -race -v -timeout 10m "$pkg" > /tmp/race-test-output.log 2>&1; then
        echo -e "${GREEN}✓ PASSED${NC} - No race conditions detected in $pkg"
        ((PASSED_COUNT++))
    else
        echo -e "${RED}✗ FAILED${NC} - Race conditions or test failures in $pkg"
        FAILED_PACKAGES+=("$pkg")
        echo "Output:"
        tail -n 50 /tmp/race-test-output.log
    fi
done

# Run specific vectorized execution tests
echo -e "\n${YELLOW}Running vectorized execution race tests...${NC}"
if go test -race -v -run "TestVectorized.*|TestConcurrent.*" ./internal/sql/executor -timeout 5m; then
    echo -e "${GREEN}✓ PASSED${NC} - Vectorized execution is race-free"
else
    echo -e "${RED}✗ FAILED${NC} - Race conditions detected in vectorized execution"
    FAILED_PACKAGES+=("vectorized")
fi

# Run cache invalidation concurrency test
echo -e "\n${YELLOW}Running cache invalidation concurrency test...${NC}"
if go test -race -v -run TestCacheInvalidationConcurrency ./internal/sql/executor -timeout 5m; then
    echo -e "${GREEN}✓ PASSED${NC} - Cache invalidation is thread-safe"
else
    echo -e "${RED}✗ FAILED${NC} - Race conditions in cache invalidation"
    FAILED_PACKAGES+=("cache-invalidation")
fi

# Summary
echo -e "\n=============================================="
echo "SUMMARY:"
echo -e "Passed: ${GREEN}${PASSED_COUNT}${NC} packages"
echo -e "Failed: ${RED}${#FAILED_PACKAGES[@]}${NC} packages"

if [ ${#FAILED_PACKAGES[@]} -gt 0 ]; then
    echo -e "\n${RED}Failed packages:${NC}"
    for pkg in "${FAILED_PACKAGES[@]}"; do
        echo "  - $pkg"
    done
    
    echo -e "\n${YELLOW}Action Required:${NC}"
    echo "1. Review the race detector output above"
    echo "2. Fix any data races found"
    echo "3. Re-run this script to verify fixes"
    exit 1
else
    echo -e "\n${GREEN}All tests passed with race detector enabled!${NC}"
    echo "The code is free from detectable race conditions."
fi

# Clean up
rm -f /tmp/race-test-output.log