.PHONY: all build test test-verbose clean fmt vet lint install

# Build variables
BINARY_NAME=quantadb
CTL_NAME=quantactl
VERSION=$(shell git describe --tags --always --dirty)
COMMIT=$(shell git rev-parse --short HEAD)
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.commit=${COMMIT}"

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOFMT=$(GOCMD) fmt
GOVET=$(GOCMD) vet

# Directories
BUILD_DIR=./build
CMD_DIR=./cmd
INTERNAL_DIR=./internal
PKG_DIR=./pkg

all: build

build: build-server build-ctl

build-server:
	@echo "Building QuantaDB server..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)/quantadb

build-ctl:
	@echo "Building QuantaDB CLI..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(CTL_NAME) $(CMD_DIR)/quantactl

test:
	@echo "Running tests..."
	@$(GOTEST) -cover ./... || (echo "TESTS FAILED" && exit 1)
	@echo "All tests passed!"

test-verbose:
	@echo "Running tests (verbose)..."
	$(GOTEST) -v -cover ./...

test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Race detection tests
test-race:
	@echo "Running tests with race detector..."
	$(GOTEST) -race -timeout 10m ./...

# Vectorized execution specific tests
test-vectorized:
	@echo "Running vectorized execution tests..."
	$(GOTEST) -v -run "TestVectorized" ./internal/sql/executor/...

test-vectorized-race:
	@echo "Running vectorized tests with race detector..."
	$(GOTEST) -race -timeout 10m -run "TestVectorized" ./internal/sql/executor/...

# Benchmarks
bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

bench-vectorized:
	@echo "Running vectorized execution benchmarks..."
	$(GOTEST) -bench="BenchmarkVectorized" -benchmem -benchtime=10s ./internal/sql/executor/...

bench-vectorized-race:
	@echo "Running vectorized benchmarks with race detector..."
	$(GOTEST) -bench="BenchmarkVectorized" -benchmem -benchtime=10s -race ./internal/sql/executor/...

# Memory profiling
bench-vectorized-mem:
	@echo "Running vectorized benchmarks with memory profiling..."
	$(GOTEST) -bench="BenchmarkVectorized" -benchmem -memprofile=vectorized_mem.prof ./internal/sql/executor/...
	@echo "Memory profile saved to vectorized_mem.prof"
	@echo "View with: go tool pprof vectorized_mem.prof"

# CPU profiling
bench-vectorized-cpu:
	@echo "Running vectorized benchmarks with CPU profiling..."
	$(GOTEST) -bench="BenchmarkVectorized" -benchmem -cpuprofile=vectorized_cpu.prof ./internal/sql/executor/...
	@echo "CPU profile saved to vectorized_cpu.prof"
	@echo "View with: go tool pprof vectorized_cpu.prof"

# Performance regression detection
bench-compare:
	@echo "Running benchmark comparison..."
	@mkdir -p benchmarks
	@if [ ! -f benchmarks/baseline.txt ]; then \
		echo "Creating baseline benchmark..."; \
		$(GOTEST) -bench="BenchmarkVectorized" -benchmem -benchtime=10s ./internal/sql/executor/... > benchmarks/baseline.txt; \
		echo "Baseline created at benchmarks/baseline.txt"; \
	else \
		echo "Running current benchmarks..."; \
		$(GOTEST) -bench="BenchmarkVectorized" -benchmem -benchtime=10s ./internal/sql/executor/... > benchmarks/current.txt; \
		echo "Comparing results..."; \
		@which benchstat > /dev/null || (echo "benchstat not installed. Run: go install golang.org/x/perf/cmd/benchstat@latest" && exit 1); \
		benchstat benchmarks/baseline.txt benchmarks/current.txt; \
	fi

# Update baseline benchmarks
bench-update-baseline:
	@echo "Updating baseline benchmarks..."
	@mkdir -p benchmarks
	$(GOTEST) -bench="BenchmarkVectorized" -benchmem -benchtime=10s ./internal/sql/executor/... > benchmarks/baseline.txt
	@echo "Baseline updated at benchmarks/baseline.txt"

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -f *.prof
	rm -rf benchmarks/current.txt

fmt:
	@echo "Formatting code..."
	$(GOFMT) ./...

vet:
	@echo "Vetting code..."
	$(GOVET) ./...

lint:
	@echo "Linting code..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run

install: build
	@echo "Installing..."
	cp $(BUILD_DIR)/$(BINARY_NAME) $(GOPATH)/bin/
	cp $(BUILD_DIR)/$(CTL_NAME) $(GOPATH)/bin/

run: build-server
	@echo "Running QuantaDB server..."
	$(BUILD_DIR)/$(BINARY_NAME)

# Development helpers
dev-deps:
	@echo "Installing development dependencies..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest
	go install golang.org/x/perf/cmd/benchstat@latest

# CI/CD targets
ci-test: lint vet test-race test-vectorized-race

ci-bench: bench-vectorized bench-compare

# Help
help:
	@echo "Available targets:"
	@echo "  make build         - Build both server and CLI"
	@echo "  make build-server  - Build only the server"
	@echo "  make build-ctl     - Build only the CLI"
	@echo "  make test          - Run tests (quiet, shows only failures)"
	@echo "  make test-verbose  - Run tests with detailed output"
	@echo "  make test-coverage - Run tests with coverage report"
	@echo "  make test-race     - Run tests with race detector"
	@echo ""
	@echo "Vectorized Execution Testing:"
	@echo "  make test-vectorized      - Run vectorized execution tests"
	@echo "  make test-vectorized-race - Run vectorized tests with race detector"
	@echo ""
	@echo "Benchmarks:"
	@echo "  make bench                - Run all benchmarks"
	@echo "  make bench-vectorized     - Run vectorized execution benchmarks"
	@echo "  make bench-vectorized-race- Run vectorized benchmarks with race detector"
	@echo "  make bench-vectorized-mem - Run with memory profiling"
	@echo "  make bench-vectorized-cpu - Run with CPU profiling"
	@echo "  make bench-compare        - Compare benchmarks against baseline"
	@echo "  make bench-update-baseline- Update baseline benchmarks"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make fmt           - Format code"
	@echo "  make vet           - Run go vet"
	@echo "  make lint          - Run linter"
	@echo "  make install       - Install binaries to GOPATH/bin"
	@echo "  make run           - Build and run the server"
	@echo "  make dev-deps      - Install development dependencies"
	@echo ""
	@echo "CI/CD:"
	@echo "  make ci-test       - Run all CI tests (lint, vet, race)"
	@echo "  make ci-bench      - Run CI benchmarks with comparison"