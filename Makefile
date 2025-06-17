.PHONY: all build test clean fmt vet lint install

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
	$(GOTEST) -v -cover ./...

test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

bench:
	@echo "Running benchmarks..."
	$(GOTEST) -bench=. -benchmem ./...

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

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

# Help
help:
	@echo "Available targets:"
	@echo "  make build        - Build both server and CLI"
	@echo "  make build-server - Build only the server"
	@echo "  make build-ctl    - Build only the CLI"
	@echo "  make test         - Run tests"
	@echo "  make test-coverage- Run tests with coverage report"
	@echo "  make bench        - Run benchmarks"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make fmt          - Format code"
	@echo "  make vet          - Run go vet"
	@echo "  make lint         - Run linter"
	@echo "  make install      - Install binaries to GOPATH/bin"
	@echo "  make run          - Build and run the server"
	@echo "  make dev-deps     - Install development dependencies"