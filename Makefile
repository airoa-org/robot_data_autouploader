# Makefile for robot_data_autouploader

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
GOFMT=gofmt

# Project info
BINARY_DAEMON=robot_data_daemon
BINARY_CLIENT=robot_data_client
PKG_CONFIG=github.com/airoa-org/robot_data_pipeline/autoloader/internal/config

# Build info
GIT_HASH=$(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(shell git branch --show-current 2>/dev/null || echo "unknown")
GIT_TAG=$(shell git describe --tags --exact-match 2>/dev/null || echo "")
GIT_REMOTE=$(shell git remote get-url origin 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

# Build flags
LDFLAGS=-s -w
LDFLAGS+=-X '$(PKG_CONFIG).BuildGitHash=$(GIT_HASH)'
LDFLAGS+=-X '$(PKG_CONFIG).BuildGitBranch=$(GIT_BRANCH)'
LDFLAGS+=-X '$(PKG_CONFIG).BuildGitTag=$(GIT_TAG)'
LDFLAGS+=-X '$(PKG_CONFIG).BuildGitRemote=$(GIT_REMOTE)'
LDFLAGS+=-X '$(PKG_CONFIG).BuildTime=$(BUILD_TIME)'

# Default target
.PHONY: all
all: build

# Build targets
.PHONY: build
build: build-daemon build-client

.PHONY: build-daemon
build-daemon:
	@echo "Building daemon..."
	@mkdir -p bin
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_DAEMON) ./cmd/daemon

.PHONY: build-client
build-client:
	@echo "Building client..."
	@mkdir -p bin
	$(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_CLIENT) ./cmd/client

# Development build (no optimization)
.PHONY: build-dev
build-dev:
	@echo "Building development version..."
	@mkdir -p bin
	$(GOBUILD) -o bin/$(BINARY_DAEMON) ./cmd/daemon
	$(GOBUILD) -o bin/$(BINARY_CLIENT) ./cmd/client

# Cross-compilation targets
.PHONY: build-linux
build-linux:
	@echo "Building for Linux..."
	@mkdir -p bin
	GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_DAEMON)-linux ./cmd/daemon
	GOOS=linux GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_CLIENT)-linux ./cmd/client

.PHONY: build-darwin
build-darwin:
	@echo "Building for macOS..."
	@mkdir -p bin
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_DAEMON)-darwin ./cmd/daemon
	GOOS=darwin GOARCH=amd64 $(GOBUILD) -ldflags "$(LDFLAGS)" -o bin/$(BINARY_CLIENT)-darwin ./cmd/client

# Build for all platforms
.PHONY: build-all
build-all: build-linux build-darwin

# Test targets
.PHONY: test
test:
	$(GOTEST) -v ./...

.PHONY: test-race
test-race:
	$(GOTEST) -race -v ./...

.PHONY: test-coverage
test-coverage:
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Code quality targets
.PHONY: fmt
fmt:
	$(GOFMT) -s -w .

.PHONY: vet
vet:
	$(GOCMD) vet ./...

.PHONY: lint
lint:
	@which golangci-lint > /dev/null || (echo "Please install golangci-lint: https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run

# Clean targets
.PHONY: clean
clean:
	$(GOCLEAN)
	rm -rf bin/
	rm -f coverage.out coverage.html

# Dependency management
.PHONY: deps
deps:
	$(GOMOD) download
	$(GOMOD) tidy

.PHONY: deps-update
deps-update:
	$(GOMOD) get -u ./...
	$(GOMOD) tidy

# Development targets
.PHONY: run-daemon
run-daemon: build-daemon
	./bin/$(BINARY_DAEMON)

.PHONY: run-client
run-client: build-client
	./bin/$(BINARY_CLIENT)

# Version info
.PHONY: version
version:
	@echo "Git Hash: $(GIT_HASH)"
	@echo "Git Branch: $(GIT_BRANCH)"
	@echo "Git Tag: $(GIT_TAG)"
	@echo "Git Remote: $(GIT_REMOTE)"
	@echo "Build Time: $(BUILD_TIME)"

.PHONY: show-version
show-version: build-daemon
	./bin/$(BINARY_DAEMON) -v

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build all binaries"
	@echo "  build-daemon- Build daemon binary"
	@echo "  build-client- Build client binary"
	@echo "  build-dev   - Build development version (no optimization)"
	@echo "  build-linux - Build for Linux"
	@echo "  build-darwin - Build for macOS"
	@echo "  build-all   - Build for all platforms"
	@echo "  test        - Run tests"
	@echo "  test-race   - Run tests with race detection"
	@echo "  test-coverage - Generate test coverage report"
	@echo "  fmt         - Format code"
	@echo "  vet         - Run go vet"
	@echo "  lint        - Run golangci-lint"
	@echo "  clean       - Clean build artifacts"
	@echo "  deps        - Download and tidy dependencies"
	@echo "  deps-update - Update dependencies"
	@echo "  run-daemon  - Build and run daemon"
	@echo "  run-client  - Build and run client"
	@echo "  version     - Show build version info"
	@echo "  show-version- Build and show daemon version"
	@echo "  help        - Show this help message"
