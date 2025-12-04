#!/bin/bash

# Build script for robot_data_autouploader with embedded git information

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    printf "${1}%s${NC}\n" "$2"
}

print_color $BLUE "Building robot_data_autouploader with embedded git information..."

# Get git information
GIT_HASH=$(git rev-parse HEAD 2>/dev/null || echo "unknown")
GIT_BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
GIT_TAG=$(git describe --tags --exact-match 2>/dev/null || echo "")
GIT_REMOTE=$(git remote get-url origin 2>/dev/null || echo "unknown")
BUILD_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Short hash for display
GIT_HASH_SHORT="${GIT_HASH:0:7}"

print_color $YELLOW "Git Information:"
print_color $NC "  Hash: $GIT_HASH_SHORT"
print_color $NC "  Branch: $GIT_BRANCH"
print_color $NC "  Tag: $GIT_TAG"
print_color $NC "  Remote: $GIT_REMOTE"
print_color $NC "  Build Time: $BUILD_TIME"

# Build flags
PKG_CONFIG="github.com/airoa-org/robot_data_pipeline/autoloader/internal/config"
LDFLAGS="-s -w"
LDFLAGS="$LDFLAGS -X '${PKG_CONFIG}.BuildGitHash=${GIT_HASH}'"
LDFLAGS="$LDFLAGS -X '${PKG_CONFIG}.BuildGitBranch=${GIT_BRANCH}'"
LDFLAGS="$LDFLAGS -X '${PKG_CONFIG}.BuildGitTag=${GIT_TAG}'"
LDFLAGS="$LDFLAGS -X '${PKG_CONFIG}.BuildGitRemote=${GIT_REMOTE}'"
LDFLAGS="$LDFLAGS -X '${PKG_CONFIG}.BuildTime=${BUILD_TIME}'"

# Create output directory
mkdir -p bin

print_color $BLUE "Building daemon..."
go build -ldflags "$LDFLAGS" -o bin/robot_data_daemon ./cmd/daemon

print_color $BLUE "Building client..."
go build -ldflags "$LDFLAGS" -o bin/robot_data_client ./cmd/client

print_color $GREEN "Build completed successfully!"
print_color $YELLOW "Binaries created:"
print_color $NC "  bin/robot_data_daemon"
print_color $NC "  bin/robot_data_client"

print_color $BLUE "Testing version output:"
print_color $NC "Daemon version:"
./bin/robot_data_daemon -v
echo
print_color $NC "Client version:"
./bin/robot_data_client -v
