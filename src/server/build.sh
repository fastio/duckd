#!/bin/bash
# Build script for DuckDB Server

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build_server"
DUCKDB_DIR="${SCRIPT_DIR}/.."

echo "=== DuckDB Server Build Script ==="
echo "Script directory: ${SCRIPT_DIR}"
echo "Build directory: ${BUILD_DIR}"

# Create build directory
mkdir -p "${BUILD_DIR}"
cd "${BUILD_DIR}"

# Configure with CMake
echo ""
echo "=== Configuring with CMake ==="
cmake "${SCRIPT_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DDUCKDB_DIR="${DUCKDB_DIR}"

# Build
echo ""
echo "=== Building ==="
cmake --build . --parallel $(sysctl -n hw.ncpu 2>/dev/null || nproc)

echo ""
echo "=== Build Complete ==="
echo "Server binary: ${BUILD_DIR}/duckdb-server"
