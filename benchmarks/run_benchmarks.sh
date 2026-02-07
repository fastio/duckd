#!/bin/bash
#===----------------------------------------------------------------------===//
#                         DuckD Server - Benchmark Runner
#
# benchmarks/run_benchmarks.sh
#
# Runs all benchmarks against a DuckD server
#===----------------------------------------------------------------------===//

set -e

# Default configuration
HOST="${DUCKD_HOST:-127.0.0.1}"
PORT="${DUCKD_PORT:-5432}"
DATABASE="${DUCKD_DATABASE:-duckd}"
USER="${DUCKD_USER:-duckd}"
PASSWORD="${DUCKD_PASSWORD:-}"
THREADS="${DUCKD_THREADS:-4}"
DURATION="${DUCKD_DURATION:-10}"

# Build directory (relative to script location)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${BUILD_DIR:-${SCRIPT_DIR}/../build/benchmarks}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

usage() {
    cat << EOF
DuckD Benchmark Runner

Usage: $0 [options] [benchmark...]

Options:
    -h, --host HOST        Server host (default: $HOST)
    -p, --port PORT        Server port (default: $PORT)
    -d, --database DB      Database name (default: $DATABASE)
    -U, --user USER        Username (default: $USER)
    -W, --password PASS    Password
    -t, --threads N        Number of threads (default: $THREADS)
    -T, --duration SECS    Test duration (default: $DURATION)
    -b, --build-dir DIR    Build directory (default: $BUILD_DIR)
    --all                  Run all benchmarks
    --json                 Output results as JSON
    --report               Run all benchmarks and output unified JSON report
    --help                 Show this help

Available benchmarks:
    connection    Connection establishment benchmark
    query         Simple query throughput benchmark
    prepared      Prepared statement benchmark
    concurrent    Concurrent connections stress test
    tpcb          TPC-B like benchmark (pgbench style)
    tpch          TPC-H analytical query benchmark
    scalability   Concurrency scaling benchmark

Environment variables:
    DUCKD_HOST, DUCKD_PORT, DUCKD_DATABASE, DUCKD_USER, DUCKD_PASSWORD
    DUCKD_THREADS, DUCKD_DURATION, BUILD_DIR

Examples:
    $0 query                          # Run query benchmark
    $0 --all                          # Run all benchmarks
    $0 -t 8 -T 30 query prepared      # Run with 8 threads for 30 seconds
    $0 -h localhost -p 5433 --all     # Run against different server
    $0 --json query prepared           # Run with JSON output
    $0 --report                        # Full report with JSON output
    $0 --report --json                 # Same as --report
EOF
}

JSON_OUTPUT=false
REPORT_MODE=false
EXTRA_ARGS=""

run_benchmark() {
    local name=$1
    local binary="${BUILD_DIR}/bench_${name}"

    if [[ ! -x "$binary" ]]; then
        if ! $JSON_OUTPUT; then
            echo -e "${RED}Error: Benchmark binary not found: $binary${NC}" >&2
            echo "Please build benchmarks first: cmake --build build --target benchmarks" >&2
        fi
        return 1
    fi

    local cmd="$binary -h $HOST -p $PORT -d $DATABASE -U $USER -t $THREADS -T $DURATION"
    [[ -n "$PASSWORD" ]] && cmd="$cmd -W $PASSWORD"

    # TPC-H and scalability don't use -t for threads
    if [[ "$name" == "tpch" ]]; then
        cmd="$binary -h $HOST -p $PORT -d $DATABASE -U $USER"
        [[ -n "$PASSWORD" ]] && cmd="$cmd -W $PASSWORD"
    fi
    if [[ "$name" == "scalability" ]]; then
        cmd="$binary -h $HOST -p $PORT -d $DATABASE -U $USER -T $DURATION"
        [[ -n "$PASSWORD" ]] && cmd="$cmd -W $PASSWORD"
    fi

    if $JSON_OUTPUT; then
        cmd="$cmd --json"
    fi

    if ! $JSON_OUTPUT; then
        echo -e "${YELLOW}======================================${NC}"
        echo -e "${YELLOW}Running: $name benchmark${NC}"
        echo -e "${YELLOW}======================================${NC}"
        echo "Command: $cmd"
        echo ""
    fi

    local output
    if output=$($cmd 2>&1); then
        if $JSON_OUTPUT; then
            echo "$output"
        else
            echo "$output"
            echo -e "${GREEN}$name benchmark completed successfully${NC}"
        fi
    else
        if ! $JSON_OUTPUT; then
            echo "$output"
            echo -e "${RED}$name benchmark failed${NC}"
        fi
        return 1
    fi

    if ! $JSON_OUTPUT; then
        echo ""
    fi
}

# Parse arguments
BENCHMARKS=()
RUN_ALL=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--host)
            HOST="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -d|--database)
            DATABASE="$2"
            shift 2
            ;;
        -U|--user)
            USER="$2"
            shift 2
            ;;
        -W|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -t|--threads)
            THREADS="$2"
            shift 2
            ;;
        -T|--duration)
            DURATION="$2"
            shift 2
            ;;
        -b|--build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        --all)
            RUN_ALL=true
            shift
            ;;
        --json)
            JSON_OUTPUT=true
            shift
            ;;
        --report)
            REPORT_MODE=true
            RUN_ALL=true
            JSON_OUTPUT=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            BENCHMARKS+=("$1")
            shift
            ;;
    esac
done

# Check if build directory exists
if [[ ! -d "$BUILD_DIR" ]]; then
    if ! $JSON_OUTPUT; then
        echo -e "${RED}Error: Build directory not found: $BUILD_DIR${NC}"
        echo "Please build benchmarks first:"
        echo "  mkdir -p build && cd build"
        echo "  cmake .."
        echo "  cmake --build . --target benchmarks"
    fi
    exit 1
fi

# Determine which benchmarks to run
if $RUN_ALL; then
    BENCHMARKS=(connection query prepared concurrent tpcb tpch scalability)
fi

if [[ ${#BENCHMARKS[@]} -eq 0 ]]; then
    echo "No benchmarks specified. Use --all or specify benchmark names."
    usage
    exit 1
fi

# Report mode: wrap all output in a JSON array
if $REPORT_MODE; then
    echo "{\"report\":\"duckd-benchmark\",\"host\":\"$HOST\",\"port\":$PORT,\"database\":\"$DATABASE\",\"results\":["
    FIRST=true
    FAILED=0
    for bench in "${BENCHMARKS[@]}"; do
        if ! $FIRST; then
            echo ","
        fi
        FIRST=false
        if ! run_benchmark "$bench"; then
            ((FAILED++)) || true
        fi
    done
    echo "]}"
    exit $FAILED
fi

# Normal mode
if ! $JSON_OUTPUT; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}DuckD Benchmark Suite${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo "Server:    $HOST:$PORT"
    echo "Database:  $DATABASE"
    echo "User:      $USER"
    echo "Threads:   $THREADS"
    echo "Duration:  ${DURATION}s"
    echo "Build Dir: $BUILD_DIR"
    echo "Benchmarks: ${BENCHMARKS[*]}"
    echo ""
fi

# Run benchmarks
FAILED=0
for bench in "${BENCHMARKS[@]}"; do
    if ! run_benchmark "$bench"; then
        ((FAILED++)) || true
    fi
done

# Summary
if ! $JSON_OUTPUT; then
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}Benchmark Summary${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo "Total benchmarks run: ${#BENCHMARKS[@]}"
    if [[ $FAILED -eq 0 ]]; then
        echo -e "${GREEN}All benchmarks passed!${NC}"
    else
        echo -e "${RED}Failed benchmarks: $FAILED${NC}"
        exit 1
    fi
fi
