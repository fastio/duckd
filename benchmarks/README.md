# DuckD Benchmark Suite

A comprehensive benchmark suite for testing DuckD server performance using the PostgreSQL wire protocol.

## Benchmarks

### 1. Connection Benchmark (`bench_connection`)

Tests connection establishment and teardown performance.

```bash
./bench_connection -h 127.0.0.1 -p 5432 -t 4 -T 10
```

Metrics measured:
- Connections per second
- Connection latency (min, max, mean, percentiles)

### 2. Query Benchmark (`bench_query`)

Tests simple query throughput using the simple query protocol.

```bash
./bench_query -h 127.0.0.1 -p 5432 -t 4 -T 10 -q select1
```

Query types:
- `select1` - `SELECT 1`
- `now` - `SELECT current_timestamp`
- `generate` - `SELECT * FROM generate_series(1, 100)`
- `table` - `SELECT * FROM bench_test LIMIT 100`
- `insert` - `INSERT INTO bench_test ...`
- `update` - `UPDATE bench_test ...`
- Custom SQL query

### 3. Prepared Statement Benchmark (`bench_prepared`)

Tests extended query protocol with prepared statements.

```bash
./bench_prepared -h 127.0.0.1 -p 5432 -t 4 -T 10 -s 10 --params 3
```

Metrics measured:
- Prepared statement executions per second
- Parameter binding overhead
- Execution latency

### 4. Concurrent Connections Benchmark (`bench_concurrent`)

Stress test with many concurrent connections.

```bash
./bench_concurrent -h 127.0.0.1 -p 5432 -t 50 -T 30
```

Tests:
- Maximum concurrent connection handling
- Connection stability under load
- Query throughput with high concurrency

### 5. TPC-B Benchmark (`bench_tpcb`)

TPC-B like benchmark similar to pgbench.

```bash
# Initialize tables (run once)
./bench_tpcb -h 127.0.0.1 -p 5432 -s 1 -i

# Run benchmark
./bench_tpcb -h 127.0.0.1 -p 5432 -t 4 -T 60 -s 1
```

Tables created:
- `pgbench_branches` - 1 per scale factor
- `pgbench_tellers` - 10 per scale factor
- `pgbench_accounts` - 100,000 per scale factor
- `pgbench_history` - Transaction log

## Building

```bash
# From project root
mkdir -p build && cd build
cmake .. -DBUILD_BENCHMARKS=ON
cmake --build . --target benchmarks
```

Requirements:
- C++17 compiler
- All dependencies (ASIO) are included in contrib/ - no external libraries needed

## Running All Benchmarks

Use the convenience script:

```bash
# Run all benchmarks
./benchmarks/run_benchmarks.sh --all

# Run specific benchmarks
./benchmarks/run_benchmarks.sh query prepared

# Custom configuration
./benchmarks/run_benchmarks.sh -h localhost -p 5433 -t 8 -T 30 --all
```

## Command Line Options

All benchmarks share common options:

| Option | Description | Default |
|--------|-------------|---------|
| `-h, --host` | Server host | 127.0.0.1 |
| `-p, --port` | Server port | 5432 |
| `-d, --database` | Database name | duckd |
| `-U, --user` | Username | duckd |
| `-W, --password` | Password | (none) |
| `-t, --threads` | Worker threads | 4 |
| `-T, --duration` | Test duration (seconds) | 10 |
| `-n, --operations` | Operations per thread | (time-based) |
| `-v, --verbose` | Verbose output | off |
| `--help` | Show help | - |

## Output Metrics

Each benchmark reports:

- **Total Operations**: Number of operations attempted
- **Successful Operations**: Operations completed without error
- **Failed Operations**: Operations that failed
- **Throughput**: Operations per second
- **Success Rate**: Percentage of successful operations

Latency statistics (in microseconds):
- **Min/Max**: Minimum and maximum latency
- **Mean**: Average latency
- **StdDev**: Standard deviation
- **P50/P90/P95/P99**: Percentile latencies

## Example Output

```
=== Query Benchmark ===

Benchmark Configuration:
  Host:        127.0.0.1:5432
  Database:    duckd
  User:        duckd
  Threads:     4
  Duration:    10 seconds
  Query Type:  SELECT_1

Running benchmark...

========================================
Benchmark: Query (SELECT_1)
========================================
Total Operations:      125432
Successful Operations: 125432
Failed Operations:     0
Total Time:            10.001 seconds
Throughput:            12541.85 ops/sec
Success Rate:          100.00%

Latency Statistics (microseconds):
  Count:  125432
  Min:    45.21
  Max:    2341.56
  Mean:   318.42
  StdDev: 156.78
  P50:    289.12
  P90:    456.78
  P95:    612.34
  P99:    1023.45
```

## Comparison with pgbench

The benchmark suite is designed to be comparable with PostgreSQL's pgbench:

| DuckD Benchmark | pgbench Equivalent |
|-----------------|-------------------|
| `bench_connection` | Connection overhead |
| `bench_query -q select1` | `pgbench -S` (select-only) |
| `bench_prepared` | Extended query protocol |
| `bench_tpcb` | `pgbench` (TPC-B) |

To compare with pgbench:

```bash
# DuckD TPC-B
./bench_tpcb -h 127.0.0.1 -p 5432 -t 4 -T 60 -s 1 -i

# PostgreSQL pgbench (for comparison)
pgbench -h 127.0.0.1 -p 5432 -c 4 -j 4 -T 60 -s 1 -i postgres
pgbench -h 127.0.0.1 -p 5432 -c 4 -j 4 -T 60 postgres
```

## Troubleshooting

### Connection failures

1. Ensure DuckD server is running
2. Check host/port configuration
3. Verify no firewall blocking connections

### Low throughput

1. Increase thread count (`-t`)
2. Check server resources (CPU, memory)
3. Review server configuration (max connections, thread pools)
