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

### 6. TPC-H Benchmark (`bench_tpch`)

Runs all 22 standard TPC-H analytical queries using DuckDB's built-in tpch extension. This is the gold standard OLAP benchmark for comparing with ClickHouse, PostgreSQL, and other databases.

```bash
# Run with default scale factor (sf=1, ~1GB data)
./bench_tpch -h 127.0.0.1 -p 5432

# Run with larger scale factor
./bench_tpch -h 127.0.0.1 -p 5432 -s 10

# JSON output for automated comparison
./bench_tpch -h 127.0.0.1 -p 5432 --json
```

Reports:
- Per-query execution time (all 22 TPC-H queries)
- Total query time and wall clock time
- Geometric mean of query times (standard OLAP comparison metric)
- Number of queries passed/failed

### 7. Scalability Benchmark (`bench_scalability`)

Measures throughput at increasing concurrency levels (1, 2, 4, 8, 16, 32, 64) to evaluate how well DuckD scales with more concurrent clients.

```bash
# Default: test all levels with 5s each
./bench_scalability -h 127.0.0.1 -p 5432

# Custom duration per level
./bench_scalability -h 127.0.0.1 -p 5432 -T 10

# Custom concurrency levels
./bench_scalability -h 127.0.0.1 -p 5432 --levels 1,4,16,64

# JSON output
./bench_scalability -h 127.0.0.1 -p 5432 --json
```

Reports per concurrency level:
- Throughput (QPS)
- P50 and P99 latency
- Scaling efficiency (actual QPS vs linear scaling from single-thread baseline)

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

# JSON output for all benchmarks
./benchmarks/run_benchmarks.sh --all --json

# Full report (runs all benchmarks, outputs unified JSON)
./benchmarks/run_benchmarks.sh --report
```

## JSON Output

All benchmarks support `--json` for structured output suitable for automated comparison and reporting.

### Individual benchmark JSON

```bash
./build/benchmarks/bench_query --json -h 127.0.0.1 -p 5432
```

```json
{"benchmark":"Query (SELECT_1)","total_operations":125432,"successful_operations":125432,"failed_operations":0,"total_time_s":10.001,"throughput_ops_sec":12541.85,"success_rate_pct":100.00,"latency":{"count":125432,"min_us":45.21,"max_us":2341.56,"mean_us":318.42,"stddev_us":156.78,"p50_us":289.12,"p90_us":456.78,"p95_us":612.34,"p99_us":1023.45}}
```

### TPC-H JSON output

```bash
./build/benchmarks/bench_tpch --json -h 127.0.0.1 -p 5432
```

```json
{"benchmark":"TPC-H","scale_factor":1,"queries_passed":22,"queries_failed":0,"total_query_time_ms":4523.12,"wall_time_ms":4612.34,"geometric_mean_ms":156.78,"queries":[{"query":1,"time_ms":234.56,"status":"ok"},{"query":2,"time_ms":89.12,"status":"ok"},...]}
```

### Scalability JSON output

```bash
./build/benchmarks/bench_scalability --json -h 127.0.0.1 -p 5432
```

```json
{"benchmark":"Scalability","duration_per_level_s":5,"baseline_qps":12500.00,"levels":[{"concurrency":1,"qps":12500.00,"p50_us":78.00,"p99_us":234.00,"scaling_efficiency_pct":100.00},{"concurrency":2,"qps":24100.00,"p50_us":81.00,"p99_us":256.00,"scaling_efficiency_pct":96.40},...]}
```

### Full report mode

```bash
./benchmarks/run_benchmarks.sh --report
```

Outputs a unified JSON document wrapping all benchmark results:

```json
{"report":"duckd-benchmark","host":"127.0.0.1","port":5432,"database":"duckd","results":[
{"benchmark":"Connection",...},
{"benchmark":"Query (SELECT_1)",...},
...
]}
```

## Cross-Database Comparison

The JSON output makes it straightforward to compare DuckD against other databases:

```bash
# Run against DuckD
./build/benchmarks/bench_tpch --json -h 127.0.0.1 -p 5432 > results_duckd.json

# Run same queries against PostgreSQL (using psql timing)
# Or use the same binary against a PG-compatible endpoint:
./build/benchmarks/bench_query --json -h pg-host -p 5432 > results_pg.json

# Compare with jq
echo "DuckD TPC-H geometric mean:"
jq '.geometric_mean_ms' results_duckd.json

echo "DuckD query throughput:"
jq '.throughput_ops_sec' results_duckd.json
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
| `--json` | Output results as JSON | off |
| `--help` | Show help | - |

TPC-H specific:
| Option | Description | Default |
|--------|-------------|---------|
| `-s, --scale` | TPC-H scale factor | 1 |

Scalability specific:
| Option | Description | Default |
|--------|-------------|---------|
| `-T, --duration` | Duration per concurrency level | 5 |
| `--levels` | Comma-separated concurrency levels | 1,2,4,8,16,32,64 |

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
| `bench_tpch` | TPC-H (no pgbench equivalent) |
| `bench_scalability` | `pgbench -c N -j N` at various N |

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

### TPC-H setup fails

1. Ensure DuckDB's tpch extension is available (it's built-in for recent versions)
2. For large scale factors, ensure sufficient memory and disk space
3. SF=1 needs ~1GB RAM, SF=10 needs ~10GB
