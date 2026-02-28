<p align="center">
  <h1 align="center">DuckD</h1>
  <p align="center">
    <strong>Deploy DuckDB as a server-side analytical database.<br>PostgreSQL-compatible. DuckDB native SDK. Zero migration cost.</strong>
  </p>
  <p align="center">
    <a href="#quick-start">Quick Start</a> &middot;
    <a href="docs/examples.md">Examples</a> &middot;
    <a href="#configuration">Configuration</a> &middot;
    <a href="#architecture">Architecture</a>
  </p>
</p>

---

## What is DuckD?

[DuckDB](https://duckdb.org) is the fastest-growing embedded analytical database — but "embedded" means it lives inside a single process. You can't deploy it behind an API, share it across a team, or plug it into BI tools that need a network endpoint.

**DuckD turns DuckDB into a deployable backend service.** It wraps DuckDB with production-grade network protocols and exposes it as a multi-user server that can be deployed, managed, and monitored like any backend infrastructure.

```bash
./duckd-server --database /data/warehouse.duckdb    # deploy as a backend service
psql -h analytics.internal -p 5432 -U any -d any    # connect from any PG client

=> SELECT * FROM read_parquet('s3://bucket/sales/*.parquet');
```

## The Problem

DuckDB has redefined single-node analytical performance, but bringing it into production backend architectures means hitting the same walls:

- **No network access.** DuckDB runs in-process. Your backend services, BI dashboards, and data pipelines can't reach it over the network.
- **Single-process, single-user.** Multiple services or team members can't query the same DuckDB concurrently.
- **No deployment story.** There's no standard way to run DuckDB as a long-lived, managed backend service with health checks, metrics, and graceful lifecycle management.
- **Integration friction.** Adopting DuckDB in existing architectures typically means rewriting data access layers to use a DuckDB-specific SDK.

## How DuckD Solves It

DuckD exposes DuckDB through two complementary network protocols, designed so that **existing applications require near-zero code changes** to connect:

### PostgreSQL Wire Protocol — universal compatibility

DuckD implements the **PostgreSQL wire protocol v3** (the same binary protocol that PostgreSQL has used since 8.0). This means:

- **Every PostgreSQL client works out of the box** — `psql`, `psycopg2`, `pg` (Node.js), JDBC, `pgx` (Go), and hundreds more.
- **Every BI tool works** — Metabase, Tableau, Grafana, DBeaver, DataGrip, Superset — just point them at `host:5432`.
- **Existing applications need zero code changes.** If your app talks to PostgreSQL, it can talk to DuckD. Change the connection string, keep everything else.
- Full support: simple & extended query, prepared statements with parameters, transactions, `COPY TO/FROM`, query cancellation.

### DuckDB Native SDK — zero-copy columnar access

For workloads that need maximum throughput, DuckD also serves **Arrow Flight SQL** (gRPC). This unlocks:

- **Native DuckDB extension** — `ATTACH 'grpc://host:port'` from any DuckDB instance. Query remote tables as if they were local. Join local and remote data. Federate across multiple DuckD servers.
- **Arrow-native data transfer** — columnar data flows over the wire in Apache Arrow format with near-zero serialization cost. Ideal for data science workloads and large result sets.
- **`duckd_query()` / `duckd_exec()`** — table functions that let a local DuckDB execute SQL on a remote DuckD and consume the results natively.

### Together: two protocols, one server

```
    ┌─────────────────────────────────────────────────────────────────────────┐
    │  Your existing infrastructure                                         │
    │                                                                       │
    │  psql, JDBC, psycopg2,          DuckDB instances,                     │
    │  Metabase, Grafana, ...         pyarrow, ADBC, ...                    │
    │         │                              │                              │
    │         │ PostgreSQL protocol           │ Arrow Flight SQL (gRPC)      │
    │         │ (zero code changes)           │ (native columnar)            │
    └─────────┼──────────────────────────────┼──────────────────────────────┘
              ▼                              ▼
        ┌──────────────────────────────────────────┐
        │              DuckD Server                │
        │                                          │
        │   TCP :5432            gRPC :8815         │
        │   PG handler           Flight SQL        │
        │         └──────┬───────────┘             │
        │                ▼                         │
        │          ExecutorPool                    │
        │          SessionManager ──► DuckDB       │
        │                                          │
        │   HTTP :8080  /health  /metrics          │
        └──────────────────────────────────────────┘
```

PostgreSQL protocol gives you **instant compatibility** with the entire database ecosystem. Arrow Flight SQL gives you **native DuckDB power** at wire speed. Same server, same data, pick the right protocol for each client.

## Features

| | Feature | Details |
|-|---------|---------|
| | **PostgreSQL Protocol v3** | Simple & extended query, prepared statements, parameters, portals |
| | **Transactions** | `BEGIN` / `COMMIT` / `ROLLBACK` with full DuckDB ACID semantics |
| | **COPY Protocol** | `COPY TO STDOUT`, `COPY FROM STDIN` — bulk import/export via `\copy` |
| | **Query Cancellation** | `Ctrl-C` in psql propagates to DuckDB's interrupt mechanism |
| | **Query Timeout** | Configurable per-server, kills runaway queries |
| | **Arrow Flight SQL** | High-throughput gRPC interface for columnar data transfer (opt-in) |
| | **DuckDB Client Extension** | `ATTACH 'grpc://host:port'` — federated queries from any DuckDB instance |
| | **Health & Metrics** | HTTP `/health` + Prometheus `/metrics` endpoint |
| | **Backend Deployment** | Daemon mode, privilege dropping, PID files, `SIGHUP` hot-reload |
| | **systemd** | `Type=notify` integration with watchdog support (Linux) |

> **Note:** DuckD does not yet implement authentication or TLS. Deploy behind a firewall or use a TLS-terminating proxy (Nginx, HAProxy, stunnel) for network-exposed deployments.

## Quick Start

```bash
# Clone
git clone --recursive https://github.com/jian-fang/duckd.git && cd duckd

# Build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Run
./build/programs/server/duckd-server

# Connect (any username/database accepted)
psql -h localhost -p 5432 -U any -d any
```

That's it. No config files, no setup, no dependencies to install at runtime.

### Build options

```bash
# With Arrow Flight SQL + DuckDB client extension
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_FLIGHT_SQL=ON

# With systemd support (Linux)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_SYSTEMD=ON

# Run tests
ctest --test-dir build --output-on-failure
```

**Requirements:** CMake >= 3.16, C++17 compiler, Git. All dependencies are vendored in `contrib/` — no system packages needed.

## Usage

### PostgreSQL protocol

```bash
# Start server
./duckd-server --database /data/warehouse.duckdb --http-port 8080

# Connect with any PostgreSQL client
psql -h localhost -p 5432 -U any -d any
```

```sql
SELECT * FROM read_parquet('s3://my-bucket/sales/*.parquet');
SELECT * FROM read_csv('/data/report.csv', header=true);
CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/**/*.parquet');
```

### DuckDB native SDK

```bash
# Start server with Flight SQL enabled
./duckd-server --database /data/warehouse.duckdb --port 5432 --flight-port 8815
```

```sql
-- Attach remote DuckD server as a local catalog
LOAD duckd_client;
ATTACH 'grpc://analytics-server:8815' AS warehouse (TYPE duckd);

-- Query remote tables, join with local data
SELECT * FROM warehouse.main.sales WHERE year = 2024;
SELECT duckd_exec('grpc://server:8815', 'INSERT INTO logs VALUES (now(), ''hello'')');
```

### More examples

| Topic | Description |
|-------|-------------|
| [Multi-language clients](docs/examples.md#connecting-from-different-languages) | Python, Node.js, Java, Go, Rust, Pandas, GUI tools |
| [COPY protocol](docs/examples.md#bulk-data-with-copy) | Bulk import/export via `\copy` and `copy_expert` |
| [Transactions](docs/examples.md#transactions-and-prepared-statements) | Prepared statements, parameterized queries, rollback |
| [Arrow Flight SQL](docs/examples.md#arrow-flight-sql) | pyarrow, ADBC — zero-copy columnar access |
| [Federated queries](docs/examples.md#duckdb-native-sdk--federated-queries) | ATTACH, `duckd_query()`, `duckd_exec()`, multi-server |
| [Monitoring](docs/examples.md#monitoring) | Health checks, Prometheus metrics integration |
| [Configuration](docs/examples.md#configuration-examples) | YAML / INI config file templates |

## Configuration

### Config file

```bash
./duckd-server --config /etc/duckd/config.yaml
```

DuckD supports both INI and YAML formats (auto-detected by file extension). See [`etc/duckd.conf.example`](etc/duckd.conf.example) and [`etc/config.yaml.example`](etc/config.yaml.example).

### All options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5432` | PostgreSQL protocol port |
| `--database` | `:memory:` | DuckDB database file (`:memory:` for in-memory) |
| `--io-threads` | auto | I/O thread count (default: CPU cores / 2) |
| `--executor-threads` | auto | Query worker threads (default: CPU cores) |
| `--max-connections` | `100` | Maximum concurrent connections |
| `--query-timeout` | `300000` | Query timeout in milliseconds (5 min) |
| `--http-port` | disabled | HTTP port for `/health` and `/metrics` |
| `--flight-port` | disabled | Arrow Flight SQL gRPC port |
| `--max-memory` | unlimited | Memory limit in bytes (Linux only) |
| `--max-open-files` | system | File descriptor limit |
| `--daemon` | `false` | Daemonize the process |
| `--pid-file` | — | PID file path |
| `--log-file` | stderr | Log output file |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error` |
| `--user` | — | Drop privileges to this user after start |
| `--config` | — | Path to config file (INI or YAML) |
| `--version` | — | Print version and exit |

### Signals

| Signal | Behavior |
|--------|----------|
| `SIGTERM` / `SIGINT` | Graceful shutdown — drains active connections |
| `SIGHUP` | Hot-reload config (log level takes effect immediately) |

## Production Deployment

### systemd

```ini
[Unit]
Description=DuckD — DuckDB network server
After=network.target

[Service]
Type=notify
User=duckd
ExecStart=/usr/local/bin/duckd-server --config /etc/duckd/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM ubuntu:24.04
COPY build/programs/server/duckd-server /usr/local/bin/
EXPOSE 5432 8080 8815
ENTRYPOINT ["duckd-server"]
CMD ["--database", "/data/warehouse.duckdb", "--http-port", "8080"]
```

```bash
docker run -d --name duckd \
  -p 5432:5432 -p 8080:8080 \
  -v ./data:/data \
  duckd --database /data/warehouse.duckdb --http-port 8080
```

## Architecture

```
                    PostgreSQL clients                Arrow Flight clients
                (psql, JDBC, psycopg2, BI tools)       (pyarrow, ADBC)
                          │                                  │
                          │ TCP :5432                         │ gRPC :8815
                          ▼                                  ▼
                    ┌───────────┐                     ┌──────────────┐
                    │ TcpServer │                     │ FlightSQL    │
                    │ (ASIO)    │                     │ Server       │
                    └─────┬─────┘                     └──────┬───────┘
                          │ PgHandler                        │
                          │ (protocol state machine)         │
                          └──────────────┬───────────────────┘
                                         ▼
                              ┌─────────────────────┐
                              │   ExecutorPool      │
                              │ (lock-free queue,   │
                              │  N worker threads)  │
                              └──────────┬──────────┘
                                         ▼
                              ┌─────────────────────┐
                              │  SessionManager     │
                              │  Session per conn   │──► DuckDB
                              │  (sticky sessions)  │
                              └─────────────────────┘
```

**Key design decisions:**

- **Sticky sessions.** Each network connection owns a dedicated DuckDB connection for its lifetime. Transactions, prepared statements, and temp tables are naturally scoped — no cross-connection leakage.
- **Async I/O, sync execution.** Network I/O is non-blocking (ASIO). Queries are submitted to a fixed-size executor pool via a lock-free concurrent queue (moodycamel), keeping I/O threads responsive under heavy query load.
- **Dual protocol.** PostgreSQL protocol for universal ecosystem compatibility (existing apps, BI tools — zero code changes); Arrow Flight SQL for native DuckDB SDK access and high-throughput columnar transfer.
- **Zero external dependencies at runtime.** Single static binary. No JVM, no Python, no cluster coordinator.


## Contributing

Contributions are welcome! Please:

1. Fork the repo and create a feature branch
2. Make sure all tests pass: `ctest --test-dir build --output-on-failure`
3. Open a pull request with a clear description

## License

[Apache License 2.0](LICENSE)
