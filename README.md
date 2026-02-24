# DuckD

A network server for [DuckDB](https://duckdb.org) that speaks the PostgreSQL wire protocol, enabling any PostgreSQL-compatible client to connect to a shared DuckDB instance.

```
psql -h localhost -p 5432 -U any -d any
```

## Why

DuckDB is an embedded database — it runs inside a single process. DuckD exposes it as a standalone server so multiple clients can share one database over a standard network protocol.

**Use cases:**
- Shared analytics backend for BI tools (Metabase, Tableau, DBeaver)
- Query Parquet/CSV files remotely via SQL
- Lightweight OLAP service without a heavy data warehouse
- Development/staging analytics environment

## Features

| Feature | Status |
|---------|--------|
| PostgreSQL wire protocol v3 | ✅ |
| Simple and extended query protocol | ✅ |
| Prepared statements | ✅ |
| Transactions | ✅ |
| Connection pooling | ✅ |
| Query cancellation | ✅ |
| Query timeout | ✅ |
| HTTP health & Prometheus metrics | ✅ |
| Arrow Flight SQL | ✅ (opt-in) |
| Daemon mode + privilege dropping | ✅ |
| systemd integration | ✅ (opt-in) |
| SSL/TLS | ❌ |
| Authentication | ❌ (trust only) |
| COPY protocol | ❌ |

> **Security note:** DuckD does not implement authentication or TLS. Deploy behind a firewall or use a TLS proxy (e.g. stunnel, Nginx) for network-exposed deployments.

## Quick Start

```bash
# Clone with submodules
git clone --recursive https://github.com/your-org/duckd.git
cd duckd

# Build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Run (in-memory database, port 5432)
./build/programs/server/duckd-server

# Connect
psql -h localhost -p 5432 -U any -d any
```

## Building

**Requirements:** CMake ≥ 3.16, C++17 compiler, Git

```bash
# Release build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# With Arrow Flight SQL support
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_FLIGHT_SQL=ON
cmake --build build -j$(nproc)

# With systemd integration (Linux)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_SYSTEMD=ON
cmake --build build -j$(nproc)

# Run tests
ctest --test-dir build --output-on-failure
```

## Configuration

### Command line

```bash
./duckd-server \
  --host 0.0.0.0 \
  --port 5432 \
  --database /data/mydb.duckdb \
  --executor-threads 8 \
  --max-connections 100 \
  --query-timeout 300000 \
  --http-port 8080
```

### Config file

```bash
./duckd-server --config /etc/duckd/duckd.conf
```

`/etc/duckd/duckd.conf`:

```ini
host = 0.0.0.0
port = 5432
database = /data/mydb.duckdb

io_threads = 4
executor_threads = 8
max_connections = 100
query_timeout_ms = 300000

http_port = 8080

# Production
daemon = true
pid_file = /var/run/duckd.pid
log_file = /var/log/duckd.log
log_level = info
user = duckd
max_open_files = 65535
```

### All options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5432` | PostgreSQL protocol port |
| `--database` | `:memory:` | Database file path |
| `--io-threads` | auto | IO thread count |
| `--executor-threads` | auto | Query executor thread count |
| `--max-connections` | `100` | Maximum concurrent connections |
| `--query-timeout` | `300000` | Query timeout in milliseconds |
| `--http-port` | disabled | HTTP port for health/metrics |
| `--flight-port` | disabled | Arrow Flight SQL port |
| `--daemon` | `false` | Run as background daemon |
| `--pid-file` | — | PID file path |
| `--log-file` | stderr | Log file path |
| `--log-level` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--user` | — | Drop privileges to this user after start |
| `--max-open-files` | system | File descriptor limit |
| `--config` | — | Config file path |

## Connecting

**psql:**
```bash
psql -h localhost -p 5432 -U any -d any
```

**Python (psycopg2):**
```python
import psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="any", database="any")
cur = conn.cursor()
cur.execute("SELECT * FROM read_parquet('/data/file.parquet')")
```

**DBeaver / TablePlus / DataGrip:** Create a PostgreSQL connection to `localhost:5432`.

## Health & Metrics

When `--http-port` is set:

```bash
# Liveness check
curl http://localhost:8080/health
# {"status":"healthy"}

# Prometheus metrics
curl http://localhost:8080/metrics
```

Exported metrics include active sessions, connection pool utilization, acquire counts, and timeouts.

## Signals

| Signal | Action |
|--------|--------|
| `SIGTERM`, `SIGINT` | Graceful shutdown |
| `SIGHUP` | Reload log level from config file |

## systemd

```ini
[Unit]
Description=DuckD Server
After=network.target

[Service]
Type=notify
User=duckd
ExecStart=/usr/local/bin/duckd-server --config /etc/duckd/duckd.conf
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

## Architecture

```
Client (psql / JDBC / psycopg2)
    │
    │ PostgreSQL wire protocol
    ▼
TcpServer  ──  PgHandler (per connection, async I/O via ASIO)
                    │
                    │  async task submission
                    ▼
              ExecutorPool (lock-free queue, N worker threads)
                    │
                    ▼
              ConnectionPool ──► DuckDB connections
```

- I/O is non-blocking via ASIO; queries run on a separate executor thread pool
- Connection pool is shared across sessions (LIFO, lazy acquisition)
- Query cancellation propagates to DuckDB's interrupt mechanism
- State mutations (transactions, prepared statements) are pinned to I/O threads for safety

## License

Apache License 2.0 — see [LICENSE](LICENSE).
