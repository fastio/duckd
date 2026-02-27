<p align="center">
  <h1 align="center">DuckD</h1>
  <p align="center">
    <strong>Turn DuckDB into a multi-user server — zero config, PostgreSQL-compatible.</strong>
  </p>
  <p align="center">
    <a href="#quick-start">Quick Start</a> &middot;
    <a href="#usage-examples">Examples</a> &middot;
    <a href="#configuration">Configuration</a> &middot;
    <a href="#architecture">Architecture</a>
  </p>
</p>

---

DuckD is a lightweight network server that wraps [DuckDB](https://duckdb.org) with the **PostgreSQL wire protocol** (v3) and optionally **Arrow Flight SQL** (gRPC). Any PostgreSQL client — `psql`, Python, Node.js, Java, Go, BI tools — can connect and query DuckDB over the network without code changes.

```bash
./duckd-server                                      # start (in-memory, port 5432)
psql -h localhost -p 5432 -U any -d any             # connect with any PG client

=> SELECT * FROM read_parquet('s3://bucket/sales/*.parquet');
```

## Why DuckD?

DuckDB is a blazing-fast embedded analytical database, but it runs inside a single process. This makes it hard to share with a team, connect BI tools, or run as a long-lived service. DuckD solves this:

| Problem | DuckD Solution |
|---------|----------------|
| DuckDB is single-process, single-user | Multi-client server with concurrent connections |
| BI tools need a network endpoint | Speaks standard PostgreSQL protocol — works out of the box |
| No way to query remote DuckDB instances | Client extension lets you `ATTACH` remote servers for federated queries |
| Moving large datasets is slow | Arrow Flight SQL streams columnar data at near-zero serialization cost |
| Need a data warehouse but it's overkill | Lightweight OLAP server — single binary, no JVM, no cluster |

## Features

| | Feature | Details |
|-|---------|---------|
| | **PostgreSQL Protocol v3** | Simple & extended query, prepared statements, parameters, portals |
| | **Transactions** | `BEGIN` / `COMMIT` / `ROLLBACK` with full DuckDB ACID semantics |
| | **COPY Protocol** | `COPY TO STDOUT`, `COPY FROM STDIN` — bulk import/export via `\copy` |
| | **Query Cancellation** | `Ctrl-C` in psql propagates to DuckDB's interrupt mechanism |
| | **Query Timeout** | Configurable per-server, kills runaway queries |
| | **Arrow Flight SQL** | High-throughput gRPC interface for columnar data transfer (opt-in) |
| | **Client Extension** | `ATTACH 'grpc://host:port'` — federated queries from any DuckDB instance |
| | **Health & Metrics** | HTTP `/health` + Prometheus `/metrics` endpoint |
| | **Production Ready** | Daemon mode, privilege dropping, PID files, `SIGHUP` reload |
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

### Build Options

```bash
# Standard build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# With Arrow Flight SQL + client extension
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_FLIGHT_SQL=ON
cmake --build build -j$(nproc)

# With systemd support (Linux)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DWITH_SYSTEMD=ON
cmake --build build -j$(nproc)

# Run tests
ctest --test-dir build --output-on-failure
```

**Requirements:** CMake >= 3.16, C++17 compiler, Git. All dependencies are vendored in `contrib/` — no system packages needed.

## Usage Examples

### Query files over the network

Connect from any PostgreSQL client and query files the server has access to — Parquet, CSV, JSON, and even remote files on S3 or HTTP:

```sql
-- Parquet (local, glob, remote)
SELECT * FROM read_parquet('/data/events/*.parquet') WHERE event_date >= '2024-01-01';
SELECT * FROM read_parquet('s3://my-bucket/sales/**/*.parquet');
SELECT * FROM read_parquet('https://example.com/dataset.parquet');

-- CSV with options
SELECT * FROM read_csv('/data/report.csv', header=true, delim='|');

-- Build tables from file scans
CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/**/*.parquet');
```

### Connect from any language

DuckD speaks standard PostgreSQL — use your existing driver:

<details>
<summary><b>psql</b></summary>

```bash
psql -h localhost -p 5432 -U any -d any
```
</details>

<details>
<summary><b>Python (psycopg2)</b></summary>

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", database="any")
cur = conn.cursor()
cur.execute("SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
for row in cur.fetchall():
    print(row)
conn.close()
```
</details>

<details>
<summary><b>Python (SQLAlchemy)</b></summary>

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://any:any@localhost:5432/any")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT product, SUM(amount) as total
        FROM read_parquet('/data/sales.parquet')
        GROUP BY product ORDER BY total DESC
    """))
    for row in result:
        print(row)
```
</details>

<details>
<summary><b>Node.js</b></summary>

```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost', port: 5432, user: 'any', database: 'any'
});
await client.connect();

const res = await client.query(
  'SELECT * FROM read_parquet($1) LIMIT 10', ['/data/sales.parquet']
);
console.log(res.rows);
await client.end();
```
</details>

<details>
<summary><b>Java (JDBC)</b></summary>

```java
Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/any", "any", "any");

Statement stmt = conn.createStatement();
ResultSet rs = stmt.executeQuery(
    "SELECT product, SUM(amount) FROM sales GROUP BY product");

while (rs.next()) {
    System.out.printf("%s: %.2f%n", rs.getString(1), rs.getDouble(2));
}
conn.close();
```
</details>

<details>
<summary><b>Go (pgx)</b></summary>

```go
conn, _ := pgx.Connect(context.Background(),
    "postgres://any:any@localhost:5432/any")
defer conn.Close(context.Background())

rows, _ := conn.Query(context.Background(),
    "SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
```
</details>

**GUI tools:** DBeaver, TablePlus, DataGrip, Metabase, Grafana — just create a PostgreSQL connection to `localhost:5432`.

### Bulk data with COPY

```bash
# Export query results
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY (SELECT * FROM sales WHERE year=2024) TO '/tmp/sales.csv' WITH (FORMAT csv, HEADER)"

# Import from CSV
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY products FROM '/tmp/products.csv' WITH (FORMAT csv, HEADER)"
```

```python
# Python: streaming COPY with psycopg2
conn = psycopg2.connect(host="localhost", port=5432, user="any", dbname="any")
cur = conn.cursor()

with open('output.csv', 'w') as f:
    cur.copy_expert("COPY (SELECT * FROM sales) TO STDOUT WITH CSV HEADER", f)

with open('data.csv', 'r') as f:
    cur.copy_expert("COPY target_table FROM STDIN WITH CSV HEADER", f)

conn.commit()
```

### Transactions and prepared statements

DuckD supports the full PostgreSQL extended query protocol:

```python
conn = psycopg2.connect(host="localhost", port=5432, user="any", dbname="any")
conn.autocommit = False
cur = conn.cursor()

try:
    cur.execute("INSERT INTO accounts VALUES (%s, %s)", (1, 1000.00))
    cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (100, 1))
    cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (100, 2))
    conn.commit()
except Exception:
    conn.rollback()
    raise
```

### Arrow Flight SQL — high-throughput columnar access

Start the server with Flight SQL enabled for near-zero-copy data transfer over gRPC:

```bash
./duckd-server --port 5432 --flight-port 8815 --http-port 8080
```

```python
import pyarrow.flight

client = pyarrow.flight.FlightClient("grpc://localhost:8815")

info = client.get_flight_info(
    pyarrow.flight.FlightDescriptor.for_command(
        b'SELECT * FROM read_parquet("/data/sales.parquet")'
    )
)

reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()            # Zero-copy Arrow Table
df = table.to_pandas()               # Convert to Pandas
print(df.describe())
```

### Federated queries with the DuckDB client extension

The `duckd-client` extension (built with `-DWITH_FLIGHT_SQL=ON`) lets any DuckDB instance transparently ATTACH a remote DuckD server. Query remote tables as if they were local and join across multiple data sources:

```sql
LOAD duckd_client;

-- Attach a remote DuckD server
ATTACH 'grpc://analytics-server:8815' AS warehouse (TYPE duckd);

-- Query remote tables transparently
SELECT * FROM warehouse.main.sales WHERE region = 'APAC' AND year = 2024;

-- Join local and remote data — the optimizer pushes filters to the remote side
SELECT o.order_id, o.total, c.name, c.tier
FROM local_orders o
JOIN warehouse.main.customers c ON o.customer_id = c.id
WHERE c.tier = 'enterprise';
```

Execute DDL/DML on remote servers:

```sql
-- duckd_exec: run statements, returns affected row count
SELECT duckd_exec('grpc://server:8815', 'CREATE TABLE logs (ts TIMESTAMP, msg VARCHAR)');
SELECT duckd_exec('grpc://server:8815', 'INSERT INTO logs VALUES (now(), ''deployed v2.1'')');

-- duckd_query: run a query, returns results as a table function
SELECT * FROM duckd_query('grpc://server:8815',
    'SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal
     FROM employees GROUP BY dept ORDER BY avg_sal DESC');
```

### Monitoring

```bash
# Health check — use in load balancer probes
curl http://localhost:8080/health
# {"status":"healthy","connections":3}

# Prometheus-compatible metrics
curl http://localhost:8080/metrics
# duckd_connections_active 3
# duckd_connections_total 42
# duckd_sessions_active 3
# duckd_bytes_sent_total 2048000
# ...
```

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'duckd'
    static_configs:
      - targets: ['localhost:8080']
```

## Configuration

### Command line

```bash
./duckd-server \
  --host 0.0.0.0 \
  --port 5432 \
  --database /data/warehouse.duckdb \
  --executor-threads 8 \
  --max-connections 200 \
  --query-timeout 600000 \
  --http-port 8080 \
  --flight-port 8815 \
  --log-file /var/log/duckd/duckd.log \
  --log-level info
```

### Config file

```bash
./duckd-server --config /etc/duckd/config.yaml
```

DuckD supports both INI and YAML formats (auto-detected by file extension). See [`etc/duckd.conf.example`](etc/duckd.conf.example) and [`etc/config.yaml.example`](etc/config.yaml.example).

<details>
<summary><b>YAML example</b></summary>

```yaml
server:
  host: "0.0.0.0"
  port: 5432
  http_port: 8080
  flight_port: 8815

database:
  path: "/var/lib/duckd/data.duckdb"

logging:
  level: info
  file: "/var/log/duckd/duckd.log"

process:
  daemon: true
  pid_file: "/var/run/duckd/duckd.pid"
  user: duckd

threads:
  io: 0         # 0 = auto (CPU cores / 2)
  executor: 0   # 0 = auto (CPU cores)

limits:
  max_connections: 100
  max_memory: 0               # bytes, 0 = unlimited
  max_open_files: 65535
  query_timeout_ms: 300000    # 5 minutes
  session_timeout_minutes: 30

pool:
  min: 5
  max: 50
  idle_timeout_seconds: 300
  acquire_timeout_ms: 5000
```
</details>

<details>
<summary><b>INI example</b></summary>

```ini
host = 0.0.0.0
port = 5432
database = /var/lib/duckd/data.duckdb

http_port = 8080
io_threads = 4
executor_threads = 8
max_connections = 100
query_timeout_ms = 300000

log_file = /var/log/duckd/duckd.log
log_level = info

daemon = true
pid_file = /var/run/duckd/duckd.pid
user = duckd
```
</details>

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
- **Dual protocol.** The PostgreSQL protocol provides universal compatibility; Arrow Flight SQL adds a high-throughput binary path for analytics workloads that need columnar data at wire speed.
- **Zero external dependencies at runtime.** Single static binary. No JVM, no Python, no cluster coordinator.

### Source layout

```
src/
├── network/          # ASIO TCP server, per-connection I/O
├── protocol/
│   ├── pg/           # PostgreSQL wire protocol v3 (handler, reader, writer)
│   └── flight/       # Arrow Flight SQL server (opt-in)
├── session/          # Session lifecycle, SessionManager
├── executor/         # ExecutorPool — lock-free task queue + worker threads
├── http/             # /health and /metrics HTTP endpoints
├── logging/          # spdlog logger with DuckDB storage backend
├── config/           # ServerConfig, YAML/INI parser
├── client/
│   ├── remote-client/  # DuckDB extension (PG protocol)
│   └── duckd-client/   # DuckDB extension (Flight SQL)
├── programs/
│   ├── server/       # Server entry point
│   └── client/       # CLI client entry point
└── tests/            # Unit + integration tests
```

## Contributing

Contributions are welcome! Please:

1. Fork the repo and create a feature branch
2. Make sure all tests pass: `ctest --test-dir build --output-on-failure`
3. Open a pull request with a clear description

## License

[Apache License 2.0](LICENSE)
