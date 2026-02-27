# DuckD

A network server for [DuckDB](https://duckdb.org) that speaks the PostgreSQL wire protocol (v3), enabling any PostgreSQL-compatible client to connect to a shared DuckDB instance. Optionally also serves [Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) for high-throughput columnar data access.

```
psql -h localhost -p 5432 -U any -d any
```

## Why

DuckDB is an embedded database — it runs inside a single process. DuckD exposes it as a standalone server so multiple clients can share one database over standard network protocols.

**Use cases:**
- Shared analytics backend for BI tools (Metabase, Tableau, DBeaver, Grafana)
- Query Parquet / CSV / JSON files remotely via SQL
- Lightweight OLAP service without deploying a heavy data warehouse
- Federated queries across remote DuckDB instances via the client extension
- Development / staging analytics environment with multi-user access

## Features

| Feature | Status |
|---------|--------|
| PostgreSQL wire protocol v3 | ✅ |
| Simple and extended query protocol | ✅ |
| Prepared statements with parameters | ✅ |
| Transactions (BEGIN / COMMIT / ROLLBACK) | ✅ |
| COPY protocol (TO STDOUT / FROM STDIN) | ✅ |
| Query cancellation | ✅ |
| Configurable query timeout | ✅ |
| HTTP health check & Prometheus metrics | ✅ |
| Arrow Flight SQL (gRPC) | ✅ (opt-in) |
| DuckDB client extension (ATTACH remote) | ✅ (opt-in) |
| Daemon mode + privilege dropping | ✅ |
| systemd integration (Type=notify) | ✅ (Linux) |
| SIGHUP config reload | ✅ |
| SSL/TLS | planned |
| Authentication | planned (trust only) |

> **Security note:** DuckD currently does not implement authentication or TLS. Deploy behind a firewall or use a TLS-terminating proxy (e.g. stunnel, Nginx, HAProxy) for network-exposed deployments.

## Quick Start

```bash
# Clone with submodules
git clone --recursive https://github.com/jian-fang/duckd.git
cd duckd

# Build
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Run (in-memory database, default port 5432)
./build/programs/server/duckd-server

# Connect with any PostgreSQL client
psql -h localhost -p 5432 -U any -d any
```

## Building

**Requirements:** CMake >= 3.16, C++17 compiler, Git. All dependencies are vendored in `contrib/` as submodules.

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

# Debug build
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j$(nproc)

# Run tests
ctest --test-dir build --output-on-failure
```

## Usage Examples

### 1. In-Memory Analytics Server

Spin up a shared in-memory DuckDB that multiple users can query concurrently:

```bash
./duckd-server --port 5432 --http-port 8080
```

```sql
-- Any user can connect and create tables from files
psql -h localhost -p 5432 -U analyst -d analytics

CREATE TABLE sales AS SELECT * FROM read_parquet('/data/sales_2024.parquet');
CREATE TABLE users AS SELECT * FROM read_csv('/data/users.csv');

SELECT u.name, SUM(s.amount) AS total
FROM sales s JOIN users u ON s.user_id = u.id
GROUP BY u.name ORDER BY total DESC LIMIT 10;
```

### 2. Persistent Database Server

Run DuckD as a long-running service with a persistent database file:

```bash
./duckd-server \
  --database /var/lib/duckd/warehouse.duckdb \
  --port 5432 \
  --http-port 8080 \
  --executor-threads 8 \
  --max-connections 200 \
  --query-timeout 600000 \
  --log-file /var/log/duckd/duckd.log \
  --log-level info
```

### 3. Query Parquet / CSV Files Remotely

Connect from any PG client and query files the server can access:

```sql
-- Directly query Parquet files (no table creation needed)
SELECT * FROM read_parquet('/data/events/*.parquet') WHERE event_date >= '2024-01-01';

-- Query CSV with options
SELECT * FROM read_csv('/data/report.csv', header=true, delim='|');

-- Query remote files (S3, HTTP)
SELECT * FROM read_parquet('s3://my-bucket/data/sales.parquet');
SELECT * FROM read_parquet('https://example.com/data.parquet');

-- Create persistent tables from file scans
CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/**/*.parquet');
```

### 4. COPY Protocol

Use COPY for bulk data import/export:

```bash
# Export query results to a local file via psql
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY (SELECT * FROM sales WHERE year=2024) TO '/tmp/sales_2024.csv' WITH (FORMAT csv, HEADER)"

# Import CSV data from stdin
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY products FROM '/tmp/products.csv' WITH (FORMAT csv, HEADER)"
```

```python
# Python: bulk import with psycopg2 copy_expert
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", dbname="any")
cur = conn.cursor()

# Export
with open('/tmp/output.csv', 'w') as f:
    cur.copy_expert("COPY (SELECT * FROM sales) TO STDOUT WITH CSV HEADER", f)

# Import
with open('/tmp/data.csv', 'r') as f:
    cur.copy_expert("COPY target_table FROM STDIN WITH CSV HEADER", f)

conn.commit()
```

### 5. Connecting from Different Languages

**psql (CLI):**
```bash
psql -h localhost -p 5432 -U any -d any
```

**Python (psycopg2):**
```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", database="any")
cur = conn.cursor()
cur.execute("SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
rows = cur.fetchall()
for row in rows:
    print(row)
conn.close()
```

**Python (SQLAlchemy):**
```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://any:any@localhost:5432/any")
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM sales"))
    print(result.scalar())
```

**Node.js (pg):**
```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  user: 'any',
  database: 'any',
});

await client.connect();
const res = await client.query('SELECT * FROM read_parquet($1)', ['/data/sales.parquet']);
console.log(res.rows);
await client.end();
```

**Java (JDBC):**
```java
import java.sql.*;

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

**Go (pgx):**
```go
import "github.com/jackc/pgx/v5"

conn, _ := pgx.Connect(context.Background(),
    "postgres://any:any@localhost:5432/any")

rows, _ := conn.Query(context.Background(),
    "SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
```

**DBeaver / TablePlus / DataGrip:** Create a PostgreSQL connection to `localhost:5432`, any username/password.

### 6. Arrow Flight SQL

Enable high-throughput columnar data access via gRPC:

```bash
# Start server with Flight SQL enabled
./duckd-server --port 5432 --flight-port 8815 --http-port 8080
```

**Python (pyarrow):**
```python
from pyarrow.flight import FlightClient, FlightCallOptions
import pyarrow.flight

client = FlightClient("grpc://localhost:8815")

# Execute a query and get results as Arrow Table
info = client.get_flight_info(
    pyarrow.flight.FlightDescriptor.for_command(
        b'SELECT * FROM read_parquet("/data/sales.parquet")'
    )
)
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()
print(table.to_pandas())
```

### 7. DuckDB Client Extension (Federated Queries)

The `duckd-client` extension (built with `WITH_FLIGHT_SQL=ON`) lets any DuckDB instance ATTACH a remote DuckD server. This enables federated queries across local and remote data.

**ATTACH remote database:**
```sql
LOAD duckd_client;

-- Attach a remote DuckD instance
ATTACH 'grpc://remote-server:8815' AS warehouse (TYPE duckd);

-- Query remote tables as if they were local
SELECT * FROM warehouse.main.sales WHERE year = 2024;

-- Join local and remote data
SELECT l.*, r.customer_name
FROM local_orders l
JOIN warehouse.main.customers r ON l.customer_id = r.id;
```

**Execute DDL/DML on remote server:**
```sql
-- duckd_exec: execute statements, returns affected row count
SELECT duckd_exec('grpc://remote-server:8815', 'CREATE TABLE logs (ts TIMESTAMP, msg VARCHAR)');
SELECT duckd_exec('grpc://remote-server:8815', 'INSERT INTO logs VALUES (now(), ''hello'')');
SELECT duckd_exec('grpc://remote-server:8815', 'DELETE FROM logs WHERE ts < ''2024-01-01''');
```

**Ad-hoc remote queries:**
```sql
-- duckd_query: execute a query and return results as a table
SELECT * FROM duckd_query('grpc://remote-server:8815',
    'SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal
     FROM employees GROUP BY dept');

-- Aggregate remote results locally
SELECT dept, SUM(cnt) FROM duckd_query('grpc://remote-server:8815',
    'SELECT * FROM employees') GROUP BY dept;
```

### 8. Prepared Statements and Transactions

DuckD supports the full PostgreSQL extended query protocol:

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", dbname="any")
conn.autocommit = False
cur = conn.cursor()

try:
    # Prepared statements with parameters
    cur.execute("CREATE TABLE IF NOT EXISTS accounts (id INT, balance DECIMAL(10,2))")
    cur.execute("INSERT INTO accounts VALUES (%s, %s)", (1, 1000.00))
    cur.execute("INSERT INTO accounts VALUES (%s, %s)", (2, 2000.00))

    # Transaction: transfer funds
    cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (100, 1))
    cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (100, 2))

    conn.commit()
except Exception:
    conn.rollback()
    raise
```

### 9. Monitoring

```bash
# Health check (for load balancers)
curl http://localhost:8080/health
# {"status":"healthy","connections":3}

# Prometheus metrics
curl http://localhost:8080/metrics
# duckd_connections_active 3
# duckd_connections_total 42
# duckd_bytes_received_total 1024000
# duckd_bytes_sent_total 2048000
# duckd_sessions_active 3
# duckd_sessions_total 42
```

Integrate with Prometheus by adding to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'duckd'
    static_configs:
      - targets: ['localhost:8080']
```

## Configuration

### Command Line

```bash
./duckd-server \
  --host 0.0.0.0 \
  --port 5432 \
  --database /data/mydb.duckdb \
  --executor-threads 8 \
  --max-connections 100 \
  --query-timeout 300000 \
  --http-port 8080 \
  --flight-port 8815
```

### Config File

```bash
./duckd-server --config /etc/duckd/duckd.conf
```

**INI format** (`duckd.conf`):

```ini
host = 0.0.0.0
port = 5432
database = /var/lib/duckd/data.duckdb

http_port = 8080
io_threads = 4
executor_threads = 8
max_connections = 100
query_timeout_ms = 300000
session_timeout_minutes = 30

log_file = /var/log/duckd/duckd.log
log_level = info

daemon = true
pid_file = /var/run/duckd/duckd.pid
user = duckd
```

**YAML format** (`config.yaml`):

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
  daemon: false
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
  validate_on_acquire: true
```

Format is auto-detected by file extension (`.yaml`/`.yml` = YAML, otherwise INI).

### All Options

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `5432` | PostgreSQL protocol port |
| `--database` | `:memory:` | Database file path |
| `--io-threads` | auto | IO thread count (CPU cores / 2) |
| `--executor-threads` | auto | Query executor thread count (CPU cores) |
| `--max-connections` | `100` | Maximum concurrent connections |
| `--query-timeout` | `300000` | Query timeout in milliseconds |
| `--http-port` | disabled | HTTP port for health/metrics |
| `--flight-port` | disabled | Arrow Flight SQL port (gRPC) |
| `--max-memory` | unlimited | Max memory in bytes (Linux only) |
| `--max-open-files` | system | File descriptor limit |
| `--daemon` | `false` | Run as background daemon |
| `--pid-file` | — | PID file path |
| `--log-file` | stderr | Log file path |
| `--log-level` | `info` | `debug`, `info`, `warn`, `error` |
| `--user` | — | Drop privileges to this user |
| `--config` | — | Config file path |
| `--version` | — | Show version info |

## Signals

| Signal | Action |
|--------|--------|
| `SIGTERM`, `SIGINT` | Graceful shutdown |
| `SIGHUP` | Reload config (log level changes take effect immediately) |

## Production Deployment

### systemd

```ini
[Unit]
Description=DuckD Server
After=network.target

[Service]
Type=notify
User=duckd
ExecStart=/usr/local/bin/duckd-server --config /etc/duckd/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

### Docker (example)

```dockerfile
FROM ubuntu:24.04
COPY build/programs/server/duckd-server /usr/local/bin/
EXPOSE 5432 8080 8815
ENTRYPOINT ["duckd-server"]
CMD ["--database", "/data/warehouse.duckdb", "--http-port", "8080"]
```

```bash
docker run -d -p 5432:5432 -p 8080:8080 \
  -v /data:/data \
  duckd --database /data/warehouse.duckdb --http-port 8080
```

## Architecture

```
Client (psql / JDBC / psycopg2 / Arrow Flight)
    │
    │  PostgreSQL wire protocol    Arrow Flight SQL (gRPC)
    ▼                              ▼
TcpServer ── PgHandler        FlightSqlServer
                │                   │
                └───────┬───────────┘
                        ▼
                  ExecutorPool (lock-free queue, N worker threads)
                        │
                        ▼
                  SessionManager ──► Session (1 DuckDB connection per session)
                        │
                        ▼
                      DuckDB
```

- **Sticky sessions:** each connection gets its own DuckDB connection, held for the session lifetime. Transactions and prepared statements are pinned to the session.
- **Async I/O:** network I/O is non-blocking via ASIO; queries run on a separate executor thread pool.
- **Lock-free queue:** tasks are submitted to the executor via a moodycamel concurrent queue.
- **Query cancellation:** propagates to DuckDB's interrupt mechanism.
- **Crash safety:** SIGSEGV/SIGABRT handlers print stack traces before exit.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
