# DuckD

DuckDB Server - A production-ready network server for DuckDB with PostgreSQL wire protocol support.

## Features

- **PostgreSQL Wire Protocol** - Compatible with psql, DBeaver, and other PostgreSQL clients
- Native binary protocol with Arrow IPC serialization
- Production-ready server features (daemon mode, privilege dropping, resource limits)
- Session management with connection pooling
- Health check and metrics endpoints
- Remote client extension for DuckDB
- Interactive CLI client

## Quick Start

```bash
# Build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j 8

# Start server (PostgreSQL protocol on port 5432)
./programs/server/duckd-server

# Connect with psql
psql -h localhost -p 5432 -U any_user -d any_db

# Or connect with any PostgreSQL client
```

## Project Structure

```
duckd/
├── contrib/
│   ├── duckdb/              # DuckDB (git submodule)
│   └── asio/                # ASIO networking library
├── src/
│   ├── network/             # TCP server, connection handling
│   ├── protocol/            # Native protocol
│   │   └── pg/              # PostgreSQL wire protocol
│   ├── session/             # Session management
│   ├── executor/            # Query execution
│   ├── serialization/       # Arrow serialization
│   ├── http/                # Health/metrics HTTP server
│   ├── config/              # Configuration
│   └── client/              # Client libraries
├── programs/
│   ├── server/              # duckd-server entry point
│   └── client/              # duckd-cli entry point
├── tests/
│   ├── unit/                # Unit tests
│   └── integration/         # Integration tests
└── etc/                     # Example configurations
```

## Building

```bash
# Clone with submodules
git clone --recursive https://github.com/user/duckd.git
cd duckd

# Or initialize submodules after clone
git submodule update --init --recursive

# Build (Release)
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j 8

# Build (Debug)
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . -j 8

# Run tests
ctest --output-on-failure
```

## Server Usage

### Basic Usage

```bash
# Start with default settings (PostgreSQL protocol, port 5432, in-memory database)
./duckd-server

# Specify database file
./duckd-server --database /path/to/mydb.duckdb

# Custom port
./duckd-server --port 15432
```

### Production Deployment

```bash
# Run as daemon with all production features
./duckd-server \
    --daemon \
    --pid-file /var/run/duckd.pid \
    --log-file /var/log/duckd.log \
    --log-level info \
    --database /data/mydb.duckdb \
    --user duckd \
    --http-port 8080 \
    --max-connections 200 \
    --max-memory 8589934592
```

### Configuration File

Create `/etc/duckd/duckd.conf`:

```ini
# Network
host = 0.0.0.0
port = 5432
protocol = postgresql

# Database
database = /data/mydb.duckdb

# Logging
log_file = /var/log/duckd.log
log_level = info

# Process
pid_file = /var/run/duckd.pid
user = duckd
daemon = true

# Resources
io_threads = 4
executor_threads = 8
max_connections = 200
max_memory = 8589934592
max_open_files = 65535
query_timeout_ms = 300000

# HTTP (health/metrics)
http_port = 8080
```

Start with config:

```bash
./duckd-server --config /etc/duckd/duckd.conf
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `-c, --config` | - | Config file path |
| `-h, --host` | 0.0.0.0 | Host to bind |
| `-p, --port` | 5432 | Port to bind |
| `-d, --database` | :memory: | Database path |
| `--protocol` | postgresql | Protocol: postgresql, native |
| `--daemon` | false | Run as daemon |
| `--pid-file` | - | PID file path |
| `--user` | - | User to run as (drops privileges) |
| `--log-file` | - | Log file path |
| `--log-level` | info | Log level: debug, info, warn, error |
| `--io-threads` | auto | IO thread count |
| `--executor-threads` | auto | Executor thread count |
| `--max-connections` | 100 | Maximum connections |
| `--http-port` | disabled | HTTP port for health/metrics |
| `--max-memory` | unlimited | Max memory in bytes |
| `--max-open-files` | system | Max file descriptors |
| `--query-timeout` | 300000 | Query timeout in ms |

### Health & Metrics

When `--http-port` is enabled:

```bash
# Health check
curl http://localhost:8080/health
# Returns: {"status":"healthy"}

# Prometheus metrics
curl http://localhost:8080/metrics
# Returns Prometheus format metrics
```

### Signals

| Signal | Action |
|--------|--------|
| SIGTERM, SIGINT | Graceful shutdown |
| SIGHUP | Reload configuration |

## PostgreSQL Protocol Support

DuckD implements PostgreSQL wire protocol v3.0, allowing standard PostgreSQL clients to connect.

### Connecting with psql

```bash
psql -h localhost -p 5432 -U myuser -d mydb
```

### Connecting with Python (psycopg2)

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="myuser",
    database="mydb"
)
cur = conn.cursor()
cur.execute("SELECT * FROM my_table")
rows = cur.fetchall()
```

### Connecting with DBeaver

1. Create new connection → PostgreSQL
2. Host: localhost, Port: 5432
3. Database: any name (DuckDB uses single database)
4. User: any name (authentication not enforced)

### Protocol Features

| Feature | Status |
|---------|--------|
| Simple Query Protocol | ✅ Supported |
| Extended Query Protocol | ✅ Supported |
| Prepared Statements | ✅ Supported |
| Parameter Binding | ✅ Supported |
| Transaction Support | ✅ Supported |
| Error Handling | ✅ Supported |
| SSL/TLS | ❌ Not yet |
| Authentication | ❌ Trust mode only |
| COPY Protocol | ❌ Not yet |

### Supported SQL

All DuckDB SQL is supported through the PostgreSQL protocol:

```sql
-- DDL
CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR);
DROP TABLE users;

-- DML
INSERT INTO users VALUES (1, 'Alice');
UPDATE users SET name = 'Bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;

-- Queries
SELECT * FROM users WHERE name LIKE 'A%';
SELECT COUNT(*), AVG(score) FROM results GROUP BY category;

-- DuckDB extensions
SELECT * FROM read_parquet('data.parquet');
SELECT * FROM read_csv('data.csv');
COPY users TO 'users.parquet' (FORMAT PARQUET);
```

## CLI Client Usage

```bash
# Connect to server
./duckd-cli localhost 5432

# Execute queries
D> SELECT 1 + 1;
D> CREATE TABLE test (id INT, name TEXT);
D> .tables
D> .help
D> .quit
```

## Python Client

```python
from duckdb_client import DuckDBClient

client = DuckDBClient('localhost', 9999)  # Native protocol
result = client.query('SELECT * FROM test')
print(result)
```

## Testing

```bash
# Run all tests
cd build && ctest --output-on-failure

# Run PostgreSQL protocol tests only
ctest -R "test_pg_" --output-on-failure

# Run specific test
./tests/test_pg_integration
```

## systemd Integration

Create `/etc/systemd/system/duckd.service`:

```ini
[Unit]
Description=DuckD Server
After=network.target

[Service]
Type=notify
User=duckd
Group=duckd
ExecStart=/usr/local/bin/duckd-server --config /etc/duckd/duckd.conf
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable duckd
sudo systemctl start duckd
sudo systemctl status duckd
```

## License

Apache License 2.0 - See [LICENSE](LICENSE) for details.
