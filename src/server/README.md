# DuckDB Server

A network interface for DuckDB, enabling remote access to DuckDB databases.

## Features

- **High Performance**: Asio-based asynchronous networking
- **Arrow IPC**: Efficient data serialization using Apache Arrow format
- **Session Management**: Connection pooling and session state management
- **Streaming Results**: Large result sets returned in batches
- **Cross-Platform**: Works on Linux, macOS, and Windows

## Building

### As Part of DuckDB

```bash
cd /path/to/duckdb
mkdir build && cd build
cmake .. -DBUILD_SERVER=ON
make -j
```

### Standalone Build

```bash
cd /path/to/duckdb/src/server
mkdir build && cd build
cmake .. -DDUCKDB_DIR=/path/to/duckdb
make -j
```

## Usage

### Starting the Server

```bash
# Basic usage (in-memory database)
./duckdb-server

# With options
./duckdb-server --host 0.0.0.0 --port 9999 --database /path/to/db.duckdb

# With config file
./duckdb-server -c /path/to/config.toml
```

### Command Line Options

```
  -c, --config <FILE>       Configuration file path
  -h, --host <HOST>         Listen address (default: 0.0.0.0)
  -p, --port <PORT>         Listen port (default: 9999)
  -d, --database <PATH>     Database path (default: :memory:)
  --io-threads <NUM>        IO thread count (default: CPU cores)
  --executor-threads <NUM>  Executor thread count (default: CPU cores * 2)
  --log-level <LEVEL>       Log level: trace/debug/info/warn/error (default: info)
  --help                    Show help
  --version                 Show version
```

### Environment Variables

```bash
DUCKDB_SERVER_HOST=0.0.0.0
DUCKDB_SERVER_PORT=9999
DUCKDB_SERVER_DATABASE=/path/to/db.duckdb
DUCKDB_SERVER_LOG_LEVEL=info
```

## Configuration

See `config/duckdb-server.toml` for a sample configuration file.

## Python Client

```python
from duckdb_client import DuckDBClient

# Connect
client = DuckDBClient('localhost', 9999)
client.connect()

# Execute query
result = client.query("SELECT * FROM my_table")
print(result.rows)

# Close
client.close()

# Or use context manager
with DuckDBClient('localhost', 9999) as client:
    result = client.query("SELECT 1 + 1 AS result")
```

## Protocol

DuckDB Server uses a custom binary protocol for efficient communication.

### Message Format

```
+------------+----------+--------+-------+----------+--------+
|   Magic    | Version  |  Type  | Flags | Reserved | Length |
|  (4 bytes) | (1 byte) |(1 byte)|(1 byte)| (1 byte)|(4 bytes)|
+------------+----------+--------+-------+----------+--------+
|                     Payload (variable)                     |
+------------------------------------------------------------+
```

### Message Types

| Type | Code | Description |
|------|------|-------------|
| HELLO | 0x01 | Client handshake |
| HELLO_RESPONSE | 0x02 | Server handshake response |
| PING | 0x03 | Heartbeat request |
| PONG | 0x04 | Heartbeat response |
| QUERY | 0x10 | SQL query |
| RESULT_HEADER | 0x40 | Result schema (Arrow) |
| RESULT_BATCH | 0x41 | Result data batch (Arrow) |
| RESULT_END | 0x42 | Result end with statistics |

## Architecture

```
                        +-------------------+
                        |   Client (Python) |
                        +-------------------+
                                 |
                        DuckDB Protocol (TCP)
                                 |
+---------------------------------------------------------------+
|                       DuckDB Server                           |
|  +------------------+  +------------------+  +---------------+ |
|  |  Network Layer   |  |  Protocol Layer  |  | Session Layer | |
|  |    (Asio)        |  |  (Message Parse) |  | (Management)  | |
|  +------------------+  +------------------+  +---------------+ |
|                                |                               |
|  +------------------+  +------------------+  +---------------+ |
|  | Executor Pool    |  | Arrow Serializer |  |    Logger     | |
|  +------------------+  +------------------+  +---------------+ |
+---------------------------------------------------------------+
                                 |
                        DuckDB C++ API
                                 |
                     +-------------------+
                     |   DuckDB Core     |
                     +-------------------+
```

## License

Same license as DuckDB (MIT License).
