# DuckD

DuckDB Server - A network server for DuckDB with client libraries.

## Features

- TCP server for DuckDB with custom binary protocol
- Arrow IPC serialization for efficient data transfer
- Session management with connection pooling
- Remote client extension for DuckDB
- Interactive CLI client

## Project Structure

```
duckd/
├── contrib/
│   ├── duckdb/          # DuckDB (git submodule)
│   └── asio/            # ASIO networking library
├── src/
│   ├── duckd/           # Server library
│   │   ├── network/     # TCP server, connection handling
│   │   ├── protocol/    # Message protocol
│   │   ├── session/     # Session management
│   │   ├── executor/    # Query execution
│   │   └── serialization/  # Arrow serialization
│   └── duckd-client/    # Client library
│       ├── remote-client/  # Remote client implementation
│       ├── cli/         # CLI client
│       └── python/      # Python client
├── programs/
│   ├── server/          # duckd-server entry point
│   └── client/          # duckd-client entry point
├── tests/               # Tests
└── scripts/             # Build scripts
```

## Building

```bash
# Clone with submodules
git clone --recursive https://github.com/user/duckd.git

# Or initialize submodules after clone
git submodule update --init --recursive

# Build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j 8

# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake --build . -j 8
```

## Usage

### Server

```bash
# Start server
./build/programs/server/duckd-server

# With options
./build/programs/server/duckd-server \
    --host 0.0.0.0 \
    --port 9999 \
    --database /path/to/db.duckdb
```

### CLI Client

```bash
# Connect to server
./build/programs/client/duckd-client localhost 9999

# Execute queries
D> SELECT 1 + 1;
D> CREATE TABLE test (id INT, name TEXT);
D> .tables
D> .help
```

### Python Client

```python
from duckdb_client import DuckDBClient

client = DuckDBClient('localhost', 9999)
result = client.query('SELECT * FROM test')
print(result)
```

## License

MIT License
