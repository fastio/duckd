# DuckD Usage Examples

Comprehensive examples for connecting to DuckD, querying data, and using advanced features.

## Table of Contents

- [Querying Files Over the Network](#querying-files-over-the-network)
- [Connecting from Different Languages](#connecting-from-different-languages)
- [Bulk Data with COPY](#bulk-data-with-copy)
- [Transactions and Prepared Statements](#transactions-and-prepared-statements)
- [Arrow Flight SQL](#arrow-flight-sql)
- [DuckDB Native SDK — Federated Queries](#duckdb-native-sdk--federated-queries)
- [Monitoring](#monitoring)
- [Configuration Examples](#configuration-examples)

---

## Querying Files Over the Network

Connect from any PostgreSQL client and query files the server has access to — Parquet, CSV, JSON, and even remote files on S3 or HTTP:

```sql
-- Parquet (local, glob, remote)
SELECT * FROM read_parquet('/data/events/*.parquet') WHERE event_date >= '2024-01-01';
SELECT * FROM read_parquet('s3://my-bucket/sales/**/*.parquet');
SELECT * FROM read_parquet('https://example.com/dataset.parquet');

-- CSV with options
SELECT * FROM read_csv('/data/report.csv', header=true, delim='|');

-- JSON
SELECT * FROM read_json('/data/logs/*.json');

-- Build persistent tables from file scans
CREATE TABLE events AS SELECT * FROM read_parquet('/data/events/**/*.parquet');

-- Cross-format joins
SELECT e.event_id, u.name
FROM read_parquet('/data/events.parquet') e
JOIN read_csv('/data/users.csv', header=true) u ON e.user_id = u.id;
```

---

## Connecting from Different Languages

DuckD speaks standard PostgreSQL. Switch the connection string, keep everything else.

### psql

```bash
psql -h localhost -p 5432 -U any -d any
```

### Python (psycopg2)

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", database="any")
cur = conn.cursor()
cur.execute("SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
for row in cur.fetchall():
    print(row)
conn.close()
```

### Python (SQLAlchemy)

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

### Python (Pandas)

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql://any:any@localhost:5432/any")
df = pd.read_sql("SELECT * FROM read_parquet('/data/sales.parquet')", engine)
print(df.describe())
```

### Node.js (pg)

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

### Java (JDBC)

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

### Go (pgx)

```go
package main

import (
    "context"
    "fmt"
    "github.com/jackc/pgx/v5"
)

func main() {
    conn, err := pgx.Connect(context.Background(),
        "postgres://any:any@localhost:5432/any")
    if err != nil {
        panic(err)
    }
    defer conn.Close(context.Background())

    rows, err := conn.Query(context.Background(),
        "SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        values, _ := rows.Values()
        fmt.Println(values)
    }
}
```

### Rust (tokio-postgres)

```rust
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5432 user=any dbname=any", NoTls
    ).await.unwrap();

    tokio::spawn(async move { connection.await.unwrap(); });

    let rows = client.query(
        "SELECT * FROM read_parquet('/data/sales.parquet') LIMIT 10", &[]
    ).await.unwrap();

    for row in &rows {
        let product: &str = row.get(0);
        println!("{}", product);
    }
}
```

### GUI Tools

DBeaver, TablePlus, DataGrip, Metabase, Grafana, Superset — create a PostgreSQL connection to `localhost:5432`, any username/password.

---

## Bulk Data with COPY

### psql

```bash
# Export query results to a local CSV
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY (SELECT * FROM sales WHERE year=2024) TO '/tmp/sales.csv' WITH (FORMAT csv, HEADER)"

# Import from CSV
psql -h localhost -p 5432 -U any -d any \
  -c "\COPY products FROM '/tmp/products.csv' WITH (FORMAT csv, HEADER)"
```

### Python (psycopg2)

```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, user="any", dbname="any")
cur = conn.cursor()

# Export to CSV
with open('/tmp/output.csv', 'w') as f:
    cur.copy_expert("COPY (SELECT * FROM sales) TO STDOUT WITH CSV HEADER", f)

# Import from CSV
with open('/tmp/data.csv', 'r') as f:
    cur.copy_expert("COPY target_table FROM STDIN WITH CSV HEADER", f)

conn.commit()
conn.close()
```

---

## Transactions and Prepared Statements

DuckD supports the full PostgreSQL extended query protocol — transactions, prepared statements, and parameterized queries all work as expected:

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
finally:
    conn.close()
```

---

## Arrow Flight SQL

Enable high-throughput columnar data access via gRPC. Ideal for data science workloads and large result sets where row-by-row PG protocol is a bottleneck.

### Start the server with Flight SQL

```bash
./duckd-server --port 5432 --flight-port 8815 --http-port 8080
```

### Python (pyarrow)

```python
import pyarrow.flight

client = pyarrow.flight.FlightClient("grpc://localhost:8815")

# Execute a query
info = client.get_flight_info(
    pyarrow.flight.FlightDescriptor.for_command(
        b'SELECT * FROM read_parquet("/data/sales.parquet")'
    )
)

# Stream results as Arrow Table (zero-copy)
reader = client.do_get(info.endpoints[0].ticket)
table = reader.read_all()

# Convert to Pandas
df = table.to_pandas()
print(df.describe())
```

### Python (ADBC)

```python
import adbc_driver_flightsql.dbapi as flight_sql

conn = flight_sql.connect(uri="grpc://localhost:8815")
cur = conn.cursor()

cur.execute("SELECT * FROM read_parquet('/data/sales.parquet')")
table = cur.fetch_arrow_table()
print(table.to_pandas())

conn.close()
```

---

## DuckDB Native SDK — Federated Queries

The `duckd-client` extension (built with `-DWITH_FLIGHT_SQL=ON`) is a **native DuckDB extension** that lets any DuckDB instance treat a remote DuckD server as a local database. Data moves between DuckDB instances in native Arrow columnar format — no protocol translation, no row-by-row conversion.

### ATTACH remote servers

```sql
LOAD duckd_client;

-- Attach a remote DuckD server — it shows up as a regular catalog
ATTACH 'grpc://analytics-server:8815' AS warehouse (TYPE duckd);

-- Query remote tables with native DuckDB syntax
SELECT * FROM warehouse.main.sales WHERE region = 'APAC' AND year = 2024;

-- Join local and remote data seamlessly
SELECT o.order_id, o.total, c.name, c.tier
FROM local_orders o
JOIN warehouse.main.customers c ON o.customer_id = c.id
WHERE c.tier = 'enterprise';
```

### Execute DDL/DML on remote servers

```sql
-- duckd_exec: run statements, returns affected row count
SELECT duckd_exec('grpc://server:8815', 'CREATE TABLE logs (ts TIMESTAMP, msg VARCHAR)');
SELECT duckd_exec('grpc://server:8815', 'INSERT INTO logs VALUES (now(), ''deployed v2.1'')');
SELECT duckd_exec('grpc://server:8815', 'DELETE FROM logs WHERE ts < ''2024-01-01''');
```

### Ad-hoc remote queries

```sql
-- duckd_query: run a query, returns results as a DuckDB table function
SELECT * FROM duckd_query('grpc://server:8815',
    'SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal
     FROM employees GROUP BY dept ORDER BY avg_sal DESC');

-- Aggregate remote results locally
SELECT region, SUM(total) FROM duckd_query('grpc://server:8815',
    'SELECT * FROM sales') GROUP BY region;
```

### Multi-server federation

```sql
LOAD duckd_client;

-- Attach multiple remote servers
ATTACH 'grpc://us-server:8815' AS us_data (TYPE duckd);
ATTACH 'grpc://eu-server:8815' AS eu_data (TYPE duckd);

-- Query across regions
SELECT 'US' as region, COUNT(*) as orders FROM us_data.main.orders
UNION ALL
SELECT 'EU' as region, COUNT(*) as orders FROM eu_data.main.orders;
```

---

## Monitoring

### Health check

Use in load balancer probes (K8s liveness/readiness, ALB health checks):

```bash
curl http://localhost:8080/health
# {"status":"healthy","connections":3}
```

### Prometheus metrics

```bash
curl http://localhost:8080/metrics
# duckd_connections_active 3
# duckd_connections_total 42
# duckd_bytes_received_total 1024000
# duckd_bytes_sent_total 2048000
# duckd_sessions_active 3
# duckd_sessions_total 42
```

### Prometheus integration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'duckd'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
```

---

## Configuration Examples

### In-memory analytics server

```bash
./duckd-server --port 5432 --http-port 8080
```

### Persistent database with monitoring

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

### Full-featured server (PG + Flight SQL + HTTP)

```bash
./duckd-server \
  --database /var/lib/duckd/warehouse.duckdb \
  --port 5432 \
  --flight-port 8815 \
  --http-port 8080 \
  --executor-threads 8 \
  --max-connections 200 \
  --log-file /var/log/duckd/duckd.log
```

### YAML config file

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

### INI config file

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
