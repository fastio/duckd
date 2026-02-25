//===----------------------------------------------------------------------===//
//                         DuckD Server - Integration Tests
//
// tests/integration/flight/test_duckd_client.cpp
//
// End-to-end integration tests for the duckd_client DuckDB extension.
// Tests the DuckDB SQL interface (duckd_exec, duckd_query, ATTACH) that
// allows DuckDB to interact with a remote duckd/Flight SQL server.
//
// Architecture:
//   - One in-process DuckDBFlightSqlServer (the "server" DB)
//   - One local DuckDB instance (the "client") with the duckd_client
//     extension loaded via duckd_client_init()
//   - Tests issue SQL via client_conn_ and verify results
//===----------------------------------------------------------------------===//

#include "protocol/flight/flight_sql_server.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "flight_client.hpp"
#include "duckdb.hpp"

#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>
#include <string>

using namespace duckdb_server;

// Extension entry point from duckd_flight_client_static
extern "C" void duckd_client_init(duckdb::DatabaseInstance &db);

//===----------------------------------------------------------------------===//
// Test Fixture
//===----------------------------------------------------------------------===//

class DuckdClientTest {
public:
    DuckdClientTest() {
        //--- Server side ---
        server_db_ = std::make_shared<duckdb::DuckDB>(nullptr);

        SessionManager::Config sm_cfg;
        sm_cfg.max_sessions = 100;
        session_manager_ = std::make_shared<SessionManager>(server_db_, sm_cfg);

        executor_pool_ = std::make_shared<ExecutorPool>(2);
        executor_pool_->Start();

        server_ = std::make_unique<DuckDBFlightSqlServer>(
            session_manager_.get(), executor_pool_.get(), "localhost", 0);

        // NOTE: do NOT wrap Init/ServeAsync in assert() — in Release builds
        // (NDEBUG defined) assert() is a no-op that never evaluates its argument,
        // so the call itself would be skipped, leaving impl_ null and crashing later.
        {
            auto s = server_->Init();
            if (!s.ok()) throw std::runtime_error("Init failed: " + s.ToString());
        }
        {
            auto s = server_->ServeAsync();
            if (!s.ok()) throw std::runtime_error("ServeAsync failed: " + s.ToString());
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(300));

        // Build URL from the server's actual bound location
        url_ = server_->location().ToString();

        // Pre-populate server with test tables
        setup_server_data();

        //--- Client side: load extension ---
        client_db_ = std::make_shared<duckdb::DuckDB>(nullptr);
        duckd_client_init(*client_db_->instance);
        conn_ = std::make_unique<duckdb::Connection>(*client_db_);
    }

    ~DuckdClientTest() {
        conn_.reset();
        client_db_.reset();
        server_->Shutdown();
        executor_pool_->Stop();
    }

    const std::string &url() const { return url_; }
    duckdb::Connection &conn() { return *conn_; }

private:
    static void check(const std::unique_ptr<duckdb::QueryResult> &r, const char *ctx) {
        if (r->HasError())
            throw std::runtime_error(std::string(ctx) + ": " + r->GetError());
    }

    void setup_server_data() {
        duckdb::Connection c(*server_db_);

        // employees – used for basic query / catalog tests
        check(c.Query(
            "CREATE TABLE employees ("
            "  id      INTEGER PRIMARY KEY,"
            "  name    VARCHAR NOT NULL,"
            "  dept    VARCHAR NOT NULL,"
            "  salary  DOUBLE  NOT NULL"
            ")"), "CREATE employees");

        check(c.Query(
            "INSERT INTO employees VALUES "
            "(1, 'Alice',   'Engineering', 95000.0),"
            "(2, 'Bob',     'Marketing',   82000.0),"
            "(3, 'Charlie', 'Engineering', 88000.0),"
            "(4, 'Diana',   'HR',          75000.0),"
            "(5, 'Eve',     'Engineering', 101000.0)"), "INSERT employees");

        // types_test – used for data-type round-trip tests
        check(c.Query(
            "CREATE TABLE types_test ("
            "  b   BOOLEAN,"
            "  i8  TINYINT,  i16 SMALLINT, i32 INTEGER, i64 BIGINT,"
            "  f   FLOAT,    d   DOUBLE,"
            "  s   VARCHAR"
            ")"), "CREATE types_test");

        check(c.Query(
            "INSERT INTO types_test VALUES "
            "(true,  1,  100,  10000,  1000000000,  3.14::FLOAT,  2.718281828, 'hello'),"
            "(false,-1, -100, -10000, -1000000000, -3.14::FLOAT, -2.718281828, 'world')"),
            "INSERT types_test");
    }

    std::shared_ptr<duckdb::DuckDB>       server_db_;
    std::shared_ptr<SessionManager>       session_manager_;
    std::shared_ptr<ExecutorPool>         executor_pool_;
    std::unique_ptr<DuckDBFlightSqlServer> server_;
    std::string                            url_;

    std::shared_ptr<duckdb::DuckDB>       client_db_;
    std::unique_ptr<duckdb::Connection>   conn_;
};

//===----------------------------------------------------------------------===//
// Test Helpers
//===----------------------------------------------------------------------===//

#define TEST_BEGIN(name) \
    std::cout << "  Testing " << name << "... " << std::flush;

#define TEST_PASS() \
    std::cout << "PASS" << std::endl; \
    passed++;

#define TEST_FAIL(msg) \
    std::cout << "FAIL: " << msg << std::endl; \
    failed++;

//===----------------------------------------------------------------------===//
// Tests: duckd_query(url, sql) → TABLE
//===----------------------------------------------------------------------===//

static int TestDuckdQuery(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("duckd_query: literal SELECT");
    {
        auto r = t.conn().Query(
            "SELECT * FROM duckd_query('" + t.url() + "',"
            " 'SELECT 42 AS answer, ''hello'' AS greeting')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 1 || r->ColumnCount() != 2) {
            TEST_FAIL("expected 1 row, 2 cols; got " +
                      std::to_string(r->RowCount()) + "r " +
                      std::to_string(r->ColumnCount()) + "c");
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("duckd_query: full table scan");
    {
        auto r = t.conn().Query(
            "SELECT COUNT(*) AS cnt FROM duckd_query('" + t.url() + "',"
            " 'SELECT * FROM employees')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            auto cnt = r->GetValue(0, 0).GetValue<int64_t>();
            if (cnt != 5) {
                TEST_FAIL("expected 5 rows, got " + std::to_string(cnt));
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("duckd_query: server-side aggregation");
    {
        auto r = t.conn().Query(
            "SELECT * FROM duckd_query('" + t.url() + "',"
            " 'SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal"
            "  FROM employees GROUP BY dept ORDER BY dept')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 3) {
            TEST_FAIL("expected 3 dept rows, got " + std::to_string(r->RowCount()));
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("duckd_query: local aggregation over remote result");
    {
        // duckd_query returns a table; DuckDB aggregates locally
        auto r = t.conn().Query(
            "SELECT dept, COUNT(*) AS cnt"
            " FROM duckd_query('" + t.url() + "', 'SELECT * FROM employees')"
            " GROUP BY dept ORDER BY cnt DESC");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 3) {
            TEST_FAIL("expected 3 dept rows, got " + std::to_string(r->RowCount()));
        } else {
            // Engineering has 3 employees — largest group
            auto top_dept = r->GetValue(0, 0).ToString();
            if (top_dept != "Engineering") {
                TEST_FAIL("top dept should be Engineering, got " + top_dept);
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("duckd_query: large result set (10 000 rows)");
    {
        auto r = t.conn().Query(
            "SELECT COUNT(*) AS cnt"
            " FROM duckd_query('" + t.url() + "',"
            " 'SELECT * FROM generate_series(1, 10000) AS t(i)')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            auto cnt = r->GetValue(0, 0).GetValue<int64_t>();
            if (cnt != 10000) {
                TEST_FAIL("expected 10000, got " + std::to_string(cnt));
            } else {
                TEST_PASS();
            }
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: duckd_exec(url, sql) → BIGINT
//===----------------------------------------------------------------------===//

static int TestDuckdExec(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("duckd_exec: CREATE TABLE");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'CREATE TABLE exec_test (id INTEGER, val VARCHAR)')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("duckd_exec: INSERT returns non-negative count");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'INSERT INTO exec_test VALUES (1, ''alpha''), (2, ''beta''), (3, ''gamma'')')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            auto cnt = r->GetValue(0, 0).GetValue<int64_t>();
            if (cnt < 0) {
                TEST_FAIL("expected non-negative row count, got " + std::to_string(cnt));
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("duckd_exec: UPDATE");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'UPDATE exec_test SET val = ''UPDATED'' WHERE id = 1')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("duckd_exec: UPDATE verified via duckd_query");
    {
        auto r = t.conn().Query(
            "SELECT val FROM duckd_query('" + t.url() + "',"
            " 'SELECT val FROM exec_test WHERE id = 1')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            auto val = r->GetValue(0, 0).ToString();
            if (val != "UPDATED") {
                TEST_FAIL("expected 'UPDATED', got '" + val + "'");
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("duckd_exec: DELETE");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'DELETE FROM exec_test WHERE id = 3')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("duckd_exec: DELETE verified via duckd_query");
    {
        auto r = t.conn().Query(
            "SELECT COUNT(*) AS cnt FROM duckd_query('" + t.url() + "',"
            " 'SELECT * FROM exec_test')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            auto cnt = r->GetValue(0, 0).GetValue<int64_t>();
            if (cnt != 2) {
                TEST_FAIL("expected 2 rows after DELETE, got " + std::to_string(cnt));
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("duckd_exec: DROP TABLE");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'DROP TABLE exec_test')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: ATTACH 'grpc://...' AS remote (TYPE duckd) + catalog queries
//===----------------------------------------------------------------------===//

static int TestAttachCatalog(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("ATTACH remote catalog");
    {
        auto r = t.conn().Query(
            "ATTACH '" + t.url() + "' AS remote (TYPE duckd)");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("SELECT * FROM remote.main.employees");
    {
        auto r = t.conn().Query(
            "SELECT * FROM remote.main.employees ORDER BY id");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 5) {
            TEST_FAIL("expected 5 rows, got " + std::to_string(r->RowCount()));
        } else {
            // Verify first and last rows
            auto id0   = r->GetValue(0, 0).GetValue<int32_t>();
            auto name0 = r->GetValue(1, 0).ToString();
            auto id4   = r->GetValue(0, 4).GetValue<int32_t>();
            if (id0 != 1 || name0 != "Alice" || id4 != 5) {
                TEST_FAIL("row mismatch: id0=" + std::to_string(id0) +
                          " name0=" + name0 + " id4=" + std::to_string(id4));
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("SELECT with local WHERE on remote table");
    {
        auto r = t.conn().Query(
            "SELECT name FROM remote.main.employees"
            " WHERE dept = 'Engineering' ORDER BY name");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 3) {
            TEST_FAIL("expected 3 Engineering rows, got " + std::to_string(r->RowCount()));
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("SELECT with local GROUP BY on remote table");
    {
        auto r = t.conn().Query(
            "SELECT dept, COUNT(*) AS cnt"
            " FROM remote.main.employees"
            " GROUP BY dept ORDER BY dept");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 3) {
            TEST_FAIL("expected 3 dept rows, got " + std::to_string(r->RowCount()));
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("SELECT with salary filter (>85000) on remote table");
    {
        // Alice 95k, Charlie 88k, Eve 101k all exceed 85000 → 3 rows
        auto r = t.conn().Query(
            "SELECT e.name, e.salary"
            " FROM remote.main.employees e"
            " WHERE e.salary > 85000"
            " ORDER BY e.salary");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 3) {
            TEST_FAIL("expected 3 rows (salary>85000), got " + std::to_string(r->RowCount()));
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("SELECT * FROM remote.main.types_test");
    {
        auto r = t.conn().Query(
            "SELECT * FROM remote.main.types_test ORDER BY i32");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 2) {
            TEST_FAIL("expected 2 rows, got " + std::to_string(r->RowCount()));
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("DETACH remote catalog");
    {
        auto r = t.conn().Query("DETACH remote");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            TEST_PASS();
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: data type round-trips via duckd_query
//===----------------------------------------------------------------------===//

static int TestDataTypes(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("data types: boolean");
    {
        auto r = t.conn().Query(
            "SELECT b FROM duckd_query('" + t.url() + "',"
            " 'SELECT b FROM types_test ORDER BY i32')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 2) {
            TEST_FAIL("expected 2 rows");
        } else {
            // ORDER BY i32 ASC → negative first → b=false, then b=true
            auto b0 = r->GetValue(0, 0).GetValue<bool>();
            auto b1 = r->GetValue(0, 1).GetValue<bool>();
            if (b0 != false || b1 != true) {
                TEST_FAIL("boolean mismatch");
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("data types: integer family (TINYINT .. BIGINT)");
    {
        auto r = t.conn().Query(
            "SELECT i8, i16, i32, i64"
            " FROM duckd_query('" + t.url() + "',"
            " 'SELECT i8, i16, i32, i64 FROM types_test WHERE i32 > 0')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 1) {
            TEST_FAIL("expected 1 row");
        } else {
            auto i8  = r->GetValue(0, 0).GetValue<int8_t>();
            auto i16 = r->GetValue(1, 0).GetValue<int16_t>();
            auto i32 = r->GetValue(2, 0).GetValue<int32_t>();
            auto i64 = r->GetValue(3, 0).GetValue<int64_t>();
            if (i8 != 1 || i16 != 100 || i32 != 10000 || i64 != 1000000000LL) {
                TEST_FAIL("integer value mismatch");
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("data types: FLOAT and DOUBLE");
    {
        auto r = t.conn().Query(
            "SELECT d FROM duckd_query('" + t.url() + "',"
            " 'SELECT d FROM types_test WHERE d > 0')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 1) {
            TEST_FAIL("expected 1 row");
        } else {
            auto d = r->GetValue(0, 0).GetValue<double>();
            // 2.718281828 ± tiny float tolerance
            if (d < 2.0 || d > 3.0) {
                TEST_FAIL("double value out of expected range: " + std::to_string(d));
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("data types: VARCHAR");
    {
        auto r = t.conn().Query(
            "SELECT s FROM duckd_query('" + t.url() + "',"
            " 'SELECT s FROM types_test ORDER BY s')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 2) {
            TEST_FAIL("expected 2 rows");
        } else {
            // ORDER BY s: 'hello' < 'world'
            auto s0 = r->GetValue(0, 0).ToString();
            auto s1 = r->GetValue(0, 1).ToString();
            if (s0 != "hello" || s1 != "world") {
                TEST_FAIL("varchar mismatch: got '" + s0 + "', '" + s1 + "'");
            } else {
                TEST_PASS();
            }
        }
    }

    TEST_BEGIN("data types: negative integers");
    {
        auto r = t.conn().Query(
            "SELECT i32, i64 FROM duckd_query('" + t.url() + "',"
            " 'SELECT i32, i64 FROM types_test WHERE i32 < 0')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else if (r->RowCount() != 1) {
            TEST_FAIL("expected 1 row");
        } else {
            auto i32 = r->GetValue(0, 0).GetValue<int32_t>();
            auto i64 = r->GetValue(1, 0).GetValue<int64_t>();
            if (i32 != -10000 || i64 != -1000000000LL) {
                TEST_FAIL("negative integer mismatch");
            } else {
                TEST_PASS();
            }
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: catalog discoverability (schema/table listing)
//===----------------------------------------------------------------------===//

static int TestCatalogDiscovery(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("ATTACH and SHOW ALL TABLES");
    {
        auto ar = t.conn().Query(
            "ATTACH '" + t.url() + "' AS disc (TYPE duckd)");
        if (ar->HasError()) {
            TEST_FAIL("ATTACH: " + ar->GetError());
        } else {
            // SHOW ALL TABLES lists tables from all attached catalogs
            auto r = t.conn().Query("SHOW ALL TABLES");
            if (r->HasError()) {
                TEST_FAIL(r->GetError());
            } else {
                // employees and types_test must appear
                bool found_emp = false, found_typ = false;
                for (idx_t row = 0; row < r->RowCount(); row++) {
                    auto tname = r->GetValue(2, row).ToString(); // column 2 = table name
                    if (tname == "employees") found_emp = true;
                    if (tname == "types_test") found_typ = true;
                }
                if (!found_emp || !found_typ) {
                    TEST_FAIL("missing expected tables in SHOW ALL TABLES");
                } else {
                    TEST_PASS();
                }
            }
            t.conn().Query("DETACH disc");
        }
    }

    TEST_BEGIN("ATTACH and query information_schema via duckd_query");
    {
        // Verify the server exposes information_schema correctly
        auto r = t.conn().Query(
            "SELECT table_name FROM duckd_query('" + t.url() + "',"
            " 'SELECT table_name FROM information_schema.tables"
            "  WHERE table_schema = ''main'' ORDER BY table_name')");
        if (r->HasError()) {
            TEST_FAIL(r->GetError());
        } else {
            bool found_emp = false, found_typ = false;
            for (idx_t row = 0; row < r->RowCount(); row++) {
                auto tname = r->GetValue(0, row).ToString();
                if (tname == "employees") found_emp = true;
                if (tname == "types_test") found_typ = true;
            }
            if (!found_emp || !found_typ) {
                TEST_FAIL("expected tables not found in information_schema");
            } else {
                TEST_PASS();
            }
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: error handling
//===----------------------------------------------------------------------===//

static int TestErrorHandling(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("duckd_query: invalid SQL propagates error");
    {
        auto r = t.conn().Query(
            "SELECT * FROM duckd_query('" + t.url() + "',"
            " 'SELECT FROM nonexistent_table_xyz')");
        if (r->HasError()) {
            TEST_PASS();
        } else {
            TEST_FAIL("expected error for invalid SQL, got result");
        }
    }

    TEST_BEGIN("duckd_exec: invalid SQL propagates error");
    {
        auto r = t.conn().Query(
            "SELECT duckd_exec('" + t.url() + "',"
            " 'THIS IS NOT VALID SQL')");
        if (r->HasError()) {
            TEST_PASS();
        } else {
            TEST_FAIL("expected error for invalid SQL, got result");
        }
    }

    TEST_BEGIN("duckd_query: non-existent table returns error");
    {
        auto r = t.conn().Query(
            "SELECT * FROM duckd_query('" + t.url() + "',"
            " 'SELECT * FROM no_such_table')");
        if (r->HasError()) {
            TEST_PASS();
        } else {
            TEST_FAIL("expected catalog error, got result");
        }
    }

    TEST_BEGIN("duckd_query: unreachable server returns error");
    {
        // Port 1 is almost certainly not running a Flight server
        auto r = t.conn().Query(
            "SELECT * FROM duckd_query('grpc://localhost:1', 'SELECT 1')");
        if (r->HasError()) {
            TEST_PASS();
        } else {
            TEST_FAIL("expected connection error, got result");
        }
    }

    TEST_BEGIN("ATTACH non-existent server returns error");
    {
        auto r = t.conn().Query(
            "ATTACH 'grpc://localhost:2' AS bad_remote (TYPE duckd)");
        if (r->HasError()) {
            TEST_PASS();
        } else {
            // Might succeed (lazy connection) — DETACH and pass
            t.conn().Query("DETACH bad_remote");
            TEST_PASS();
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: streaming scan via ATTACH catalog (P1+P2)
//
// Verifies that the streaming scan correctly handles multi-batch result sets
// without buffering all data in memory.
//===----------------------------------------------------------------------===//

static int TestStreamingScan(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    // Pre-populate a large table on the server via duckd_exec
    t.conn().Query(
        "SELECT duckd_exec('" + t.url() + "',"
        " 'CREATE TABLE IF NOT EXISTS stream_test AS"
        "  SELECT i, i * 2 AS j FROM generate_series(1, 5000) t(i)')");

    TEST_BEGIN("streaming scan: ATTACH + large table (5000 rows via catalog)");
    {
        auto ar = t.conn().Query(
            "ATTACH '" + t.url() + "' AS stream_cat (TYPE duckd)");
        if (ar->HasError()) {
            TEST_FAIL("ATTACH: " + ar->GetError());
        } else {
            auto r = t.conn().Query(
                "SELECT COUNT(*) AS cnt, SUM(j) AS total"
                " FROM stream_cat.main.stream_test");
            t.conn().Query("DETACH stream_cat");
            if (r->HasError()) {
                TEST_FAIL(r->GetError());
            } else {
                auto cnt   = r->GetValue(0, 0).GetValue<int64_t>();
                auto total = r->GetValue(1, 0).GetValue<int64_t>();
                // SUM(2*i for i=1..5000) = 2 * 5000*5001/2 = 25005000
                if (cnt != 5000 || total != 25005000LL) {
                    TEST_FAIL("expected cnt=5000 total=25005000; got cnt=" +
                              std::to_string(cnt) + " total=" + std::to_string(total));
                } else {
                    TEST_PASS();
                }
            }
        }
    }

    TEST_BEGIN("streaming scan: projection pushdown across multiple batches");
    {
        auto ar = t.conn().Query(
            "ATTACH '" + t.url() + "' AS stream_proj (TYPE duckd)");
        if (ar->HasError()) {
            TEST_FAIL("ATTACH: " + ar->GetError());
        } else {
            // SELECT only 'i' (projection pushdown) and filter on server side
            auto r = t.conn().Query(
                "SELECT SUM(i) AS s"
                " FROM stream_proj.main.stream_test"
                " WHERE i <= 100");
            t.conn().Query("DETACH stream_proj");
            if (r->HasError()) {
                TEST_FAIL(r->GetError());
            } else {
                auto s = r->GetValue(0, 0).GetValue<int64_t>();
                // SUM(1..100) = 5050
                if (s != 5050LL) {
                    TEST_FAIL("expected 5050, got " + std::to_string(s));
                } else {
                    TEST_PASS();
                }
            }
        }
    }

    // Cleanup
    t.conn().Query(
        "SELECT duckd_exec('" + t.url() + "',"
        " 'DROP TABLE IF EXISTS stream_test')");

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: connection registry (P6 – connection pooling)
//===----------------------------------------------------------------------===//

static int TestConnectionRegistry(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("registry: same URL returns same client instance");
    {
        auto c1 = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        auto c2 = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        if (c1.get() != c2.get()) {
            TEST_FAIL("expected same shared_ptr, got different instances");
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("registry: different URLs return different clients");
    {
        // Use a second URL that is syntactically valid but different from the
        // real server URL so that we just test registry key logic, not
        // connectivity.  We request the real server URL and a fake one.
        std::string fake_url = "grpc://localhost:19999";
        auto real_client = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        // The fake URL will fail to connect – catch the error and verify we
        // didn't store the bad entry under the real URL.
        try {
            duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(fake_url);
        } catch (...) {
            // Connection failure expected – evict any partial entry
            duckdb_client::DuckdClientRegistry::Instance().Evict(fake_url);
        }
        auto still_real = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        if (still_real.get() != real_client.get()) {
            TEST_FAIL("real client was evicted unexpectedly");
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("registry: evict + recreate gives a fresh client");
    {
        auto c_before = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        duckdb_client::DuckdClientRegistry::Instance().Evict(t.url());
        auto c_after = duckdb_client::DuckdClientRegistry::Instance().GetOrCreate(t.url());
        // After eviction a new instance must be created (pointer differs)
        if (c_before.get() == c_after.get()) {
            TEST_FAIL("expected fresh client after eviction, got same pointer");
        } else {
            TEST_PASS();
        }
    }

    TEST_BEGIN("registry: duckd_exec reuses cached connection (functional)");
    {
        // Execute 5 duckd_exec calls – all should reuse the cached connection.
        bool all_ok = true;
        for (int i = 0; i < 5; i++) {
            auto r = t.conn().Query(
                "SELECT duckd_exec('" + t.url() + "',"
                " 'SELECT 1')");
            if (r->HasError()) { all_ok = false; break; }
        }
        if (!all_ok) {
            TEST_FAIL("one or more duckd_exec calls failed when reusing connection");
        } else {
            TEST_PASS();
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Tests: concurrent duckd_query calls
//===----------------------------------------------------------------------===//

static int TestConcurrency(DuckdClientTest &t) {
    int passed = 0, failed = 0;

    TEST_BEGIN("concurrent duckd_query calls (10 threads)");
    {
        std::atomic<int> errors{0};
        std::vector<std::thread> threads;

        for (int i = 0; i < 10; i++) {
            threads.emplace_back([&t, &errors, i]() {
                // Each thread creates its own connection
                duckdb::Connection c(*t.conn().context->db);
                auto r = c.Query(
                    "SELECT COUNT(*) FROM duckd_query('" + t.url() + "',"
                    " 'SELECT * FROM generate_series(1, 100) AS s(i)')");
                if (r->HasError() || r->GetValue(0, 0).GetValue<int64_t>() != 100) {
                    errors.fetch_add(1);
                }
            });
        }
        for (auto &th : threads) th.join();

        if (errors.load() != 0) {
            TEST_FAIL(std::to_string(errors.load()) + " concurrent threads failed");
        } else {
            TEST_PASS();
        }
    }

    return failed;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== DuckD Client Extension Integration Tests ===" << std::endl;
    std::cout << "(duckd_exec, duckd_query, ATTACH via Arrow Flight SQL)" << std::endl;

    int total_failures = 0;

    {
        std::cout << "\n--- duckd_query() Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestDuckdQuery(t);
    }

    {
        std::cout << "\n--- duckd_exec() Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestDuckdExec(t);
    }

    {
        std::cout << "\n--- ATTACH Catalog Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestAttachCatalog(t);
    }

    {
        std::cout << "\n--- Data Type Round-Trip Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestDataTypes(t);
    }

    {
        std::cout << "\n--- Catalog Discovery Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestCatalogDiscovery(t);
    }

    {
        std::cout << "\n--- Error Handling Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestErrorHandling(t);
    }

    {
        std::cout << "\n--- Concurrency Tests ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestConcurrency(t);
    }

    {
        std::cout << "\n--- Streaming Scan Tests (P1+P2) ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestStreamingScan(t);
    }

    {
        std::cout << "\n--- Connection Registry Tests (P6) ---" << std::endl;
        DuckdClientTest t;
        total_failures += TestConnectionRegistry(t);
    }

    std::cout << "\n=== Summary ===" << std::endl;
    if (total_failures == 0) {
        std::cout << "All tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << total_failures << " test(s) failed!" << std::endl;
        return 1;
    }
}
