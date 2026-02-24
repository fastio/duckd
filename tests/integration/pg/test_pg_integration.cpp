//===----------------------------------------------------------------------===//
//                         DuckD Server - Integration Tests
//
// tests/integration/pg/test_pg_integration.cpp
//
// End-to-end integration tests for PostgreSQL protocol
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_handler.hpp"
#include "protocol/pg/pg_protocol.hpp"
#include "protocol/pg/pg_message_reader.hpp"
#include "session/session.hpp"
#include "executor/executor_pool.hpp"
#include "duckdb.hpp"
#include <cassert>
#include <iostream>
#include <cstring>
#include <functional>
#include <atomic>
#include <chrono>
#include <thread>

using namespace duckdb_server;
using namespace duckdb_server::pg;

//===----------------------------------------------------------------------===//
// Test Fixture
//===----------------------------------------------------------------------===//

class PgProtocolTest {
public:
    PgProtocolTest() {
        // Create in-memory DuckDB database
        db_ = std::make_unique<duckdb::DuckDB>(nullptr);

        // Create executor pool for async operations
        executor_pool_ = std::make_shared<ExecutorPool>(2);
        executor_pool_->Start();

        // Create session with session_id and db instance
        session_ = std::make_shared<Session>(1, db_->instance);

        // Create handler with send callback, executor pool, and resume callback
        handler_ = std::make_unique<PgHandler>(
            session_,
            [this](std::vector<uint8_t> data) {
                received_data_.insert(received_data_.end(), data.begin(), data.end());
            },
            executor_pool_,
            [this](PgHandler::StateUpdate state_update) {
                if (state_update) {
                    state_update();
                }
                async_complete_.store(true);
            }
        );
    }

    ~PgProtocolTest() {
        handler_.reset();
        if (executor_pool_) {
            executor_pool_->Stop();
        }
    }

    void ClearReceived() {
        received_data_.clear();
    }

    // Wait for async operation to complete
    void WaitForAsync() {
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!async_complete_.load()) {
            if (std::chrono::steady_clock::now() > deadline) {
                std::cerr << "TIMEOUT waiting for async operation" << std::endl;
                assert(false && "Async operation timed out");
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        async_complete_.store(false);
    }

    // Process data and handle async results; returns true if connection should continue
    bool ProcessAndWait(const std::vector<uint8_t>& buf) {
        auto result = handler_->ProcessData(buf.data(), buf.size());
        if (result == ProcessResult::ASYNC_PENDING) {
            WaitForAsync();
            // Process any remaining buffer
            auto pending_result = handler_->ProcessPendingBuffer();
            if (pending_result == ProcessResult::ASYNC_PENDING) {
                WaitForAsync();
                pending_result = handler_->ProcessPendingBuffer();
            }
            return pending_result != ProcessResult::CLOSE;
        }
        return result != ProcessResult::CLOSE;
    }

    // Send startup message
    bool SendStartup(const std::string& user = "test", const std::string& database = "test") {
        std::vector<uint8_t> buf;

        // Length placeholder
        size_t length_pos = buf.size();
        WriteInt32(buf, 0);

        // Protocol version 3.0
        WriteInt32(buf, PROTOCOL_VERSION_3_0);

        // Parameters
        WriteString(buf, "user");
        WriteString(buf, user);
        WriteString(buf, "database");
        WriteString(buf, database);
        buf.push_back(0);

        // Update length
        int32_t length = static_cast<int32_t>(buf.size());
        int32_t network_length = HostToNetwork32(length);
        std::memcpy(buf.data() + length_pos, &network_length, 4);

        return ProcessAndWait(buf);
    }

    // Send simple query
    bool SendQuery(const std::string& sql) {
        std::vector<uint8_t> buf;
        buf.push_back('Q');

        // Length
        int32_t length = static_cast<int32_t>(4 + sql.size() + 1);
        WriteInt32(buf, length);

        // Query string
        WriteString(buf, sql);

        return ProcessAndWait(buf);
    }

    // Send Parse message
    bool SendParse(const std::string& name, const std::string& query,
                   const std::vector<int32_t>& param_types = {}) {
        std::vector<uint8_t> buf;
        buf.push_back('P');

        // Build body first
        std::vector<uint8_t> body;
        WriteString(body, name);
        WriteString(body, query);
        WriteInt16(body, static_cast<int16_t>(param_types.size()));
        for (int32_t oid : param_types) {
            WriteInt32(body, oid);
        }

        // Length
        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());

        return ProcessAndWait(buf);
    }

    // Send Bind message
    bool SendBind(const std::string& portal, const std::string& statement,
                  const std::vector<std::string>& params = {}) {
        std::vector<uint8_t> buf;
        buf.push_back('B');

        std::vector<uint8_t> body;
        WriteString(body, portal);
        WriteString(body, statement);

        // Parameter format codes (all text)
        WriteInt16(body, 0);

        // Parameter values
        WriteInt16(body, static_cast<int16_t>(params.size()));
        for (const auto& param : params) {
            WriteInt32(body, static_cast<int32_t>(param.size()));
            body.insert(body.end(), param.begin(), param.end());
        }

        // Result format codes (all text)
        WriteInt16(body, 0);

        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());

        return ProcessAndWait(buf);
    }

    // Send Describe message
    bool SendDescribe(char type, const std::string& name) {
        std::vector<uint8_t> buf;
        buf.push_back('D');

        std::vector<uint8_t> body;
        body.push_back(static_cast<uint8_t>(type));
        WriteString(body, name);

        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());

        return ProcessAndWait(buf);
    }

    // Send Execute message
    bool SendExecute(const std::string& portal, int32_t max_rows = 0) {
        std::vector<uint8_t> buf;
        buf.push_back('E');

        std::vector<uint8_t> body;
        WriteString(body, portal);
        WriteInt32(body, max_rows);

        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());

        return ProcessAndWait(buf);
    }

    // Send Sync message
    bool SendSync() {
        std::vector<uint8_t> buf = {'S', 0, 0, 0, 4};
        return ProcessAndWait(buf);
    }

    // Send Close message
    bool SendClose(char type, const std::string& name) {
        std::vector<uint8_t> buf;
        buf.push_back('C');

        std::vector<uint8_t> body;
        body.push_back(static_cast<uint8_t>(type));
        WriteString(body, name);

        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());

        return ProcessAndWait(buf);
    }

    // Send Terminate
    bool SendTerminate() {
        std::vector<uint8_t> buf = {'X', 0, 0, 0, 4};
        return ProcessAndWait(buf);
    }

    // Send a raw message with arbitrary type (for error handling tests)
    bool SendRawMessage(char type, const std::vector<uint8_t>& body = {}) {
        std::vector<uint8_t> buf;
        buf.push_back(static_cast<uint8_t>(type));
        int32_t length = static_cast<int32_t>(4 + body.size());
        WriteInt32(buf, length);
        buf.insert(buf.end(), body.begin(), body.end());
        return ProcessAndWait(buf);
    }

    // Check if received data contains message type
    bool HasMessageType(char type) const {
        for (size_t i = 0; i < received_data_.size(); ) {
            if (received_data_[i] == static_cast<uint8_t>(type)) {
                return true;
            }
            if (i + 5 > received_data_.size()) break;
            int32_t len;
            std::memcpy(&len, received_data_.data() + i + 1, 4);
            len = NetworkToHost32(len);
            i += 1 + len;
        }
        return false;
    }

    // Extract CommandComplete tag
    std::string GetCommandCompleteTag() const {
        for (size_t i = 0; i < received_data_.size(); ) {
            char type = static_cast<char>(received_data_[i]);
            if (i + 5 > received_data_.size()) break;
            int32_t len;
            std::memcpy(&len, received_data_.data() + i + 1, 4);
            len = NetworkToHost32(len);

            if (type == 'C') {
                std::string tag(reinterpret_cast<const char*>(received_data_.data() + i + 5));
                return tag;
            }

            i += 1 + len;
        }
        return "";
    }

    // Get ReadyForQuery status
    char GetReadyForQueryStatus() const {
        for (size_t i = 0; i < received_data_.size(); ) {
            char type = static_cast<char>(received_data_[i]);
            if (i + 5 > received_data_.size()) break;
            int32_t len;
            std::memcpy(&len, received_data_.data() + i + 1, 4);
            len = NetworkToHost32(len);

            if (type == 'Z' && len == 5) {
                return static_cast<char>(received_data_[i + 5]);
            }

            i += 1 + len;
        }
        return '\0';
    }

    // Count DataRow messages
    int CountDataRows() const {
        int count = 0;
        for (size_t i = 0; i < received_data_.size(); ) {
            char type = static_cast<char>(received_data_[i]);
            if (i + 5 > received_data_.size()) break;
            int32_t len;
            std::memcpy(&len, received_data_.data() + i + 1, 4);
            len = NetworkToHost32(len);

            if (type == 'D') {
                count++;
            }

            i += 1 + len;
        }
        return count;
    }

    PgConnectionState GetState() const {
        return handler_->GetState();
    }

private:
    void WriteInt32(std::vector<uint8_t>& buf, int32_t value) {
        int32_t network = HostToNetwork32(value);
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network);
        buf.insert(buf.end(), ptr, ptr + 4);
    }

    void WriteInt16(std::vector<uint8_t>& buf, int16_t value) {
        int16_t network = HostToNetwork16(value);
        const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&network);
        buf.insert(buf.end(), ptr, ptr + 2);
    }

    void WriteString(std::vector<uint8_t>& buf, const std::string& str) {
        buf.insert(buf.end(), str.begin(), str.end());
        buf.push_back(0);
    }

    std::unique_ptr<duckdb::DuckDB> db_;
    std::shared_ptr<ExecutorPool> executor_pool_;
    std::shared_ptr<Session> session_;
    std::unique_ptr<PgHandler> handler_;
    std::vector<uint8_t> received_data_;
    std::atomic<bool> async_complete_{false};
};

//===----------------------------------------------------------------------===//
// Connection Tests
//===----------------------------------------------------------------------===//

void TestStartupHandshake() {
    std::cout << "  Testing Startup Handshake..." << std::endl;

    PgProtocolTest test;
    assert(test.SendStartup("testuser", "testdb"));

    // Should receive AuthenticationOk, ParameterStatus, BackendKeyData, ReadyForQuery
    assert(test.HasMessageType('R'));  // Authentication
    assert(test.HasMessageType('S'));  // ParameterStatus
    assert(test.HasMessageType('K'));  // BackendKeyData
    assert(test.HasMessageType('Z'));  // ReadyForQuery
    assert(test.GetReadyForQueryStatus() == 'I');  // Idle
    assert(test.GetState() == PgConnectionState::READY);

    std::cout << "    PASSED" << std::endl;
}

void TestTerminateConnection() {
    std::cout << "  Testing Terminate Connection..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(!test.SendTerminate());  // Returns false to close connection
    assert(test.GetState() == PgConnectionState::CLOSED);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Simple Query Protocol Tests
//===----------------------------------------------------------------------===//

void TestSimpleQuerySelect() {
    std::cout << "  Testing Simple Query SELECT..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("SELECT 1 + 1 as result"));

    assert(test.HasMessageType('T'));  // RowDescription
    assert(test.HasMessageType('D'));  // DataRow
    assert(test.HasMessageType('C'));  // CommandComplete
    assert(test.HasMessageType('Z'));  // ReadyForQuery
    assert(test.GetCommandCompleteTag() == "SELECT 1");
    assert(test.CountDataRows() == 1);

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryCreateTable() {
    std::cout << "  Testing Simple Query CREATE TABLE..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("CREATE TABLE test_table (id INTEGER, name VARCHAR)"));

    assert(test.HasMessageType('C'));
    assert(test.HasMessageType('Z'));
    assert(test.GetCommandCompleteTag() == "CREATE TABLE");

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryInsert() {
    std::cout << "  Testing Simple Query INSERT..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE users (id INTEGER, name VARCHAR)");
    test.ClearReceived();

    assert(test.SendQuery("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"));

    assert(test.HasMessageType('C'));
    assert(test.HasMessageType('Z'));

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryMultipleRows() {
    std::cout << "  Testing Simple Query Multiple Rows..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE items (id INTEGER)");
    test.SendQuery("INSERT INTO items VALUES (1), (2), (3), (4), (5)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM items"));

    assert(test.CountDataRows() == 5);
    assert(test.GetCommandCompleteTag() == "SELECT 5");

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryUpdate() {
    std::cout << "  Testing Simple Query UPDATE..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE counters (id INTEGER, count INTEGER)");
    test.SendQuery("INSERT INTO counters VALUES (1, 10), (2, 20)");
    test.ClearReceived();

    assert(test.SendQuery("UPDATE counters SET count = count + 1 WHERE id = 1"));

    assert(test.HasMessageType('C'));
    // DuckDB returns affected rows via RowDescription/DataRow

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryDelete() {
    std::cout << "  Testing Simple Query DELETE..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE to_delete (id INTEGER)");
    test.SendQuery("INSERT INTO to_delete VALUES (1), (2), (3)");
    test.ClearReceived();

    assert(test.SendQuery("DELETE FROM to_delete WHERE id > 1"));

    assert(test.HasMessageType('C'));

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryEmptyQuery() {
    std::cout << "  Testing Simple Query Empty..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery(""));

    assert(test.HasMessageType('I'));  // EmptyQueryResponse
    assert(test.HasMessageType('Z'));

    std::cout << "    PASSED" << std::endl;
}

void TestSimpleQueryError() {
    std::cout << "  Testing Simple Query Error..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM nonexistent_table"));

    assert(test.HasMessageType('E'));  // ErrorResponse
    assert(test.HasMessageType('Z'));
    assert(test.GetReadyForQueryStatus() == 'I');  // Back to Idle

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Extended Query Protocol Tests
//===----------------------------------------------------------------------===//

void TestExtendedQueryParse() {
    std::cout << "  Testing Extended Query Parse..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendParse("stmt1", "SELECT 1"));

    assert(test.HasMessageType('1'));  // ParseComplete

    std::cout << "    PASSED" << std::endl;
}

void TestExtendedQueryBindExecute() {
    std::cout << "  Testing Extended Query Bind/Execute..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendParse("stmt1", "SELECT $1::INTEGER + $2::INTEGER as sum");
    test.ClearReceived();

    assert(test.SendBind("portal1", "stmt1", {"10", "20"}));
    assert(test.HasMessageType('2'));  // BindComplete
    test.ClearReceived();

    assert(test.SendExecute("portal1"));
    assert(test.HasMessageType('D'));  // DataRow
    assert(test.HasMessageType('C'));  // CommandComplete

    std::cout << "    PASSED" << std::endl;
}

void TestExtendedQueryDescribe() {
    std::cout << "  Testing Extended Query Describe..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();

    test.SendQuery("CREATE TABLE describe_test (id INTEGER, name VARCHAR, active BOOLEAN)");
    test.ClearReceived();

    test.SendParse("desc_stmt", "SELECT * FROM describe_test");
    test.ClearReceived();

    assert(test.SendDescribe('S', "desc_stmt"));
    assert(test.HasMessageType('t'));  // ParameterDescription
    assert(test.HasMessageType('T'));  // RowDescription

    std::cout << "    PASSED" << std::endl;
}

void TestExtendedQueryClose() {
    std::cout << "  Testing Extended Query Close..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendParse("to_close", "SELECT 1");
    test.ClearReceived();

    assert(test.SendClose('S', "to_close"));
    assert(test.HasMessageType('3'));  // CloseComplete

    std::cout << "    PASSED" << std::endl;
}

void TestExtendedQuerySync() {
    std::cout << "  Testing Extended Query Sync..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendSync());
    assert(test.HasMessageType('Z'));  // ReadyForQuery

    std::cout << "    PASSED" << std::endl;
}

void TestExtendedQueryFullCycle() {
    std::cout << "  Testing Extended Query Full Cycle..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();

    // Create table
    test.SendQuery("CREATE TABLE ext_test (id INTEGER, value VARCHAR)");
    test.SendQuery("INSERT INTO ext_test VALUES (1, 'one'), (2, 'two'), (3, 'three')");
    test.ClearReceived();

    // Parse
    test.SendParse("select_stmt", "SELECT * FROM ext_test WHERE id > $1::INTEGER");
    assert(test.HasMessageType('1'));
    test.ClearReceived();

    // Bind
    test.SendBind("", "select_stmt", {"1"});
    assert(test.HasMessageType('2'));
    test.ClearReceived();

    // Execute
    test.SendExecute("");
    assert(test.CountDataRows() == 2);  // id=2 and id=3
    test.ClearReceived();

    // Sync
    test.SendSync();
    assert(test.HasMessageType('Z'));

    // Close
    test.ClearReceived();
    test.SendClose('S', "select_stmt");
    assert(test.HasMessageType('3'));

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// SQL Feature Tests
//===----------------------------------------------------------------------===//

void TestSQLDataTypes() {
    std::cout << "  Testing SQL Data Types..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE types_test ("
                   "  bool_col BOOLEAN,"
                   "  int_col INTEGER,"
                   "  bigint_col BIGINT,"
                   "  float_col FLOAT,"
                   "  double_col DOUBLE,"
                   "  varchar_col VARCHAR,"
                   "  date_col DATE,"
                   "  timestamp_col TIMESTAMP"
                   ")");
    test.ClearReceived();

    test.SendQuery("INSERT INTO types_test VALUES ("
                   "  true, 42, 9223372036854775807, 3.14, 2.718281828,"
                   "  'hello world', '2024-01-15', '2024-01-15 10:30:00'"
                   ")");
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM types_test"));
    assert(test.HasMessageType('T'));
    assert(test.CountDataRows() == 1);

    std::cout << "    PASSED" << std::endl;
}

void TestSQLNullHandling() {
    std::cout << "  Testing SQL NULL Handling..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE null_test (id INTEGER, value VARCHAR)");
    test.SendQuery("INSERT INTO null_test VALUES (1, NULL), (2, 'not null'), (NULL, 'null id')");
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM null_test"));
    assert(test.CountDataRows() == 3);

    std::cout << "    PASSED" << std::endl;
}

void TestSQLAggregation() {
    std::cout << "  Testing SQL Aggregation..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE agg_test (category VARCHAR, amount DOUBLE)");
    test.SendQuery("INSERT INTO agg_test VALUES "
                   "('A', 10), ('A', 20), ('B', 15), ('B', 25), ('B', 30)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT category, SUM(amount), AVG(amount), COUNT(*) "
                          "FROM agg_test GROUP BY category ORDER BY category"));
    assert(test.CountDataRows() == 2);

    std::cout << "    PASSED" << std::endl;
}

void TestSQLJoin() {
    std::cout << "  Testing SQL JOIN..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE join_users (id INTEGER PRIMARY KEY, name VARCHAR)");
    test.SendQuery("CREATE TABLE join_orders (id INTEGER, user_id INTEGER, amount DOUBLE)");
    test.SendQuery("INSERT INTO join_users VALUES (1, 'Alice'), (2, 'Bob')");
    test.SendQuery("INSERT INTO join_orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT u.name, SUM(o.amount) as total "
                          "FROM join_users u "
                          "JOIN join_orders o ON u.id = o.user_id "
                          "GROUP BY u.name ORDER BY u.name"));
    assert(test.CountDataRows() == 2);

    std::cout << "    PASSED" << std::endl;
}

void TestSQLSubquery() {
    std::cout << "  Testing SQL Subquery..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE subq_test (id INTEGER, value INTEGER)");
    test.SendQuery("INSERT INTO subq_test VALUES (1, 10), (2, 20), (3, 30), (4, 40)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM subq_test WHERE value > "
                          "(SELECT AVG(value) FROM subq_test)"));
    assert(test.CountDataRows() == 2);  // 30 and 40 are above avg of 25

    std::cout << "    PASSED" << std::endl;
}

void TestSQLCTE() {
    std::cout << "  Testing SQL CTE (WITH clause)..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("WITH numbers AS ("
                          "  SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3"
                          ") SELECT SUM(n) FROM numbers"));
    assert(test.CountDataRows() == 1);

    std::cout << "    PASSED" << std::endl;
}

void TestSQLWindowFunction() {
    std::cout << "  Testing SQL Window Function..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE window_test (dept VARCHAR, employee VARCHAR, salary INTEGER)");
    test.SendQuery("INSERT INTO window_test VALUES "
                   "('Sales', 'Alice', 50000), ('Sales', 'Bob', 60000), "
                   "('Engineering', 'Charlie', 70000), ('Engineering', 'David', 80000)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT dept, employee, salary, "
                          "RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rank "
                          "FROM window_test ORDER BY dept, rank"));
    assert(test.CountDataRows() == 4);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Error Handling Tests
//===----------------------------------------------------------------------===//

void TestSQLSyntaxError() {
    std::cout << "  Testing SQL Syntax Error..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("SELEC * FROM table"));  // Typo in SELECT

    assert(test.HasMessageType('E'));
    assert(test.HasMessageType('Z'));
    assert(test.GetState() == PgConnectionState::READY);  // Should remain usable

    std::cout << "    PASSED" << std::endl;
}

void TestSQLConstraintViolation() {
    std::cout << "  Testing SQL Constraint Violation..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE pk_test (id INTEGER PRIMARY KEY)");
    test.SendQuery("INSERT INTO pk_test VALUES (1)");
    test.ClearReceived();

    assert(test.SendQuery("INSERT INTO pk_test VALUES (1)"));  // Duplicate PK

    assert(test.HasMessageType('E'));
    assert(test.HasMessageType('Z'));

    std::cout << "    PASSED" << std::endl;
}

void TestSQLTableNotFound() {
    std::cout << "  Testing SQL Table Not Found..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("SELECT * FROM does_not_exist"));

    assert(test.HasMessageType('E'));
    assert(test.HasMessageType('Z'));

    std::cout << "    PASSED" << std::endl;
}

void TestSQLColumnNotFound() {
    std::cout << "  Testing SQL Column Not Found..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendQuery("CREATE TABLE col_test (id INTEGER)");
    test.ClearReceived();

    assert(test.SendQuery("SELECT nonexistent_column FROM col_test"));

    assert(test.HasMessageType('E'));

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Transaction Tests
//===----------------------------------------------------------------------===//

void TestTransactionBeginCommit() {
    std::cout << "  Testing Transaction BEGIN/COMMIT..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();

    // BEGIN
    test.ClearReceived();
    assert(test.SendQuery("BEGIN"));
    assert(test.GetCommandCompleteTag() == "BEGIN");
    assert(test.GetReadyForQueryStatus() == 'T');  // InTransaction

    // Query within transaction
    test.ClearReceived();
    assert(test.SendQuery("SELECT 42"));
    assert(test.GetReadyForQueryStatus() == 'T');  // Still in transaction

    // COMMIT
    test.ClearReceived();
    assert(test.SendQuery("COMMIT"));
    assert(test.GetCommandCompleteTag() == "COMMIT");
    assert(test.GetReadyForQueryStatus() == 'I');  // Back to idle

    std::cout << "    PASSED" << std::endl;
}

void TestTransactionBeginRollback() {
    std::cout << "  Testing Transaction BEGIN/ROLLBACK..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();

    // BEGIN
    test.ClearReceived();
    assert(test.SendQuery("BEGIN"));
    assert(test.GetReadyForQueryStatus() == 'T');

    // Create a table in the transaction
    test.ClearReceived();
    test.SendQuery("CREATE TABLE rollback_test (id INTEGER)");
    assert(test.GetReadyForQueryStatus() == 'T');

    // ROLLBACK
    test.ClearReceived();
    assert(test.SendQuery("ROLLBACK"));
    assert(test.GetCommandCompleteTag() == "ROLLBACK");
    assert(test.GetReadyForQueryStatus() == 'I');

    std::cout << "    PASSED" << std::endl;
}

void TestTransactionFailedQuery() {
    std::cout << "  Testing Transaction failed query state (E status)..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();

    // BEGIN
    test.ClearReceived();
    assert(test.SendQuery("BEGIN"));
    assert(test.GetReadyForQueryStatus() == 'T');

    // Query that fails inside transaction
    test.ClearReceived();
    assert(test.SendQuery("SELECT * FROM no_such_table_xyz_abc"));
    assert(test.HasMessageType('E'));                    // ErrorResponse
    assert(test.GetReadyForQueryStatus() == 'E');        // Failed transaction state

    // ROLLBACK to recover
    test.ClearReceived();
    assert(test.SendQuery("ROLLBACK"));
    assert(test.GetReadyForQueryStatus() == 'I');        // Back to idle

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Command Tag Tests
//===----------------------------------------------------------------------===//

void TestCommandTagDropTable() {
    std::cout << "  Testing Command Tag DROP TABLE..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.SendQuery("CREATE TABLE drop_target (id INTEGER)");
    test.ClearReceived();

    assert(test.SendQuery("DROP TABLE drop_target"));
    assert(test.GetCommandCompleteTag() == "DROP TABLE");

    std::cout << "    PASSED" << std::endl;
}

void TestCommandTagCreateView() {
    std::cout << "  Testing Command Tag CREATE VIEW..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    assert(test.SendQuery("CREATE VIEW view_tag_test AS SELECT 1 AS id"));
    assert(test.GetCommandCompleteTag() == "CREATE VIEW");

    std::cout << "    PASSED" << std::endl;
}

void TestCommandTagCreateIndex() {
    std::cout << "  Testing Command Tag CREATE INDEX..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.SendQuery("CREATE TABLE idx_table (id INTEGER, name VARCHAR)");
    test.ClearReceived();

    assert(test.SendQuery("CREATE INDEX idx_on_id ON idx_table (id)"));
    assert(test.GetCommandCompleteTag() == "CREATE INDEX");

    std::cout << "    PASSED" << std::endl;
}

void TestCommandTagAlterTable() {
    std::cout << "  Testing Command Tag ALTER TABLE..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.SendQuery("CREATE TABLE alter_target (id INTEGER)");
    test.ClearReceived();

    assert(test.SendQuery("ALTER TABLE alter_target ADD COLUMN name VARCHAR"));
    assert(test.GetCommandCompleteTag() == "ALTER TABLE");

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Portal Describe and Close Tests
//===----------------------------------------------------------------------===//

void TestDescribePortal() {
    std::cout << "  Testing Describe Portal (type='P')..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    // Parse a statement with result columns
    test.SendParse("dp_stmt", "SELECT 1 AS num, 'hello' AS str");
    test.ClearReceived();

    // Bind to a portal
    test.SendBind("dp_portal", "dp_stmt");
    assert(test.HasMessageType('2'));  // BindComplete
    test.ClearReceived();

    // Describe the portal
    assert(test.SendDescribe('P', "dp_portal"));
    // Portal Describe returns RowDescription (no ParameterDescription for portals)
    assert(test.HasMessageType('T'));  // RowDescription

    std::cout << "    PASSED" << std::endl;
}

void TestClosePortal() {
    std::cout << "  Testing Close Portal (type='P')..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    test.SendParse("cp_stmt", "SELECT 42");
    test.ClearReceived();

    test.SendBind("cp_portal", "cp_stmt");
    assert(test.HasMessageType('2'));  // BindComplete
    test.ClearReceived();

    // Close the portal (not the statement)
    assert(test.SendClose('P', "cp_portal"));
    assert(test.HasMessageType('3'));  // CloseComplete

    // Statement should still exist - we can bind again
    test.ClearReceived();
    test.SendBind("cp_portal2", "cp_stmt");
    assert(test.HasMessageType('2'));  // BindComplete

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Execute Max Rows (Portal Suspension) Tests
//===----------------------------------------------------------------------===//

void TestExecuteMaxRows() {
    std::cout << "  Testing Execute max_rows (PortalSuspended)..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.SendQuery("CREATE TABLE maxrows_src (id INTEGER)");
    test.SendQuery("INSERT INTO maxrows_src VALUES (1),(2),(3),(4),(5)");
    test.ClearReceived();

    test.SendParse("mr_stmt", "SELECT id FROM maxrows_src ORDER BY id");
    test.ClearReceived();

    test.SendBind("mr_portal", "mr_stmt");
    assert(test.HasMessageType('2'));  // BindComplete
    test.ClearReceived();

    // Execute with max_rows=2 → should get 2 DataRows + PortalSuspended
    assert(test.SendExecute("mr_portal", 2));
    assert(test.CountDataRows() == 2);
    assert(test.HasMessageType('s'));  // PortalSuspended

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Unknown Message Type Tests
//===----------------------------------------------------------------------===//

void TestUnknownMessageType() {
    std::cout << "  Testing Unknown Message Type..." << std::endl;

    PgProtocolTest test;
    test.SendStartup();
    test.ClearReceived();

    // Send message with unknown type '?' (no body)
    assert(test.SendRawMessage('?'));
    assert(test.HasMessageType('E'));  // ErrorResponse
    assert(test.HasMessageType('Z'));  // ReadyForQuery (connection still alive)
    assert(test.GetState() == PgConnectionState::READY);

    // Verify connection is still usable
    test.ClearReceived();
    assert(test.SendQuery("SELECT 1"));
    assert(test.HasMessageType('D'));  // DataRow

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== PostgreSQL Protocol Integration Tests ===" << std::endl;

    std::cout << "\n1. Connection Tests:" << std::endl;
    TestStartupHandshake();
    TestTerminateConnection();

    std::cout << "\n2. Simple Query Protocol:" << std::endl;
    TestSimpleQuerySelect();
    TestSimpleQueryCreateTable();
    TestSimpleQueryInsert();
    TestSimpleQueryMultipleRows();
    TestSimpleQueryUpdate();
    TestSimpleQueryDelete();
    TestSimpleQueryEmptyQuery();
    TestSimpleQueryError();

    std::cout << "\n3. Extended Query Protocol:" << std::endl;
    TestExtendedQueryParse();
    TestExtendedQueryBindExecute();
    TestExtendedQueryDescribe();
    TestExtendedQueryClose();
    TestExtendedQuerySync();
    TestExtendedQueryFullCycle();

    std::cout << "\n4. SQL Feature Tests:" << std::endl;
    TestSQLDataTypes();
    TestSQLNullHandling();
    TestSQLAggregation();
    TestSQLJoin();
    TestSQLSubquery();
    TestSQLCTE();
    TestSQLWindowFunction();

    std::cout << "\n5. Error Handling:" << std::endl;
    TestSQLSyntaxError();
    TestSQLConstraintViolation();
    TestSQLTableNotFound();
    TestSQLColumnNotFound();

    std::cout << "\n6. Transaction Tests:" << std::endl;
    TestTransactionBeginCommit();
    TestTransactionBeginRollback();
    TestTransactionFailedQuery();

    std::cout << "\n7. Command Tag Tests:" << std::endl;
    TestCommandTagDropTable();
    TestCommandTagCreateView();
    TestCommandTagCreateIndex();
    TestCommandTagAlterTable();

    std::cout << "\n8. Portal Describe and Close:" << std::endl;
    TestDescribePortal();
    TestClosePortal();

    std::cout << "\n9. Execute Max Rows:" << std::endl;
    TestExecuteMaxRows();

    std::cout << "\n10. Unknown Message Type:" << std::endl;
    TestUnknownMessageType();

    std::cout << "\n=== All integration tests PASSED ===" << std::endl;
    return 0;
}
