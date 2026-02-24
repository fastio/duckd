//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/session/test_session.cpp
//
// Unit tests for Session
//===----------------------------------------------------------------------===//

#include "session/session.hpp"
#include "session/connection_pool.hpp"
#include "duckdb.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>

using namespace duckdb_server;

//===----------------------------------------------------------------------===//
// Helpers
//===----------------------------------------------------------------------===//

static std::shared_ptr<duckdb::DuckDB> CreateDB() {
    return std::make_shared<duckdb::DuckDB>(nullptr);
}

//===----------------------------------------------------------------------===//
// Legacy Constructor Tests
//===----------------------------------------------------------------------===//

void TestSessionLegacyConstruction() {
    std::cout << "  Testing legacy construction (owned connection)..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    assert(session.GetSessionId() == 1);
    assert(session.HasConnection());
    assert(!session.HasActiveConnection());  // Not yet acquired

    // GetConnection should return owned connection
    auto& conn = session.GetConnection();
    (void)conn;
    assert(session.HasActiveConnection());

    std::cout << "    PASSED" << std::endl;
}

void TestSessionPoolConstruction() {
    std::cout << "  Testing pool construction (lazy acquisition)..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 5;
    ConnectionPool pool(db->instance, config);

    Session session(42, &pool);

    assert(session.GetSessionId() == 42);
    assert(session.HasConnection());          // Has pool pointer
    assert(!session.HasActiveConnection());   // Not yet acquired

    // Lazy acquisition on first GetConnection
    auto& conn = session.GetConnection();
    (void)conn;
    assert(session.HasActiveConnection());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// State Management Tests
//===----------------------------------------------------------------------===//

void TestSessionState() {
    std::cout << "  Testing session state management..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Defaults
    assert(session.GetCurrentDatabase() == "main");
    assert(session.GetCurrentSchema() == "main");
    assert(session.GetClientInfo().empty());
    assert(session.GetUsername().empty());

    // Set values
    session.SetCurrentDatabase("testdb");
    assert(session.GetCurrentDatabase() == "testdb");

    session.SetCurrentSchema("public");
    assert(session.GetCurrentSchema() == "public");

    session.SetClientInfo("psql 15.0");
    assert(session.GetClientInfo() == "psql 15.0");

    session.SetUsername("admin");
    assert(session.GetUsername() == "admin");

    std::cout << "    PASSED" << std::endl;
}

void TestSessionBackendKeyData() {
    std::cout << "  Testing backend key data..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Defaults
    assert(session.GetBackendProcessId() == 0);
    assert(session.GetBackendSecretKey() == 0);

    // Set
    session.SetBackendKeyData(12345, 67890);
    assert(session.GetBackendProcessId() == 12345);
    assert(session.GetBackendSecretKey() == 67890);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Prepared Statement Tests
//===----------------------------------------------------------------------===//

void TestPreparedStatements() {
    std::cout << "  Testing prepared statements..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Initially none
    assert(session.GetPreparedStatement("stmt1") == nullptr);

    // Prepare a statement
    auto& conn = session.GetConnection();
    auto stmt = conn.Prepare("SELECT $1::INTEGER AS val");
    assert(!stmt->HasError());

    session.AddPreparedStatement("stmt1", std::move(stmt));

    // Get it back
    auto* retrieved = session.GetPreparedStatement("stmt1");
    assert(retrieved != nullptr);

    // Non-existent
    assert(session.GetPreparedStatement("nonexistent") == nullptr);

    std::cout << "    PASSED" << std::endl;
}

void TestRemovePreparedStatement() {
    std::cout << "  Testing RemovePreparedStatement..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    auto& conn = session.GetConnection();
    auto stmt = conn.Prepare("SELECT 1");
    session.AddPreparedStatement("stmt1", std::move(stmt));

    assert(session.GetPreparedStatement("stmt1") != nullptr);

    // Remove
    bool removed = session.RemovePreparedStatement("stmt1");
    assert(removed);
    assert(session.GetPreparedStatement("stmt1") == nullptr);

    // Remove non-existent
    removed = session.RemovePreparedStatement("nonexistent");
    assert(!removed);

    std::cout << "    PASSED" << std::endl;
}

void TestClearPreparedStatements() {
    std::cout << "  Testing ClearPreparedStatements..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    auto& conn = session.GetConnection();
    session.AddPreparedStatement("s1", conn.Prepare("SELECT 1"));
    session.AddPreparedStatement("s2", conn.Prepare("SELECT 2"));
    session.AddPreparedStatement("s3", conn.Prepare("SELECT 3"));

    assert(session.GetPreparedStatement("s1") != nullptr);
    assert(session.GetPreparedStatement("s2") != nullptr);
    assert(session.GetPreparedStatement("s3") != nullptr);

    session.ClearPreparedStatements();

    assert(session.GetPreparedStatement("s1") == nullptr);
    assert(session.GetPreparedStatement("s2") == nullptr);
    assert(session.GetPreparedStatement("s3") == nullptr);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Query Tracking Tests
//===----------------------------------------------------------------------===//

void TestQueryTracking() {
    std::cout << "  Testing query tracking..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Defaults
    assert(session.GetCurrentQueryId() == 0);
    assert(!session.IsCancelRequested());
    assert(!session.IsQueryRunning());

    // Set query
    session.SetCurrentQueryId(42);
    assert(session.GetCurrentQueryId() == 42);

    // Query running
    session.MarkQueryStart();
    assert(session.IsQueryRunning());

    session.MarkQueryEnd();
    assert(!session.IsQueryRunning());

    // Cancel
    session.RequestCancel();
    assert(session.IsCancelRequested());

    session.ClearCancel();
    assert(!session.IsCancelRequested());

    std::cout << "    PASSED" << std::endl;
}

void TestInterruptQuery() {
    std::cout << "  Testing InterruptQuery..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Ensure connection exists
    session.GetConnection();

    // Should not crash
    session.InterruptQuery();
    assert(session.IsCancelRequested());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Expiry Tests
//===----------------------------------------------------------------------===//

void TestSessionExpiry() {
    std::cout << "  Testing session expiry..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Not expired with long timeout
    assert(!session.IsExpired(std::chrono::minutes(30)));

    // Not expired with 1 minute timeout (just created)
    assert(!session.IsExpired(std::chrono::minutes(1)));

    // Expired with 0 minute timeout
    assert(session.IsExpired(std::chrono::minutes(0)));

    std::cout << "    PASSED" << std::endl;
}

void TestSessionTouch() {
    std::cout << "  Testing Touch..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    auto before = session.GetLastActive();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    session.Touch();
    auto after = session.GetLastActive();

    assert(after > before);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Connection Release Tests
//===----------------------------------------------------------------------===//

void TestSessionReleaseConnection() {
    std::cout << "  Testing ReleaseConnection..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 5;
    ConnectionPool pool(db->instance, config);

    Session session(1, &pool);

    // Acquire
    session.GetConnection();
    assert(session.HasActiveConnection());

    auto stats = pool.GetStats();
    assert(stats.in_use == 1);

    // Release
    session.ReleaseConnection();
    assert(!session.HasActiveConnection());

    stats = pool.GetStats();
    assert(stats.in_use == 0);

    // Can re-acquire
    session.GetConnection();
    assert(session.HasActiveConnection());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Timestamp Tests
//===----------------------------------------------------------------------===//

void TestSessionTimestamps() {
    std::cout << "  Testing timestamps..." << std::endl;

    auto before = Clock::now();
    auto db = CreateDB();
    Session session(1, db->instance);
    auto after = Clock::now();

    assert(session.GetCreatedAt() >= before);
    assert(session.GetCreatedAt() <= after);
    assert(session.GetLastActive() >= before);
    assert(session.GetLastActive() <= after);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== Session Unit Tests ===" << std::endl;

    std::cout << "\n1. Construction Tests:" << std::endl;
    TestSessionLegacyConstruction();
    TestSessionPoolConstruction();

    std::cout << "\n2. State Management:" << std::endl;
    TestSessionState();
    TestSessionBackendKeyData();

    std::cout << "\n3. Prepared Statements:" << std::endl;
    TestPreparedStatements();
    TestRemovePreparedStatement();
    TestClearPreparedStatements();

    std::cout << "\n4. Query Tracking:" << std::endl;
    TestQueryTracking();
    TestInterruptQuery();

    std::cout << "\n5. Session Expiry:" << std::endl;
    TestSessionExpiry();
    TestSessionTouch();

    std::cout << "\n6. Connection Release:" << std::endl;
    TestSessionReleaseConnection();

    std::cout << "\n7. Timestamps:" << std::endl;
    TestSessionTimestamps();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
