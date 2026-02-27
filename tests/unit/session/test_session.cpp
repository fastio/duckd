//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/session/test_session.cpp
//
// Unit tests for Session (sticky session model)
//===----------------------------------------------------------------------===//

#include "session/session.hpp"
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
// Construction Tests
//===----------------------------------------------------------------------===//

void TestSessionConstruction() {
    std::cout << "  Testing construction..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    assert(session.GetSessionId() == 1);

    // Connection is not yet created (lazy)
    auto& conn = session.GetConnection();
    (void)conn;

    // Connection is now live - verify it works
    auto result = conn.Query("SELECT 42");
    assert(!result->HasError());

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
// Sticky Connection Tests
//===----------------------------------------------------------------------===//

void TestStickyConnection() {
    std::cout << "  Testing sticky connection (same connection across calls)..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // First GetConnection creates it
    auto& conn1 = session.GetConnection();
    // Second GetConnection returns the same object
    auto& conn2 = session.GetConnection();

    assert(&conn1 == &conn2);

    // Verify the connection works
    auto result = conn1.Query("SELECT 1 AS n");
    assert(!result->HasError());

    std::cout << "    PASSED" << std::endl;
}

void TestConnectionPersistsAcrossQueries() {
    std::cout << "  Testing connection persists (session state)..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    auto& conn = session.GetConnection();

    // Create a temp table
    auto r1 = conn.Query("CREATE TEMP TABLE t(x INTEGER)");
    assert(!r1->HasError());

    // Insert
    auto r2 = conn.Query("INSERT INTO t VALUES (42)");
    assert(!r2->HasError());

    // Query still works on same connection
    auto r3 = conn.Query("SELECT x FROM t");
    assert(!r3->HasError());

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

    // Interrupt before connection is created (should not crash)
    session.InterruptQuery();
    assert(session.IsCancelRequested());

    session.ClearCancel();

    // Interrupt after connection is created
    session.GetConnection();
    session.InterruptQuery();
    assert(session.IsCancelRequested());

    std::cout << "    PASSED" << std::endl;
}

void TestMarkQueryEndClearsCancelFlag() {
    std::cout << "  Testing MarkQueryEnd clears cancel_requested..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Simulate: query starts, gets interrupted, then ends
    session.MarkQueryStart();
    assert(session.IsQueryRunning());
    assert(!session.IsCancelRequested());

    session.InterruptQuery();
    assert(session.IsCancelRequested());

    // MarkQueryEnd should clear both query_running AND cancel_requested
    session.MarkQueryEnd();
    assert(!session.IsQueryRunning());
    assert(!session.IsCancelRequested());

    std::cout << "    PASSED" << std::endl;
}

void TestInterruptRunningQuery() {
    std::cout << "  Testing interrupt a running DuckDB query..." << std::endl;

    auto db = CreateDB();
    Session session(1, db->instance);

    // Force connection creation
    auto& conn = session.GetConnection();
    (void)conn;

    // Run a long query in a background thread and interrupt it
    std::atomic<bool> query_started{false};
    std::atomic<bool> query_done{false};
    bool had_error = false;

    std::thread query_thread([&]() {
        session.MarkQueryStart();
        query_started = true;
        // generate_series produces a large result set that takes time to iterate
        auto result = session.GetConnection().Query(
            "SELECT count(*) FROM generate_series(1, 100000000)");
        had_error = result->HasError();
        session.MarkQueryEnd();
        query_done = true;
    });

    // Wait for query to start executing
    while (!query_started) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Give DuckDB a moment to begin execution
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Interrupt
    session.InterruptQuery();

    // Wait for query thread to finish (should finish quickly after interrupt)
    query_thread.join();

    assert(query_done);
    // The query should have been interrupted (error) OR finished before interrupt arrived.
    // Either way, cancel_requested should be cleared by MarkQueryEnd.
    assert(!session.IsCancelRequested());
    assert(!session.IsQueryRunning());

    // Subsequent query should succeed (connection is still usable)
    auto result = session.GetConnection().Query("SELECT 42 AS answer");
    assert(!result->HasError());

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

    std::cout << "\n1. Construction:" << std::endl;
    TestSessionConstruction();

    std::cout << "\n2. State Management:" << std::endl;
    TestSessionState();
    TestSessionBackendKeyData();

    std::cout << "\n3. Sticky Connection:" << std::endl;
    TestStickyConnection();
    TestConnectionPersistsAcrossQueries();

    std::cout << "\n4. Query Tracking:" << std::endl;
    TestQueryTracking();
    TestInterruptQuery();
    TestMarkQueryEndClearsCancelFlag();
    TestInterruptRunningQuery();

    std::cout << "\n5. Session Expiry:" << std::endl;
    TestSessionExpiry();
    TestSessionTouch();

    std::cout << "\n6. Timestamps:" << std::endl;
    TestSessionTimestamps();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
