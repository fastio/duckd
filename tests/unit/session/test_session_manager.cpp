//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/session/test_session_manager.cpp
//
// Unit tests for SessionManager (sticky session model)
//===----------------------------------------------------------------------===//

#include "session/session_manager.hpp"
#include "duckdb.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>

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

void TestSessionManagerConstruction() {
    std::cout << "  Testing construction..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db);

    assert(mgr.GetActiveSessionCount() == 0);
    assert(mgr.GetTotalSessionsCreated() == 0);
    assert(mgr.GetMaxSessions() == DEFAULT_MAX_CONNECTIONS);

    std::cout << "    PASSED" << std::endl;
}

void TestSessionManagerLegacyConstruction() {
    std::cout << "  Testing legacy construction..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db, 200, std::chrono::minutes(60));

    assert(mgr.GetMaxSessions() == 200);
    assert(mgr.GetActiveSessionCount() == 0);

    std::cout << "    PASSED" << std::endl;
}

void TestSessionManagerCustomConfig() {
    std::cout << "  Testing custom config construction..." << std::endl;

    auto db = CreateDB();
    SessionManager::Config config;
    config.max_sessions = 50;

    SessionManager mgr(db, config);

    assert(mgr.GetMaxSessions() == 50);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Session Creation Tests
//===----------------------------------------------------------------------===//

void TestCreateSession() {
    std::cout << "  Testing CreateSession..." << std::endl;

    auto db = CreateDB();
    SessionManager::Config config;
    config.max_sessions = 100;

    SessionManager mgr(db, config);

    auto session = mgr.CreateSession();
    assert(session != nullptr);
    assert(session->GetSessionId() >= 1);
    assert(mgr.GetActiveSessionCount() == 1);
    assert(mgr.GetTotalSessionsCreated() == 1);

    std::cout << "    PASSED" << std::endl;
}

void TestCreateMultipleSessions() {
    std::cout << "  Testing CreateSession multiple..." << std::endl;

    auto db = CreateDB();
    SessionManager::Config config;
    config.max_sessions = 100;

    SessionManager mgr(db, config);

    std::vector<SessionPtr> sessions;
    for (int i = 0; i < 10; i++) {
        auto s = mgr.CreateSession();
        assert(s != nullptr);
        sessions.push_back(s);
    }

    assert(mgr.GetActiveSessionCount() == 10);
    assert(mgr.GetTotalSessionsCreated() == 10);

    // Session IDs should be unique
    for (size_t i = 0; i < sessions.size(); i++) {
        for (size_t j = i + 1; j < sessions.size(); j++) {
            assert(sessions[i]->GetSessionId() != sessions[j]->GetSessionId());
        }
    }

    std::cout << "    PASSED" << std::endl;
}

void TestCreateSessionMaxLimit() {
    std::cout << "  Testing CreateSession max limit..." << std::endl;

    auto db = CreateDB();
    SessionManager::Config config;
    config.max_sessions = 3;

    SessionManager mgr(db, config);

    // Create up to max
    std::vector<SessionPtr> sessions;
    for (int i = 0; i < 3; i++) {
        auto s = mgr.CreateSession();
        assert(s != nullptr);
        sessions.push_back(s);
    }

    // Next should fail
    auto overflow = mgr.CreateSession();
    assert(overflow == nullptr);

    assert(mgr.GetActiveSessionCount() == 3);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Get Session Tests
//===----------------------------------------------------------------------===//

void TestGetSession() {
    std::cout << "  Testing GetSession..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db);

    auto session = mgr.CreateSession();
    uint64_t id = session->GetSessionId();

    auto retrieved = mgr.GetSession(id);
    assert(retrieved != nullptr);
    assert(retrieved->GetSessionId() == id);

    // Non-existent
    auto missing = mgr.GetSession(99999);
    assert(missing == nullptr);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Remove Session Tests
//===----------------------------------------------------------------------===//

void TestRemoveSession() {
    std::cout << "  Testing RemoveSession..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db);

    auto session = mgr.CreateSession();
    uint64_t id = session->GetSessionId();
    session.reset();  // Release our reference

    assert(mgr.GetActiveSessionCount() == 1);

    bool removed = mgr.RemoveSession(id);
    assert(removed);
    assert(mgr.GetActiveSessionCount() == 0);

    // Double remove
    removed = mgr.RemoveSession(id);
    assert(!removed);

    // Remove non-existent
    removed = mgr.RemoveSession(99999);
    assert(!removed);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Cancel Query Tests
//===----------------------------------------------------------------------===//

void TestCancelQuery() {
    std::cout << "  Testing CancelQuery..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db);

    auto session = mgr.CreateSession();
    session->SetBackendKeyData(100, 200);

    // Cancel with correct credentials
    bool cancelled = mgr.CancelQuery(100, 200);
    assert(cancelled);
    assert(session->IsCancelRequested());

    // Cancel with wrong credentials
    session->ClearCancel();
    cancelled = mgr.CancelQuery(100, 999);
    assert(!cancelled);
    assert(!session->IsCancelRequested());

    // Cancel with wrong PID
    cancelled = mgr.CancelQuery(999, 200);
    assert(!cancelled);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Session with Connection Tests
//===----------------------------------------------------------------------===//

void TestSessionUsesConnection() {
    std::cout << "  Testing session uses sticky connection..." << std::endl;

    auto db = CreateDB();
    SessionManager mgr(db);

    auto session = mgr.CreateSession();

    // Use connection
    auto& conn = session->GetConnection();
    auto result = conn.Query("SELECT 42 AS answer");
    assert(!result->HasError());

    // Same connection returned on second call
    auto& conn2 = session->GetConnection();
    assert(&conn == &conn2);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Concurrent Session Creation Tests
//===----------------------------------------------------------------------===//

void TestConcurrentSessionCreation() {
    std::cout << "  Testing concurrent session creation..." << std::endl;

    auto db = CreateDB();
    SessionManager::Config config;
    config.max_sessions = 10000;

    SessionManager mgr(db, config);

    const int num_threads = 4;
    const int sessions_per_thread = 25;
    std::atomic<int> created{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < sessions_per_thread; i++) {
                auto s = mgr.CreateSession();
                if (s) created++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(created == num_threads * sessions_per_thread);
    assert(mgr.GetActiveSessionCount() == static_cast<size_t>(num_threads * sessions_per_thread));

    std::cout << "    PASSED (" << created.load() << " sessions created)" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== SessionManager Unit Tests ===" << std::endl;

    std::cout << "\n1. Construction Tests:" << std::endl;
    TestSessionManagerConstruction();
    TestSessionManagerLegacyConstruction();
    TestSessionManagerCustomConfig();

    std::cout << "\n2. Session Creation:" << std::endl;
    TestCreateSession();
    TestCreateMultipleSessions();
    TestCreateSessionMaxLimit();

    std::cout << "\n3. Get Session:" << std::endl;
    TestGetSession();

    std::cout << "\n4. Remove Session:" << std::endl;
    TestRemoveSession();

    std::cout << "\n5. Cancel Query:" << std::endl;
    TestCancelQuery();

    std::cout << "\n6. Session with Connection:" << std::endl;
    TestSessionUsesConnection();

    std::cout << "\n7. Concurrent Access:" << std::endl;
    TestConcurrentSessionCreation();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
