//===----------------------------------------------------------------------===//
//                         DuckD Server - Unit Tests
//
// tests/unit/session/test_connection_pool.cpp
//
// Unit tests for ConnectionPool
//===----------------------------------------------------------------------===//

#include "session/connection_pool.hpp"
#include "duckdb.hpp"
#include <cassert>
#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <vector>

using namespace duckdb_server;

// Helper: create in-memory DuckDB instance
static duckdb::shared_ptr<duckdb::DatabaseInstance> CreateDB() {
    auto db = std::make_unique<duckdb::DuckDB>(nullptr);
    return db->instance;
}

//===----------------------------------------------------------------------===//
// Construction Tests
//===----------------------------------------------------------------------===//

void TestPoolDefaultConstruction() {
    std::cout << "  Testing default construction..." << std::endl;

    auto db = CreateDB();
    ConnectionPool pool(db);

    auto stats = pool.GetStats();
    // Default min_connections = 5
    assert(stats.current_size >= 5);
    assert(stats.available >= 5);
    assert(stats.in_use == 0);
    assert(stats.total_created >= 5);

    std::cout << "    PASSED (created " << stats.current_size << " connections)" << std::endl;
}

void TestPoolCustomConfig() {
    std::cout << "  Testing custom config..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 10;
    config.acquire_timeout = std::chrono::milliseconds(1000);

    ConnectionPool pool(db, config);

    auto stats = pool.GetStats();
    assert(stats.current_size >= 2);
    assert(stats.available >= 2);

    auto& cfg = pool.GetConfig();
    assert(cfg.min_connections == 2);
    assert(cfg.max_connections == 10);
    assert(cfg.acquire_timeout == std::chrono::milliseconds(1000));

    std::cout << "    PASSED" << std::endl;
}

void TestPoolZeroMinConnections() {
    std::cout << "  Testing zero min connections..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 0;
    config.max_connections = 5;

    ConnectionPool pool(db, config);

    auto stats = pool.GetStats();
    assert(stats.current_size == 0);
    assert(stats.available == 0);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Acquire/Release Tests
//===----------------------------------------------------------------------===//

void TestAcquireRelease() {
    std::cout << "  Testing Acquire/Release..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 10;

    ConnectionPool pool(db, config);

    {
        auto conn = pool.Acquire();
        assert(static_cast<bool>(conn));
        assert(conn.Get() != nullptr);

        auto stats = pool.GetStats();
        assert(stats.in_use == 1);
        assert(stats.acquire_count == 1);

        // Use connection
        auto result = conn->Query("SELECT 42 AS answer");
        assert(!result->HasError());
    }
    // conn released by RAII

    auto stats = pool.GetStats();
    assert(stats.in_use == 0);

    std::cout << "    PASSED" << std::endl;
}

void TestAcquireMultiple() {
    std::cout << "  Testing Acquire multiple connections..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 10;

    ConnectionPool pool(db, config);

    std::vector<PooledConnection> connections;
    for (int i = 0; i < 5; i++) {
        auto conn = pool.Acquire();
        assert(static_cast<bool>(conn));
        connections.push_back(std::move(conn));
    }

    auto stats = pool.GetStats();
    assert(stats.in_use == 5);
    assert(stats.acquire_count == 5);

    // Release all
    connections.clear();

    stats = pool.GetStats();
    assert(stats.in_use == 0);
    assert(stats.available >= 5);

    std::cout << "    PASSED" << std::endl;
}

void TestAcquireMaxLimit() {
    std::cout << "  Testing Acquire at max limit..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 3;
    config.acquire_timeout = std::chrono::milliseconds(100);

    ConnectionPool pool(db, config);

    // Acquire all connections
    std::vector<PooledConnection> connections;
    for (int i = 0; i < 3; i++) {
        auto conn = pool.Acquire();
        assert(static_cast<bool>(conn));
        connections.push_back(std::move(conn));
    }

    auto stats = pool.GetStats();
    assert(stats.in_use == 3);

    // Next acquire should timeout (returns empty)
    auto conn = pool.Acquire(std::chrono::milliseconds(50));
    assert(!static_cast<bool>(conn));

    stats = pool.GetStats();
    assert(stats.acquire_timeout_count >= 1);

    std::cout << "    PASSED" << std::endl;
}

void TestManualRelease() {
    std::cout << "  Testing manual Release..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 5;

    ConnectionPool pool(db, config);

    auto conn = pool.Acquire();
    assert(static_cast<bool>(conn));

    auto stats = pool.GetStats();
    assert(stats.in_use == 1);

    conn.Release();
    assert(!static_cast<bool>(conn));
    assert(conn.Get() == nullptr);

    stats = pool.GetStats();
    assert(stats.in_use == 0);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// PooledConnection Move Semantics Tests
//===----------------------------------------------------------------------===//

void TestPooledConnectionMove() {
    std::cout << "  Testing PooledConnection move semantics..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 5;

    ConnectionPool pool(db, config);

    // Move constructor
    auto conn1 = pool.Acquire();
    assert(static_cast<bool>(conn1));

    auto conn2 = std::move(conn1);
    assert(static_cast<bool>(conn2));
    assert(!static_cast<bool>(conn1));

    // Move assignment
    PooledConnection conn3;
    assert(!static_cast<bool>(conn3));
    conn3 = std::move(conn2);
    assert(static_cast<bool>(conn3));
    assert(!static_cast<bool>(conn2));

    // Only one in use
    auto stats = pool.GetStats();
    assert(stats.in_use == 1);

    conn3.Release();
    stats = pool.GetStats();
    assert(stats.in_use == 0);

    std::cout << "    PASSED" << std::endl;
}

void TestPooledConnectionDefaultConstruct() {
    std::cout << "  Testing PooledConnection default construction..." << std::endl;

    PooledConnection conn;
    assert(!static_cast<bool>(conn));
    assert(conn.Get() == nullptr);

    // Release on empty should be safe
    conn.Release();

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Validation Tests
//===----------------------------------------------------------------------===//

void TestValidateOnAcquire() {
    std::cout << "  Testing validate_on_acquire..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 2;
    config.max_connections = 5;
    config.validate_on_acquire = true;

    ConnectionPool pool(db, config);

    // Should still work - connections are valid
    auto conn = pool.Acquire();
    assert(static_cast<bool>(conn));

    auto result = conn->Query("SELECT 1");
    assert(!result->HasError());

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Stats Tests
//===----------------------------------------------------------------------===//

void TestPoolStats() {
    std::cout << "  Testing GetStats..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 3;
    config.max_connections = 10;

    ConnectionPool pool(db, config);

    auto stats = pool.GetStats();
    assert(stats.total_created >= 3);
    assert(stats.total_destroyed == 0);
    assert(stats.current_size >= 3);
    assert(stats.available >= 3);
    assert(stats.in_use == 0);
    assert(stats.acquire_count == 0);
    assert(stats.acquire_timeout_count == 0);
    assert(stats.validation_failure_count == 0);

    // Acquire and check
    auto conn = pool.Acquire();
    stats = pool.GetStats();
    assert(stats.acquire_count == 1);
    assert(stats.in_use == 1);

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Resize Tests
//===----------------------------------------------------------------------===//

void TestSetMinConnections() {
    std::cout << "  Testing SetMinConnections..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 20;

    ConnectionPool pool(db, config);

    auto stats = pool.GetStats();
    assert(stats.current_size >= 1);

    // Increase min
    pool.SetMinConnections(5);
    stats = pool.GetStats();
    assert(stats.current_size >= 5);

    std::cout << "    PASSED" << std::endl;
}

void TestSetMaxConnections() {
    std::cout << "  Testing SetMaxConnections..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 3;
    config.acquire_timeout = std::chrono::milliseconds(50);

    ConnectionPool pool(db, config);

    // Acquire max
    std::vector<PooledConnection> conns;
    for (int i = 0; i < 3; i++) {
        conns.push_back(pool.Acquire());
    }

    // Can't acquire more
    auto fail_conn = pool.Acquire(std::chrono::milliseconds(50));
    assert(!static_cast<bool>(fail_conn));

    // Increase max
    pool.SetMaxConnections(5);

    // Now should succeed
    auto ok_conn = pool.Acquire();
    assert(static_cast<bool>(ok_conn));

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Concurrent Access Tests
//===----------------------------------------------------------------------===//

void TestConcurrentAcquireRelease() {
    std::cout << "  Testing concurrent Acquire/Release..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 5;
    config.max_connections = 20;

    ConnectionPool pool(db, config);

    const int num_threads = 8;
    const int ops_per_thread = 50;
    std::atomic<int> completed{0};
    std::atomic<int> errors{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < ops_per_thread; i++) {
                auto conn = pool.Acquire();
                if (!conn) {
                    errors++;
                    continue;
                }
                // Use connection
                auto result = conn->Query("SELECT 1");
                if (result->HasError()) {
                    errors++;
                }
                completed++;
                // conn auto-released
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    assert(errors == 0);
    assert(completed == num_threads * ops_per_thread);

    auto stats = pool.GetStats();
    assert(stats.in_use == 0);
    assert(stats.acquire_count == static_cast<size_t>(num_threads * ops_per_thread));

    std::cout << "    PASSED (" << completed.load() << " operations, 0 errors)" << std::endl;
}

//===----------------------------------------------------------------------===//
// Shutdown Tests
//===----------------------------------------------------------------------===//

void TestPoolShutdown() {
    std::cout << "  Testing Shutdown..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 3;
    config.max_connections = 10;

    ConnectionPool pool(db, config);

    // Acquire some
    auto conn1 = pool.Acquire();
    assert(static_cast<bool>(conn1));

    pool.Shutdown();

    // Acquire after shutdown should return empty
    auto conn2 = pool.Acquire(std::chrono::milliseconds(50));
    assert(!static_cast<bool>(conn2));

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Connection Reuse Tests
//===----------------------------------------------------------------------===//

void TestConnectionReuse() {
    std::cout << "  Testing connection reuse (LIFO)..." << std::endl;

    auto db = CreateDB();
    ConnectionPool::Config config;
    config.min_connections = 1;
    config.max_connections = 5;

    ConnectionPool pool(db, config);

    // Acquire, remember pointer, release
    duckdb::Connection* raw_ptr;
    {
        auto conn = pool.Acquire();
        raw_ptr = conn.Get();
    }

    // Next acquire should get the same connection (LIFO)
    {
        auto conn = pool.Acquire();
        assert(conn.Get() == raw_ptr);
    }

    std::cout << "    PASSED" << std::endl;
}

//===----------------------------------------------------------------------===//
// Main
//===----------------------------------------------------------------------===//

int main() {
    std::cout << "=== ConnectionPool Unit Tests ===" << std::endl;

    std::cout << "\n1. Construction Tests:" << std::endl;
    TestPoolDefaultConstruction();
    TestPoolCustomConfig();
    TestPoolZeroMinConnections();

    std::cout << "\n2. Acquire/Release Tests:" << std::endl;
    TestAcquireRelease();
    TestAcquireMultiple();
    TestAcquireMaxLimit();
    TestManualRelease();

    std::cout << "\n3. PooledConnection Tests:" << std::endl;
    TestPooledConnectionMove();
    TestPooledConnectionDefaultConstruct();

    std::cout << "\n4. Validation Tests:" << std::endl;
    TestValidateOnAcquire();

    std::cout << "\n5. Stats Tests:" << std::endl;
    TestPoolStats();

    std::cout << "\n6. Resize Tests:" << std::endl;
    TestSetMinConnections();
    TestSetMaxConnections();

    std::cout << "\n7. Concurrent Access:" << std::endl;
    TestConcurrentAcquireRelease();

    std::cout << "\n8. Shutdown:" << std::endl;
    TestPoolShutdown();

    std::cout << "\n9. Connection Reuse:" << std::endl;
    TestConnectionReuse();

    std::cout << "\n=== All tests PASSED ===" << std::endl;
    return 0;
}
