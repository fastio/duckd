//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/connection_pool.hpp
//
// DuckDB connection pool for efficient connection reuse
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "duckdb.hpp"
#include <parallel_hashmap/phmap.h>
#include <vector>
#include <condition_variable>
#include <atomic>
#include <functional>

namespace duckdb_server {

//===----------------------------------------------------------------------===//
// Pooled Connection - RAII wrapper that returns connection to pool
//===----------------------------------------------------------------------===//

class ConnectionPool;

class PooledConnection {
public:
    PooledConnection() : pool(nullptr), connection(nullptr) {}
    PooledConnection(ConnectionPool* pool_p, duckdb::Connection* conn);
    ~PooledConnection();

    // Move only
    PooledConnection(PooledConnection&& other) noexcept;
    PooledConnection& operator=(PooledConnection&& other) noexcept;
    PooledConnection(const PooledConnection&) = delete;
    PooledConnection& operator=(const PooledConnection&) = delete;

    // Access the connection
    duckdb::Connection* Get() const { return connection; }
    duckdb::Connection* operator->() const { return connection; }
    duckdb::Connection& operator*() const { return *connection; }

    // Check if valid
    explicit operator bool() const { return connection != nullptr; }

    // Release back to pool manually (called automatically by destructor)
    void Release();

private:
    ConnectionPool* pool;
    duckdb::Connection* connection;
};

//===----------------------------------------------------------------------===//
// Connection Pool
//===----------------------------------------------------------------------===//

class ConnectionPool {
public:
    struct Config {
        size_t min_connections;
        size_t max_connections;
        std::chrono::seconds idle_timeout;
        std::chrono::milliseconds acquire_timeout;
        bool validate_on_acquire;

        Config()
            : min_connections(5)
            , max_connections(50)
            , idle_timeout(300)
            , acquire_timeout(5000)
            , validate_on_acquire(false) {}
    };

    struct Stats {
        size_t total_created = 0;
        size_t total_destroyed = 0;
        size_t current_size = 0;
        size_t available = 0;
        size_t in_use = 0;
        size_t acquire_count = 0;
        size_t acquire_timeout_count = 0;
        size_t validation_failure_count = 0;
    };

    explicit ConnectionPool(duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p,
                           const Config& config_p = Config{});
    ~ConnectionPool();

    // Non-copyable
    ConnectionPool(const ConnectionPool&) = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // Acquire a connection from the pool
    // Returns empty PooledConnection if timeout or pool exhausted
    PooledConnection Acquire();
    PooledConnection Acquire(std::chrono::milliseconds timeout);

    // Get statistics
    Stats GetStats() const;

    // Configuration
    const Config& GetConfig() const { return config; }

    // Resize pool
    void SetMinConnections(size_t min_conn);
    void SetMaxConnections(size_t max_conn);

    // Shutdown pool (release all connections)
    void Shutdown();

private:
    friend class PooledConnection;

    // Internal connection wrapper with metadata
    struct PoolEntry {
        std::unique_ptr<duckdb::Connection> connection;
        TimePoint created_at;
        TimePoint last_used;
        uint64_t use_count = 0;

        PoolEntry(std::unique_ptr<duckdb::Connection> conn)
            : connection(std::move(conn))
            , created_at(Clock::now())
            , last_used(Clock::now()) {}
    };

    // Release connection back to pool (called by PooledConnection)
    void Release(duckdb::Connection* conn);

    // Create a new connection
    std::unique_ptr<PoolEntry> CreateConnection();

    // Validate a connection is still usable
    bool ValidateConnection(duckdb::Connection* conn);

    // Maintenance: remove idle connections, ensure minimum pool size
    void MaintenanceLoop();

    // Ensure minimum connections exist
    void EnsureMinConnections();

private:
    duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance;
    Config config;

    // Pool state
    std::vector<std::unique_ptr<PoolEntry>> available;
    // Using phmap::flat_hash_map for better performance than std::unordered_map
    // (SwissTable-based implementation from Google Abseil)
    phmap::flat_hash_map<duckdb::Connection*, std::unique_ptr<PoolEntry>> in_use;
    mutable std::mutex mutex;
    std::condition_variable available_cv;

    // Statistics
    mutable std::atomic<size_t> total_created{0};
    mutable std::atomic<size_t> total_destroyed{0};
    mutable std::atomic<size_t> acquire_count{0};
    mutable std::atomic<size_t> acquire_timeout_count{0};
    mutable std::atomic<size_t> validation_failure_count{0};

    // Maintenance thread
    std::atomic<bool> running{true};
    std::thread maintenance_thread;
    std::mutex maintenance_mutex;
    std::condition_variable maintenance_cv;
};

} // namespace duckdb_server
