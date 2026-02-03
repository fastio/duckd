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
#include <queue>
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
    PooledConnection() : pool_(nullptr), connection_(nullptr) {}
    PooledConnection(ConnectionPool* pool, duckdb::Connection* conn);
    ~PooledConnection();

    // Move only
    PooledConnection(PooledConnection&& other) noexcept;
    PooledConnection& operator=(PooledConnection&& other) noexcept;
    PooledConnection(const PooledConnection&) = delete;
    PooledConnection& operator=(const PooledConnection&) = delete;

    // Access the connection
    duckdb::Connection* Get() const { return connection_; }
    duckdb::Connection* operator->() const { return connection_; }
    duckdb::Connection& operator*() const { return *connection_; }

    // Check if valid
    explicit operator bool() const { return connection_ != nullptr; }

    // Release back to pool manually (called automatically by destructor)
    void Release();

private:
    ConnectionPool* pool_;
    duckdb::Connection* connection_;
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
            , validate_on_acquire(true) {}
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

    explicit ConnectionPool(duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance,
                           const Config& config = Config{});
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
    const Config& GetConfig() const { return config_; }

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
    duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_;
    Config config_;

    // Pool state
    std::queue<std::unique_ptr<PoolEntry>> available_;
    std::unordered_map<duckdb::Connection*, std::unique_ptr<PoolEntry>> in_use_;
    mutable std::mutex mutex_;
    std::condition_variable available_cv_;

    // Statistics
    mutable std::atomic<size_t> total_created_{0};
    mutable std::atomic<size_t> total_destroyed_{0};
    mutable std::atomic<size_t> acquire_count_{0};
    mutable std::atomic<size_t> acquire_timeout_count_{0};
    mutable std::atomic<size_t> validation_failure_count_{0};

    // Maintenance thread
    std::atomic<bool> running_{true};
    std::thread maintenance_thread_;
};

} // namespace duckdb_server
