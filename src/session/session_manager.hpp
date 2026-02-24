//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session_manager.hpp
//
// Session manager with connection pooling
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "session/session.hpp"
#include "session/connection_pool.hpp"
#include "duckdb.hpp"
#include <parallel_hashmap/phmap.h>

namespace duckdb_server {

class SessionManager {
public:
    struct Config {
        // Session settings
        size_t max_sessions;
        std::chrono::minutes session_timeout;

        // Connection pool settings
        size_t pool_min_connections;
        size_t pool_max_connections;
        std::chrono::seconds pool_idle_timeout;
        std::chrono::milliseconds pool_acquire_timeout;
        bool pool_validate_on_acquire;

        Config()
            : max_sessions(DEFAULT_MAX_CONNECTIONS)
            , session_timeout(DEFAULT_SESSION_TIMEOUT_MINUTES)
            , pool_min_connections(5)
            , pool_max_connections(50)
            , pool_idle_timeout(300)
            , pool_acquire_timeout(5000)
            , pool_validate_on_acquire(false) {}
    };

    explicit SessionManager(std::shared_ptr<duckdb::DuckDB> db_p,
                           const Config& config_p = Config{},
                           std::chrono::milliseconds query_timeout_p = std::chrono::milliseconds(300000));

    // Legacy constructor for backward compatibility
    explicit SessionManager(std::shared_ptr<duckdb::DuckDB> db_p,
                           size_t max_sessions,
                           std::chrono::minutes session_timeout = std::chrono::minutes(DEFAULT_SESSION_TIMEOUT_MINUTES));

    ~SessionManager();

    // Non-copyable
    SessionManager(const SessionManager&) = delete;
    SessionManager& operator=(const SessionManager&) = delete;

    // Create a new session
    SessionPtr CreateSession();

    // Get an existing session
    SessionPtr GetSession(uint64_t session_id);

    // Remove a session
    bool RemoveSession(uint64_t session_id);

    // Cleanup expired sessions
    size_t CleanupExpiredSessions();

    // Get statistics
    size_t GetActiveSessionCount() const;
    size_t GetMaxSessions() const { return config.max_sessions; }
    uint64_t GetTotalSessionsCreated() const { return total_sessions_created; }

    // Connection pool statistics
    ConnectionPool::Stats GetPoolStats() const;

    // Cancel a running query by process_id and secret_key
    bool CancelQuery(int32_t process_id, int32_t secret_key);

    // Get DuckDB instance
    duckdb::DuckDB& GetDatabase() { return *db; }

    // Get connection pool
    ConnectionPool& GetConnectionPool() { return *connection_pool; }

private:
    // Generate next session ID
    uint64_t NextSessionId();

    // Start cleanup timer
    void StartCleanupTimer();

private:
    // DuckDB instance
    std::shared_ptr<duckdb::DuckDB> db;

    // Connection pool
    std::unique_ptr<ConnectionPool> connection_pool;

    // Sessions - using parallel_flat_hash_map for high-concurrency access
    // The parallel version internally shards the map and uses fine-grained locking
    phmap::parallel_flat_hash_map<
        uint64_t,
        SessionPtr,
        phmap::priv::hash_default_hash<uint64_t>,
        phmap::priv::hash_default_eq<uint64_t>,
        phmap::priv::Allocator<phmap::priv::Pair<const uint64_t, SessionPtr>>,
        4,  // N=4 means 2^4=16 submaps for parallel access
        std::mutex
    > sessions;

    // Configuration
    Config config;
    std::chrono::milliseconds query_timeout;

    // Session ID generator
    std::atomic<uint64_t> next_session_id;

    // Statistics
    std::atomic<uint64_t> total_sessions_created;

    // Cleanup timer
    std::atomic<bool> cleanup_running;
    std::thread cleanup_thread;
    std::mutex cleanup_mutex;
    std::condition_variable cleanup_cv;
    int cleanup_counter = 0;
};

} // namespace duckdb_server
