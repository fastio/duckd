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
            , pool_validate_on_acquire(true) {}
    };

    explicit SessionManager(std::shared_ptr<duckdb::DuckDB> db,
                           const Config& config = Config{});

    // Legacy constructor for backward compatibility
    explicit SessionManager(std::shared_ptr<duckdb::DuckDB> db,
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
    size_t GetMaxSessions() const { return config_.max_sessions; }
    uint64_t GetTotalSessionsCreated() const { return total_sessions_created_; }

    // Connection pool statistics
    ConnectionPool::Stats GetPoolStats() const;

    // Get DuckDB instance
    duckdb::DuckDB& GetDatabase() { return *db_; }

    // Get connection pool
    ConnectionPool& GetConnectionPool() { return *connection_pool_; }

private:
    // Generate next session ID
    uint64_t NextSessionId();

    // Start cleanup timer
    void StartCleanupTimer();

private:
    // DuckDB instance
    std::shared_ptr<duckdb::DuckDB> db_;

    // Connection pool
    std::unique_ptr<ConnectionPool> connection_pool_;

    // Sessions
    std::unordered_map<uint64_t, SessionPtr> sessions_;
    mutable std::mutex sessions_mutex_;

    // Configuration
    Config config_;

    // Session ID generator
    std::atomic<uint64_t> next_session_id_;

    // Statistics
    std::atomic<uint64_t> total_sessions_created_;

    // Cleanup timer
    std::atomic<bool> cleanup_running_;
    std::thread cleanup_thread_;
};

} // namespace duckdb_server
