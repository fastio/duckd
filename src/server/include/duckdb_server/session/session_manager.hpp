//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// duckdb_server/session/session_manager.hpp
//
// Session manager
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_server/common.hpp"
#include "duckdb_server/session/session.hpp"
#include "duckdb.hpp"

namespace duckdb_server {

class SessionManager {
public:
    explicit SessionManager(std::shared_ptr<duckdb::DuckDB> db,
                           size_t max_sessions = DEFAULT_MAX_CONNECTIONS,
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
    size_t GetMaxSessions() const { return max_sessions_; }
    uint64_t GetTotalSessionsCreated() const { return total_sessions_created_; }
    
    // Get DuckDB instance
    duckdb::DuckDB& GetDatabase() { return *db_; }

private:
    // Generate next session ID
    uint64_t NextSessionId();
    
    // Start cleanup timer
    void StartCleanupTimer();

private:
    // DuckDB instance
    std::shared_ptr<duckdb::DuckDB> db_;
    
    // Sessions
    std::unordered_map<uint64_t, SessionPtr> sessions_;
    mutable std::mutex sessions_mutex_;
    
    // Configuration
    size_t max_sessions_;
    std::chrono::minutes session_timeout_;
    
    // Session ID generator
    std::atomic<uint64_t> next_session_id_;
    
    // Statistics
    std::atomic<uint64_t> total_sessions_created_;
    
    // Cleanup timer
    std::atomic<bool> cleanup_running_;
    std::thread cleanup_thread_;
};

} // namespace duckdb_server
