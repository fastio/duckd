//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session.hpp
//
// Session management
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "session/connection_pool.hpp"
#include "duckdb.hpp"
#include <parallel_hashmap/phmap.h>

namespace duckdb_server {

class Session {
public:
    using Ptr = std::shared_ptr<Session>;

    // Constructor with pooled connection
    Session(uint64_t session_id, PooledConnection connection);

    // Legacy constructor (creates own connection, for backward compatibility)
    Session(uint64_t session_id,
            duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance);

    ~Session();

    // Non-copyable
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Getters
    uint64_t GetSessionId() const { return session_id_; }
    TimePoint GetCreatedAt() const { return created_at_; }
    TimePoint GetLastActive() const { return last_active_; }

    // Get the underlying connection
    duckdb::Connection& GetConnection();

    // Check if session has a valid connection
    bool HasConnection() const;

    // State
    const std::string& GetCurrentDatabase() const { return current_database_; }
    void SetCurrentDatabase(const std::string& db) { current_database_ = db; }

    const std::string& GetCurrentSchema() const { return current_schema_; }
    void SetCurrentSchema(const std::string& schema) { current_schema_ = schema; }

    // Prepared statements
    void AddPreparedStatement(const std::string& name,
                              std::unique_ptr<duckdb::PreparedStatement> stmt);
    duckdb::PreparedStatement* GetPreparedStatement(const std::string& name);
    bool RemovePreparedStatement(const std::string& name);
    void ClearPreparedStatements();

    // Query tracking
    uint64_t GetCurrentQueryId() const { return current_query_id_; }
    void SetCurrentQueryId(uint64_t id) { current_query_id_ = id; }

    bool IsCancelRequested() const { return cancel_requested_; }
    void RequestCancel() { cancel_requested_ = true; }
    void ClearCancel() { cancel_requested_ = false; }

    // Update activity timestamp
    void Touch() { last_active_ = Clock::now(); }

    // Check if session is expired
    bool IsExpired(std::chrono::minutes timeout) const;

    // Client info
    void SetClientInfo(const std::string& info) { client_info_ = info; }
    const std::string& GetClientInfo() const { return client_info_; }

    void SetUsername(const std::string& user) { username_ = user; }
    const std::string& GetUsername() const { return username_; }

private:
    // Session ID
    uint64_t session_id_;

    // Timestamps
    TimePoint created_at_;
    TimePoint last_active_;

    // DuckDB connection - either pooled or owned
    PooledConnection pooled_connection_;
    std::unique_ptr<duckdb::Connection> owned_connection_;  // Legacy fallback

    // Session state
    std::string current_database_;
    std::string current_schema_;
    std::string client_info_;
    std::string username_;

    // Prepared statements
    // Using phmap::flat_hash_map for better performance than std::unordered_map
    phmap::flat_hash_map<std::string, std::unique_ptr<duckdb::PreparedStatement>> prepared_statements_;
    std::mutex prepared_mutex_;

    // Current query tracking
    std::atomic<uint64_t> current_query_id_;
    std::atomic<bool> cancel_requested_;
};

} // namespace duckdb_server
