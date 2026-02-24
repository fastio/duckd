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

    // Constructor with connection pool (lazy acquisition)
    Session(uint64_t session_id_p, ConnectionPool* pool_p);

    // Legacy constructor (creates own connection, for backward compatibility)
    Session(uint64_t session_id_p,
            duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p);

    ~Session();

    // Non-copyable
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Getters
    uint64_t GetSessionId() const { return session_id; }
    TimePoint GetCreatedAt() const { return created_at; }
    TimePoint GetLastActive() const { return last_active; }

    // Get the underlying connection (acquires from pool on demand)
    duckdb::Connection& GetConnection();

    // Release connection back to pool (only if not in transaction)
    void ReleaseConnection();

    // Check if session currently holds an active connection
    bool HasActiveConnection() const;

    // Check if session has any valid connection source
    bool HasConnection() const;

    // State
    const std::string& GetCurrentDatabase() const { return current_database; }
    void SetCurrentDatabase(const std::string& db) { current_database = db; }

    const std::string& GetCurrentSchema() const { return current_schema; }
    void SetCurrentSchema(const std::string& schema) { current_schema = schema; }

    // Prepared statements
    void AddPreparedStatement(const std::string& name,
                              std::unique_ptr<duckdb::PreparedStatement> stmt);
    duckdb::PreparedStatement* GetPreparedStatement(const std::string& name);
    bool RemovePreparedStatement(const std::string& name);
    void ClearPreparedStatements();

    // Query tracking
    uint64_t GetCurrentQueryId() const { return current_query_id; }
    void SetCurrentQueryId(uint64_t id) { current_query_id = id; }

    bool IsCancelRequested() const { return cancel_requested; }
    void RequestCancel() { cancel_requested = true; }
    void ClearCancel() { cancel_requested = false; }

    // Query timing
    void MarkQueryStart() { query_start_time = Clock::now(); query_running.store(true, std::memory_order_release); }
    void MarkQueryEnd() { query_running = false; }
    bool IsQueryRunning() const { return query_running.load(std::memory_order_acquire); }
    TimePoint GetQueryStartTime() const { return query_start_time; }

    // Interrupt the running query
    void InterruptQuery();

    // Update activity timestamp
    void Touch() { last_active = Clock::now(); }

    // Check if session is expired
    bool IsExpired(std::chrono::minutes timeout) const;

    // Backend key data (for cancel requests)
    void SetBackendKeyData(int32_t process_id_p, int32_t secret_key_p) {
        backend_process_id = process_id_p;
        backend_secret_key = secret_key_p;
    }
    int32_t GetBackendProcessId() const { return backend_process_id; }
    int32_t GetBackendSecretKey() const { return backend_secret_key; }

    // Client info
    void SetClientInfo(const std::string& info) { client_info = info; }
    const std::string& GetClientInfo() const { return client_info; }

    void SetUsername(const std::string& user) { username = user; }
    const std::string& GetUsername() const { return username; }

private:
    // Session ID
    uint64_t session_id;

    // Timestamps
    TimePoint created_at;
    TimePoint last_active;

    // Connection pool (for lazy acquisition)
    ConnectionPool* pool = nullptr;

    // DuckDB connection - lazily acquired from pool or owned
    PooledConnection active_connection;
    std::unique_ptr<duckdb::Connection> owned_connection;  // Legacy fallback

    // Session state
    std::string current_database;
    std::string current_schema;
    std::string client_info;
    std::string username;
    int32_t backend_process_id = 0;
    int32_t backend_secret_key = 0;

    // Prepared statements
    // Using phmap::flat_hash_map for better performance than std::unordered_map
    phmap::flat_hash_map<std::string, std::unique_ptr<duckdb::PreparedStatement>> prepared_statements;
    std::mutex prepared_mutex;

    // Current query tracking
    std::atomic<uint64_t> current_query_id;
    std::atomic<bool> cancel_requested;
    std::atomic<bool> query_running{false};
    TimePoint query_start_time;
};

} // namespace duckdb_server
