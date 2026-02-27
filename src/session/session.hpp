//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session.hpp
//
// Session management - sticky session model
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "duckdb.hpp"

namespace duckdb_server {

class Session {
public:
    using Ptr = std::shared_ptr<Session>;

    Session(uint64_t session_id_p, duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p);
    ~Session();

    // Non-copyable
    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Getters
    uint64_t GetSessionId() const { return session_id; }
    TimePoint GetCreatedAt() const { return created_at; }
    TimePoint GetLastActive() const { return last_active; }

    // Get the underlying connection (created lazily, held for session lifetime)
    duckdb::Connection& GetConnection();

    // State
    const std::string& GetCurrentDatabase() const { return current_database; }
    void SetCurrentDatabase(const std::string& db) { current_database = db; }

    const std::string& GetCurrentSchema() const { return current_schema; }
    void SetCurrentSchema(const std::string& schema) { current_schema = schema; }

    // Query tracking
    uint64_t GetCurrentQueryId() const { return current_query_id; }
    void SetCurrentQueryId(uint64_t id) { current_query_id = id; }

    bool IsCancelRequested() const { return cancel_requested; }
    void RequestCancel() { cancel_requested = true; }
    void ClearCancel() { cancel_requested = false; }

    // Query timing
    void MarkQueryStart() { query_start_time = Clock::now(); query_running.store(true, std::memory_order_release); }
    void MarkQueryEnd() { query_running.store(false, std::memory_order_release); cancel_requested.store(false, std::memory_order_relaxed); }
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

    // DuckDB database instance (used to create connection on demand)
    duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance;

    // DuckDB connection - created lazily, held for session lifetime
    std::unique_ptr<duckdb::Connection> connection;

    // Session state
    std::string current_database;
    std::string current_schema;
    std::string client_info;
    std::string username;
    int32_t backend_process_id = 0;
    int32_t backend_secret_key = 0;

    // Current query tracking
    std::atomic<uint64_t> current_query_id;
    std::atomic<bool> cancel_requested;
    std::atomic<bool> query_running{false};
    TimePoint query_start_time;
};

} // namespace duckdb_server
