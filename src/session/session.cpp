//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session.cpp
//
// Session implementation
//===----------------------------------------------------------------------===//

#include "session/session.hpp"
#include "logging/logger.hpp"
#include <stdexcept>

namespace duckdb_server {

// Constructor with connection pool (lazy acquisition)
Session::Session(uint64_t session_id, ConnectionPool* pool)
    : session_id_(session_id)
    , created_at_(Clock::now())
    , last_active_(Clock::now())
    , pool_(pool)
    , current_database_("main")
    , current_schema_("main")
    , current_query_id_(0)
    , cancel_requested_(false) {
}

// Legacy constructor (creates own connection)
Session::Session(uint64_t session_id, duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance)
    : session_id_(session_id)
    , created_at_(Clock::now())
    , last_active_(Clock::now())
    , owned_connection_(std::make_unique<duckdb::Connection>(*db_instance))
    , current_database_("main")
    , current_schema_("main")
    , current_query_id_(0)
    , cancel_requested_(false) {
}

Session::~Session() {
    LOG_DEBUG("session", "Session " + std::to_string(session_id_) + " destructor - returning connection to pool");
    ClearPreparedStatements();
    // PooledConnection will automatically return to pool in its destructor
}

duckdb::Connection& Session::GetConnection() {
    // Lazy acquisition: acquire from pool on first use
    if (active_connection_) {
        return *active_connection_;
    }
    if (owned_connection_) {
        return *owned_connection_;
    }
    if (pool_) {
        active_connection_ = pool_->Acquire();
        if (active_connection_) {
            return *active_connection_;
        }
        throw std::runtime_error("Failed to acquire connection from pool");
    }
    throw std::runtime_error("Session has no valid connection");
}

void Session::ReleaseConnection() {
    if (active_connection_) {
        LOG_DEBUG("session", "Session " + std::to_string(session_id_) + " releasing connection back to pool");
        active_connection_.Release();
    }
}

bool Session::HasActiveConnection() const {
    return static_cast<bool>(active_connection_) || static_cast<bool>(owned_connection_);
}

bool Session::HasConnection() const {
    return static_cast<bool>(active_connection_) || static_cast<bool>(owned_connection_) || (pool_ != nullptr);
}

void Session::AddPreparedStatement(const std::string& name,
                                   std::unique_ptr<duckdb::PreparedStatement> stmt) {
    std::lock_guard<std::mutex> lock(prepared_mutex_);
    prepared_statements_[name] = std::move(stmt);
}

duckdb::PreparedStatement* Session::GetPreparedStatement(const std::string& name) {
    std::lock_guard<std::mutex> lock(prepared_mutex_);
    auto it = prepared_statements_.find(name);
    if (it != prepared_statements_.end()) {
        return it->second.get();
    }
    return nullptr;
}

bool Session::RemovePreparedStatement(const std::string& name) {
    std::lock_guard<std::mutex> lock(prepared_mutex_);
    return prepared_statements_.erase(name) > 0;
}

void Session::ClearPreparedStatements() {
    std::lock_guard<std::mutex> lock(prepared_mutex_);
    prepared_statements_.clear();
}

bool Session::IsExpired(std::chrono::minutes timeout) const {
    auto now = Clock::now();
    return (now - last_active_) > timeout;
}

} // namespace duckdb_server
