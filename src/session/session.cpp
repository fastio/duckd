//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session.cpp
//
// Session implementation
//===----------------------------------------------------------------------===//

#include "session/session.hpp"
#include "utils/logger.hpp"
#include <stdexcept>

namespace duckdb_server {

// Constructor with pooled connection
Session::Session(uint64_t session_id, PooledConnection connection)
    : session_id_(session_id)
    , created_at_(Clock::now())
    , last_active_(Clock::now())
    , pooled_connection_(std::move(connection))
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
    ClearPreparedStatements();
    // PooledConnection will automatically return to pool in its destructor
}

duckdb::Connection& Session::GetConnection() {
    if (pooled_connection_) {
        return *pooled_connection_;
    }
    if (owned_connection_) {
        return *owned_connection_;
    }
    throw std::runtime_error("Session has no valid connection");
}

bool Session::HasConnection() const {
    return static_cast<bool>(pooled_connection_) || static_cast<bool>(owned_connection_);
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
