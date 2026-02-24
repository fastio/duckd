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
Session::Session(uint64_t session_id_p, ConnectionPool* pool_p)
    : session_id(session_id_p)
    , created_at(Clock::now())
    , last_active(Clock::now())
    , pool(pool_p)
    , current_database("main")
    , current_schema("main")
    , current_query_id(0)
    , cancel_requested(false) {
}

// Legacy constructor (creates own connection)
Session::Session(uint64_t session_id_p, duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p)
    : session_id(session_id_p)
    , created_at(Clock::now())
    , last_active(Clock::now())
    , owned_connection(std::make_unique<duckdb::Connection>(*db_instance_p))
    , current_database("main")
    , current_schema("main")
    , current_query_id(0)
    , cancel_requested(false) {
}

Session::~Session() {
    LOG_DEBUG("session", "Session " + std::to_string(session_id) + " destructor - returning connection to pool");
    ClearPreparedStatements();
    // PooledConnection will automatically return to pool in its destructor
}

duckdb::Connection& Session::GetConnection() {
    // Lazy acquisition: acquire from pool on first use
    if (active_connection) {
        return *active_connection;
    }
    if (owned_connection) {
        return *owned_connection;
    }
    if (pool) {
        active_connection = pool->Acquire();
        if (active_connection) {
            return *active_connection;
        }
        throw std::runtime_error("Failed to acquire connection from pool");
    }
    throw std::runtime_error("Session has no valid connection");
}

void Session::ReleaseConnection() {
    if (active_connection) {
        LOG_DEBUG("session", "Session " + std::to_string(session_id) + " releasing connection back to pool");
        active_connection.Release();
    }
}

bool Session::HasActiveConnection() const {
    return static_cast<bool>(active_connection) || static_cast<bool>(owned_connection);
}

bool Session::HasConnection() const {
    return static_cast<bool>(active_connection) || static_cast<bool>(owned_connection) || (pool != nullptr);
}

void Session::AddPreparedStatement(const std::string& name,
                                   std::unique_ptr<duckdb::PreparedStatement> stmt) {
    std::lock_guard<std::mutex> lock(prepared_mutex);
    prepared_statements[name] = std::move(stmt);
}

duckdb::PreparedStatement* Session::GetPreparedStatement(const std::string& name) {
    std::lock_guard<std::mutex> lock(prepared_mutex);
    auto it = prepared_statements.find(name);
    if (it != prepared_statements.end()) {
        return it->second.get();
    }
    return nullptr;
}

bool Session::RemovePreparedStatement(const std::string& name) {
    std::lock_guard<std::mutex> lock(prepared_mutex);
    return prepared_statements.erase(name) > 0;
}

void Session::ClearPreparedStatements() {
    std::lock_guard<std::mutex> lock(prepared_mutex);
    prepared_statements.clear();
}

void Session::InterruptQuery() {
    cancel_requested = true;
    if (active_connection) {
        active_connection->Interrupt();
    } else if (owned_connection) {
        owned_connection->Interrupt();
    }
}

bool Session::IsExpired(std::chrono::minutes timeout) const {
    auto now = Clock::now();
    return (now - last_active) > timeout;
}

} // namespace duckdb_server
