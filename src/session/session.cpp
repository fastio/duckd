//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session.cpp
//
// Session implementation - sticky session model
//===----------------------------------------------------------------------===//

#include "session/session.hpp"
#include "logging/logger.hpp"

namespace duckdb_server {

Session::Session(uint64_t session_id_p, duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p)
    : session_id(session_id_p)
    , created_at(Clock::now())
    , last_active(Clock::now())
    , db_instance(std::move(db_instance_p))
    , current_database("main")
    , current_schema("main")
    , current_query_id(0)
    , cancel_requested(false) {
}

Session::~Session() {
    LOG_DEBUG("session", "Session " + std::to_string(session_id) + " destroyed");
}

duckdb::Connection& Session::GetConnection() {
    if (!connection) {
        connection = std::make_unique<duckdb::Connection>(*db_instance);
    }
    return *connection;
}

void Session::InterruptQuery() {
    cancel_requested = true;
    if (connection) {
        connection->Interrupt();
    }
}

bool Session::IsExpired(std::chrono::minutes timeout) const {
    auto now = Clock::now();
    return (now - last_active) > timeout;
}

} // namespace duckdb_server
