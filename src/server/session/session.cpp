//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// session/session.cpp
//
// Session implementation
//===----------------------------------------------------------------------===//

#include "duckdb_server/session/session.hpp"
#include "duckdb_server/utils/logger.hpp"

namespace duckdb_server {

Session::Session(uint64_t session_id, duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance)
    : session_id_(session_id)
    , created_at_(Clock::now())
    , last_active_(Clock::now())
    , connection_(std::make_unique<duckdb::Connection>(*db_instance))
    , current_database_("main")
    , current_schema_("main")
    , current_query_id_(0)
    , cancel_requested_(false) {
}

Session::~Session() {
    ClearPreparedStatements();
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
