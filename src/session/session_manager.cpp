//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/session_manager.cpp
//
// Session manager implementation with connection pooling
//===----------------------------------------------------------------------===//

#include "session/session_manager.hpp"
#include "logging/logger.hpp"
#include "logging/duckdb_log_storage.hpp"

namespace duckdb_server {

namespace {

// Register DuckDB logging with spdlog
void SetupDuckDBLogging(duckdb::DatabaseInstance& db) {
    if (duckd::RegisterSpdlogStorage(db, duckd::Logger::Get())) {
        LOG_DEBUG("session_manager", "DuckDB logging integrated with spdlog");
    } else {
        LOG_WARN("session_manager", "Failed to integrate DuckDB logging with spdlog");
    }
}

} // anonymous namespace

SessionManager::SessionManager(std::shared_ptr<duckdb::DuckDB> db,
                               const Config& config)
    : db_(db)
    , config_(config)
    , next_session_id_(1)
    , total_sessions_created_(0)
    , cleanup_running_(false) {

    // Create connection pool
    ConnectionPool::Config pool_config;
    pool_config.min_connections = config.pool_min_connections;
    pool_config.max_connections = config.pool_max_connections;
    pool_config.idle_timeout = config.pool_idle_timeout;
    pool_config.acquire_timeout = config.pool_acquire_timeout;
    pool_config.validate_on_acquire = config.pool_validate_on_acquire;

    connection_pool_ = std::make_unique<ConnectionPool>(db_->instance, pool_config);

    // Setup DuckDB logging integration
    SetupDuckDBLogging(*db_->instance);

    // Start cleanup thread
    StartCleanupTimer();

    LOG_INFO("session_manager", "Session manager initialized (max_sessions=" +
             std::to_string(config_.max_sessions) + ")");
}

// Legacy constructor
SessionManager::SessionManager(std::shared_ptr<duckdb::DuckDB> db,
                               size_t max_sessions,
                               std::chrono::minutes session_timeout)
    : db_(db)
    , next_session_id_(1)
    , total_sessions_created_(0)
    , cleanup_running_(false) {

    config_.max_sessions = max_sessions;
    config_.session_timeout = session_timeout;

    // Create connection pool with defaults based on max_sessions
    ConnectionPool::Config pool_config;
    pool_config.min_connections = std::min(size_t(5), max_sessions / 4 + 1);
    pool_config.max_connections = max_sessions;

    connection_pool_ = std::make_unique<ConnectionPool>(db_->instance, pool_config);

    // Setup DuckDB logging integration
    SetupDuckDBLogging(*db_->instance);

    // Start cleanup thread
    StartCleanupTimer();

    LOG_INFO("session_manager", "Session manager initialized (legacy mode, max_sessions=" +
             std::to_string(max_sessions) + ")");
}

SessionManager::~SessionManager() {
    cleanup_running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }

    // Clear all sessions (will return connections to pool)
    sessions_.clear();

    // Connection pool will be destroyed after sessions
    LOG_INFO("session_manager", "Session manager shutdown");
}

SessionPtr SessionManager::CreateSession() {
    // Check max sessions (approximate check for performance)
    if (sessions_.size() >= config_.max_sessions) {
        LOG_WARN("session_manager", "Maximum sessions reached: " +
                 std::to_string(config_.max_sessions));
        return nullptr;
    }

    // Generate session ID
    uint64_t session_id = NextSessionId();

    // Create session with pool pointer (lazy connection acquisition)
    auto session = std::make_shared<Session>(session_id, connection_pool_.get());

    // Store using thread-safe insertion
    sessions_.insert({session_id, session});
    total_sessions_created_++;

    LOG_DEBUG("session_manager", "Created session " + std::to_string(session_id) +
              " (total: " + std::to_string(sessions_.size()) + ")");

    return session;
}

SessionPtr SessionManager::GetSession(uint64_t session_id) {
    SessionPtr result = nullptr;

    // Thread-safe lookup using if_contains
    sessions_.if_contains(session_id, [&result](const auto& item) {
        result = item.second;
    });

    return result;
}

bool SessionManager::RemoveSession(uint64_t session_id) {
    size_t erased = sessions_.erase(session_id);

    if (erased > 0) {
        LOG_DEBUG("session_manager", "Removed session " + std::to_string(session_id) +
                  " (total: " + std::to_string(sessions_.size()) + ")");
        return true;
    }

    return false;
}

size_t SessionManager::CleanupExpiredSessions() {
    std::vector<uint64_t> expired;

    // First pass: collect expired session IDs
    // Using for_each for thread-safe iteration
    sessions_.for_each([this, &expired](const auto& item) {
        if (item.second->IsExpired(config_.session_timeout)) {
            expired.push_back(item.first);
        }
    });

    // Second pass: remove expired sessions
    for (uint64_t id : expired) {
        sessions_.erase(id);
    }

    if (!expired.empty()) {
        LOG_INFO("session_manager", "Cleaned up " + std::to_string(expired.size()) +
                 " expired sessions");
    }

    return expired.size();
}

size_t SessionManager::GetActiveSessionCount() const {
    return sessions_.size();
}

ConnectionPool::Stats SessionManager::GetPoolStats() const {
    return connection_pool_->GetStats();
}

uint64_t SessionManager::NextSessionId() {
    return next_session_id_.fetch_add(1);
}

void SessionManager::StartCleanupTimer() {
    cleanup_running_ = true;

    cleanup_thread_ = std::thread([this]() {
        while (cleanup_running_) {
            // Sleep for 1 minute
            for (int i = 0; i < 60 && cleanup_running_; ++i) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }

            if (cleanup_running_) {
                CleanupExpiredSessions();
            }
        }
    });
}

} // namespace duckdb_server
