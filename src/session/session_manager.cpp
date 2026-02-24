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

SessionManager::SessionManager(std::shared_ptr<duckdb::DuckDB> db_p,
                               const Config& config_p,
                               std::chrono::milliseconds query_timeout_p)
    : db(db_p)
    , config(config_p)
    , query_timeout(query_timeout_p)
    , next_session_id(1)
    , total_sessions_created(0)
    , cleanup_running(false) {

    // Create connection pool
    ConnectionPool::Config pool_config;
    pool_config.min_connections = config_p.pool_min_connections;
    pool_config.max_connections = config_p.pool_max_connections;
    pool_config.idle_timeout = config_p.pool_idle_timeout;
    pool_config.acquire_timeout = config_p.pool_acquire_timeout;
    pool_config.validate_on_acquire = config_p.pool_validate_on_acquire;

    connection_pool = std::make_unique<ConnectionPool>(db->instance, pool_config);

    // Setup DuckDB logging integration
    SetupDuckDBLogging(*db->instance);

    // Start cleanup thread
    StartCleanupTimer();

    LOG_INFO("session_manager", "Session manager initialized (max_sessions=" +
             std::to_string(config.max_sessions) + ")");
}

// Legacy constructor
SessionManager::SessionManager(std::shared_ptr<duckdb::DuckDB> db_p,
                               size_t max_sessions,
                               std::chrono::minutes session_timeout)
    : db(db_p)
    , next_session_id(1)
    , total_sessions_created(0)
    , cleanup_running(false) {

    config.max_sessions = max_sessions;
    config.session_timeout = session_timeout;

    // Create connection pool with defaults based on max_sessions
    ConnectionPool::Config pool_config;
    pool_config.min_connections = std::min(size_t(5), max_sessions / 4 + 1);
    pool_config.max_connections = max_sessions;

    connection_pool = std::make_unique<ConnectionPool>(db->instance, pool_config);

    // Setup DuckDB logging integration
    SetupDuckDBLogging(*db->instance);

    // Start cleanup thread
    StartCleanupTimer();

    LOG_INFO("session_manager", "Session manager initialized (legacy mode, max_sessions=" +
             std::to_string(max_sessions) + ")");
}

SessionManager::~SessionManager() {
    cleanup_running = false;
    cleanup_cv.notify_all();
    if (cleanup_thread.joinable()) {
        cleanup_thread.join();
    }

    // Clear all sessions (will return connections to pool)
    sessions.clear();

    // Connection pool will be destroyed after sessions
    LOG_INFO("session_manager", "Session manager shutdown");
}

SessionPtr SessionManager::CreateSession() {
    // Check max sessions (approximate check for performance)
    if (sessions.size() >= config.max_sessions) {
        LOG_WARN("session_manager", "Maximum sessions reached: " +
                 std::to_string(config.max_sessions));
        return nullptr;
    }

    // Generate session ID
    uint64_t session_id = NextSessionId();

    // Create session with pool pointer (lazy connection acquisition)
    auto session = std::make_shared<Session>(session_id, connection_pool.get());

    // Store using thread-safe insertion
    sessions.insert({session_id, session});
    total_sessions_created++;

    LOG_DEBUG("session_manager", "Created session " + std::to_string(session_id) +
              " (total: " + std::to_string(sessions.size()) + ")");

    return session;
}

SessionPtr SessionManager::GetSession(uint64_t session_id) {
    SessionPtr result = nullptr;

    // Thread-safe lookup using if_contains
    sessions.if_contains(session_id, [&result](const auto& item) {
        result = item.second;
    });

    return result;
}

bool SessionManager::RemoveSession(uint64_t session_id) {
    size_t erased = sessions.erase(session_id);

    if (erased > 0) {
        LOG_DEBUG("session_manager", "Removed session " + std::to_string(session_id) +
                  " (total: " + std::to_string(sessions.size()) + ")");
        return true;
    }

    return false;
}

size_t SessionManager::CleanupExpiredSessions() {
    std::vector<uint64_t> expired;

    // First pass: collect expired session IDs
    // Using for_each for thread-safe iteration
    sessions.for_each([this, &expired](const auto& item) {
        if (item.second->IsExpired(config.session_timeout)) {
            expired.push_back(item.first);
        }
    });

    // Second pass: remove expired sessions
    for (uint64_t id : expired) {
        sessions.erase(id);
    }

    if (!expired.empty()) {
        LOG_INFO("session_manager", "Cleaned up " + std::to_string(expired.size()) +
                 " expired sessions");
    }

    return expired.size();
}

bool SessionManager::CancelQuery(int32_t process_id, int32_t secret_key) {
    bool cancelled = false;
    sessions.for_each([&](const auto& item) {
        auto& session = item.second;
        if (session->GetBackendProcessId() == process_id &&
            session->GetBackendSecretKey() == secret_key) {
            LOG_INFO("session_manager", "Cancelling query for session " +
                     std::to_string(item.first));
            session->InterruptQuery();
            cancelled = true;
        }
    });
    return cancelled;
}

size_t SessionManager::GetActiveSessionCount() const {
    return sessions.size();
}

ConnectionPool::Stats SessionManager::GetPoolStats() const {
    return connection_pool->GetStats();
}

uint64_t SessionManager::NextSessionId() {
    return next_session_id.fetch_add(1);
}

void SessionManager::StartCleanupTimer() {
    cleanup_running = true;

    cleanup_thread = std::thread([this]() {
        while (cleanup_running) {
            // Wait up to 5 seconds, wake immediately on shutdown
            {
                std::unique_lock<std::mutex> lock(cleanup_mutex);
                cleanup_cv.wait_for(lock, std::chrono::seconds(5),
                                    [this] { return !cleanup_running.load(); });
            }

            if (!cleanup_running) break;

            // Check for query timeouts
            if (query_timeout.count() > 0) {
                auto now = Clock::now();
                sessions.for_each([this, &now](const auto& item) {
                    auto& session = item.second;
                    if (session->IsQueryRunning()) {
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - session->GetQueryStartTime());
                        if (elapsed > query_timeout) {
                            LOG_WARN("session_manager", "Query timeout for session " +
                                     std::to_string(item.first) + " after " +
                                     std::to_string(elapsed.count()) + "ms");
                            session->InterruptQuery();
                        }
                    }
                });
            }

            // Session cleanup less frequently (every 12 iterations = ~60 seconds)
            if (++cleanup_counter >= 12) {
                cleanup_counter = 0;
                CleanupExpiredSessions();
            }
        }
    });
}

} // namespace duckdb_server
