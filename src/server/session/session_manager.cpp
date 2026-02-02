//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// session/session_manager.cpp
//
// Session manager implementation
//===----------------------------------------------------------------------===//

#include "duckdb_server/session/session_manager.hpp"
#include "duckdb_server/utils/logger.hpp"

namespace duckdb_server {

SessionManager::SessionManager(std::shared_ptr<duckdb::DuckDB> db,
                               size_t max_sessions,
                               std::chrono::minutes session_timeout)
    : db_(db)
    , max_sessions_(max_sessions)
    , session_timeout_(session_timeout)
    , next_session_id_(1)
    , total_sessions_created_(0)
    , cleanup_running_(false) {
    
    // Start cleanup thread
    StartCleanupTimer();
}

SessionManager::~SessionManager() {
    cleanup_running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    // Clear all sessions
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    sessions_.clear();
}

SessionPtr SessionManager::CreateSession() {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    // Check max sessions
    if (sessions_.size() >= max_sessions_) {
        LOG_WARN("session_manager", "Maximum sessions reached: " + 
                 std::to_string(max_sessions_));
        return nullptr;
    }
    
    // Generate session ID
    uint64_t session_id = NextSessionId();
    
    // Create session
    auto session = std::make_shared<Session>(session_id, db_->instance);
    
    // Store
    sessions_[session_id] = session;
    total_sessions_created_++;
    
    LOG_DEBUG("session_manager", "Created session " + std::to_string(session_id) + 
              " (total: " + std::to_string(sessions_.size()) + ")");
    
    return session;
}

SessionPtr SessionManager::GetSession(uint64_t session_id) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        return it->second;
    }
    
    return nullptr;
}

bool SessionManager::RemoveSession(uint64_t session_id) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) {
        sessions_.erase(it);
        LOG_DEBUG("session_manager", "Removed session " + std::to_string(session_id) + 
                  " (total: " + std::to_string(sessions_.size()) + ")");
        return true;
    }
    
    return false;
}

size_t SessionManager::CleanupExpiredSessions() {
    std::vector<uint64_t> expired;
    
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        
        for (const auto& [id, session] : sessions_) {
            if (session->IsExpired(session_timeout_)) {
                expired.push_back(id);
            }
        }
        
        for (uint64_t id : expired) {
            sessions_.erase(id);
        }
    }
    
    if (!expired.empty()) {
        LOG_INFO("session_manager", "Cleaned up " + std::to_string(expired.size()) + 
                 " expired sessions");
    }
    
    return expired.size();
}

size_t SessionManager::GetActiveSessionCount() const {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    return sessions_.size();
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
