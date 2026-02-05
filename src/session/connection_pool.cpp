//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// session/connection_pool.cpp
//
// DuckDB connection pool implementation
//===----------------------------------------------------------------------===//

#include "session/connection_pool.hpp"
#include "logging/logger.hpp"

namespace duckdb_server {

//===----------------------------------------------------------------------===//
// PooledConnection
//===----------------------------------------------------------------------===//

PooledConnection::PooledConnection(ConnectionPool* pool, duckdb::Connection* conn)
    : pool_(pool), connection_(conn) {}

PooledConnection::~PooledConnection() {
    Release();
}

PooledConnection::PooledConnection(PooledConnection&& other) noexcept
    : pool_(other.pool_), connection_(other.connection_) {
    other.pool_ = nullptr;
    other.connection_ = nullptr;
}

PooledConnection& PooledConnection::operator=(PooledConnection&& other) noexcept {
    if (this != &other) {
        Release();
        pool_ = other.pool_;
        connection_ = other.connection_;
        other.pool_ = nullptr;
        other.connection_ = nullptr;
    }
    return *this;
}

void PooledConnection::Release() {
    if (pool_ && connection_) {
        pool_->Release(connection_);
        pool_ = nullptr;
        connection_ = nullptr;
    }
}

//===----------------------------------------------------------------------===//
// ConnectionPool
//===----------------------------------------------------------------------===//

ConnectionPool::ConnectionPool(duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance,
                               const Config& config)
    : db_instance_(std::move(db_instance))
    , config_(config) {

    // Create minimum connections
    EnsureMinConnections();

    LOG_INFO("conn_pool", "Connection pool created with " +
             std::to_string(available_.size()) + " connections " +
             "(min=" + std::to_string(config_.min_connections) +
             ", max=" + std::to_string(config_.max_connections) + ")");

    // Start maintenance thread
    maintenance_thread_ = std::thread(&ConnectionPool::MaintenanceLoop, this);
}

ConnectionPool::~ConnectionPool() {
    Shutdown();
}

void ConnectionPool::Shutdown() {
    // Stop maintenance thread
    running_ = false;
    available_cv_.notify_all();

    if (maintenance_thread_.joinable()) {
        maintenance_thread_.join();
    }

    // Clear all connections
    std::lock_guard<std::mutex> lock(mutex_);

    size_t total = available_.size() + in_use_.size();

    while (!available_.empty()) {
        available_.pop();
        total_destroyed_++;
    }

    // Note: in_use_ connections will be destroyed when PooledConnection objects
    // are destroyed, but we clear our tracking here
    in_use_.clear();

    LOG_INFO("conn_pool", "Connection pool shutdown, " +
             std::to_string(total) + " connections released");
}

PooledConnection ConnectionPool::Acquire() {
    return Acquire(config_.acquire_timeout);
}

PooledConnection ConnectionPool::Acquire(std::chrono::milliseconds timeout) {
    acquire_count_++;
    auto deadline = Clock::now() + timeout;

    std::unique_lock<std::mutex> lock(mutex_);

    while (running_) {
        // Try to get from available pool
        while (!available_.empty()) {
            auto entry = std::move(available_.front());
            available_.pop();

            // Validate if configured
            if (config_.validate_on_acquire) {
                if (!ValidateConnection(entry->connection.get())) {
                    validation_failure_count_++;
                    total_destroyed_++;
                    LOG_DEBUG("conn_pool", "Discarded invalid connection during acquire");
                    continue;
                }
            }

            // Update metadata and move to in_use
            entry->last_used = Clock::now();
            entry->use_count++;

            duckdb::Connection* raw_ptr = entry->connection.get();
            in_use_[raw_ptr] = std::move(entry);

            LOG_DEBUG("conn_pool", "Acquired connection (available=" +
                     std::to_string(available_.size()) +
                     ", in_use=" + std::to_string(in_use_.size()) + ")");

            return PooledConnection(this, raw_ptr);
        }

        // No available connections, try to create new one if under max
        size_t current_size = available_.size() + in_use_.size();
        if (current_size < config_.max_connections) {
            auto entry = CreateConnection();
            if (entry) {
                entry->use_count = 1;
                duckdb::Connection* raw_ptr = entry->connection.get();
                in_use_[raw_ptr] = std::move(entry);

                LOG_DEBUG("conn_pool", "Created new connection (total=" +
                         std::to_string(in_use_.size() + available_.size()) + ")");

                return PooledConnection(this, raw_ptr);
            }
        }

        // Wait for a connection to become available
        auto now = Clock::now();
        if (now >= deadline) {
            acquire_timeout_count_++;
            LOG_WARN("conn_pool", "Connection acquire timeout after " +
                    std::to_string(timeout.count()) + "ms");
            return PooledConnection();
        }

        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        available_cv_.wait_for(lock, wait_time);
    }

    // Pool is shutting down
    return PooledConnection();
}

void ConnectionPool::Release(duckdb::Connection* conn) {
    if (!conn) return;

    std::lock_guard<std::mutex> lock(mutex_);

    auto it = in_use_.find(conn);
    if (it == in_use_.end()) {
        LOG_WARN("conn_pool", "Releasing unknown connection");
        return;
    }

    auto entry = std::move(it->second);
    in_use_.erase(it);

    if (!running_) {
        // Pool is shutting down, destroy connection
        total_destroyed_++;
        return;
    }

    // Update last used time
    entry->last_used = Clock::now();

    // Return to available pool
    available_.push(std::move(entry));

    LOG_DEBUG("conn_pool", "Released connection (available=" +
              std::to_string(available_.size()) +
              ", in_use=" + std::to_string(in_use_.size()) + ")");

    // Notify waiters
    available_cv_.notify_one();
}

std::unique_ptr<ConnectionPool::PoolEntry> ConnectionPool::CreateConnection() {
    try {
        auto conn = std::make_unique<duckdb::Connection>(*db_instance_);
        total_created_++;
        return std::make_unique<PoolEntry>(std::move(conn));
    } catch (const std::exception& e) {
        LOG_ERROR("conn_pool", "Failed to create connection: " + std::string(e.what()));
        return nullptr;
    }
}

bool ConnectionPool::ValidateConnection(duckdb::Connection* conn) {
    try {
        // Simple validation: execute a trivial query
        auto result = conn->Query("SELECT 1");
        return !result->HasError();
    } catch (...) {
        return false;
    }
}

void ConnectionPool::MaintenanceLoop() {
    while (running_) {
        // Sleep for 30 seconds between maintenance runs
        for (int i = 0; i < 30 && running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        if (!running_) break;

        std::lock_guard<std::mutex> lock(mutex_);

        auto now = Clock::now();
        size_t removed = 0;

        // Remove idle connections that exceed minimum pool size
        while (available_.size() > config_.min_connections) {
            auto& entry = available_.front();
            auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
                now - entry->last_used);

            if (idle_time >= config_.idle_timeout) {
                available_.pop();
                total_destroyed_++;
                removed++;
            } else {
                // Queue is roughly FIFO, so if front is not idle, others won't be either
                break;
            }
        }

        if (removed > 0) {
            LOG_DEBUG("conn_pool", "Removed " + std::to_string(removed) +
                     " idle connections (available=" + std::to_string(available_.size()) + ")");
        }

        // Ensure minimum connections (outside of lock would be better, but simpler this way)
        while (available_.size() + in_use_.size() < config_.min_connections) {
            auto entry = CreateConnection();
            if (entry) {
                available_.push(std::move(entry));
            } else {
                break;
            }
        }
    }
}

void ConnectionPool::EnsureMinConnections() {
    std::lock_guard<std::mutex> lock(mutex_);

    while (available_.size() < config_.min_connections) {
        auto entry = CreateConnection();
        if (entry) {
            available_.push(std::move(entry));
        } else {
            LOG_WARN("conn_pool", "Failed to create minimum connections, only " +
                    std::to_string(available_.size()) + " created");
            break;
        }
    }
}

ConnectionPool::Stats ConnectionPool::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);

    Stats stats;
    stats.total_created = total_created_.load();
    stats.total_destroyed = total_destroyed_.load();
    stats.current_size = available_.size() + in_use_.size();
    stats.available = available_.size();
    stats.in_use = in_use_.size();
    stats.acquire_count = acquire_count_.load();
    stats.acquire_timeout_count = acquire_timeout_count_.load();
    stats.validation_failure_count = validation_failure_count_.load();

    return stats;
}

void ConnectionPool::SetMinConnections(size_t min_conn) {
    config_.min_connections = min_conn;
    EnsureMinConnections();
}

void ConnectionPool::SetMaxConnections(size_t max_conn) {
    config_.max_connections = max_conn;
}

} // namespace duckdb_server
