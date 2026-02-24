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

PooledConnection::PooledConnection(ConnectionPool* pool_p, duckdb::Connection* conn)
    : pool(pool_p), connection(conn) {}

PooledConnection::~PooledConnection() {
    Release();
}

PooledConnection::PooledConnection(PooledConnection&& other) noexcept
    : pool(other.pool), connection(other.connection) {
    other.pool = nullptr;
    other.connection = nullptr;
}

PooledConnection& PooledConnection::operator=(PooledConnection&& other) noexcept {
    if (this != &other) {
        Release();
        pool = other.pool;
        connection = other.connection;
        other.pool = nullptr;
        other.connection = nullptr;
    }
    return *this;
}

void PooledConnection::Release() {
    if (pool && connection) {
        pool->Release(connection);
        pool = nullptr;
        connection = nullptr;
    }
}

//===----------------------------------------------------------------------===//
// ConnectionPool
//===----------------------------------------------------------------------===//

ConnectionPool::ConnectionPool(duckdb::shared_ptr<duckdb::DatabaseInstance> db_instance_p,
                               const Config& config_p)
    : db_instance(std::move(db_instance_p))
    , config(config_p) {

    // Create minimum connections
    EnsureMinConnections();

    LOG_INFO("conn_pool", "Connection pool created with " +
             std::to_string(available.size()) + " connections " +
             "(min=" + std::to_string(config.min_connections) +
             ", max=" + std::to_string(config.max_connections) + ")");

    // Start maintenance thread
    maintenance_thread = std::thread(&ConnectionPool::MaintenanceLoop, this);
}

ConnectionPool::~ConnectionPool() {
    Shutdown();
}

void ConnectionPool::Shutdown() {
    // Stop maintenance thread
    running = false;
    available_cv.notify_all();
    maintenance_cv.notify_all();

    if (maintenance_thread.joinable()) {
        maintenance_thread.join();
    }

    // Clear all connections
    std::lock_guard<std::mutex> lock(mutex);

    size_t total = available.size() + in_use.size();

    total_destroyed += available.size();
    available.clear();

    // Note: in_use connections will be destroyed when PooledConnection objects
    // are destroyed, but we clear our tracking here
    in_use.clear();

    LOG_INFO("conn_pool", "Connection pool shutdown, " +
             std::to_string(total) + " connections released");
}

PooledConnection ConnectionPool::Acquire() {
    return Acquire(config.acquire_timeout);
}

PooledConnection ConnectionPool::Acquire(std::chrono::milliseconds timeout) {
    acquire_count++;
    auto deadline = Clock::now() + timeout;

    std::unique_lock<std::mutex> lock(mutex);

    while (running) {
        // Try to get from available pool
        while (!available.empty()) {
            auto entry = std::move(available.back());
            available.pop_back();

            // Validate if configured
            if (config.validate_on_acquire) {
                if (!ValidateConnection(entry->connection.get())) {
                    validation_failure_count++;
                    total_destroyed++;
                    LOG_DEBUG("conn_pool", "Discarded invalid connection during acquire");
                    continue;
                }
            }

            // Update metadata and move to in_use
            entry->last_used = Clock::now();
            entry->use_count++;

            duckdb::Connection* raw_ptr = entry->connection.get();
            in_use[raw_ptr] = std::move(entry);

            LOG_DEBUG("conn_pool", "Acquired connection (available=" +
                     std::to_string(available.size()) +
                     ", in_use=" + std::to_string(in_use.size()) + ")");

            return PooledConnection(this, raw_ptr);
        }

        // No available connections, try to create new one if under max
        size_t current_size = available.size() + in_use.size();
        if (current_size < config.max_connections) {
            auto entry = CreateConnection();
            if (entry) {
                entry->use_count = 1;
                duckdb::Connection* raw_ptr = entry->connection.get();
                in_use[raw_ptr] = std::move(entry);

                LOG_DEBUG("conn_pool", "Created new connection (total=" +
                         std::to_string(in_use.size() + available.size()) + ")");

                return PooledConnection(this, raw_ptr);
            }
        }

        // Wait for a connection to become available
        auto now = Clock::now();
        if (now >= deadline) {
            acquire_timeout_count++;
            LOG_WARN("conn_pool", "Connection acquire timeout after " +
                    std::to_string(timeout.count()) + "ms");
            return PooledConnection();
        }

        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        available_cv.wait_for(lock, wait_time);
    }

    // Pool is shutting down
    return PooledConnection();
}

void ConnectionPool::Release(duckdb::Connection* conn) {
    if (!conn) return;

    std::lock_guard<std::mutex> lock(mutex);

    auto it = in_use.find(conn);
    if (it == in_use.end()) {
        LOG_WARN("conn_pool", "Releasing unknown connection");
        return;
    }

    auto entry = std::move(it->second);
    in_use.erase(it);

    if (!running) {
        // Pool is shutting down, destroy connection
        total_destroyed++;
        return;
    }

    // Update last used time
    entry->last_used = Clock::now();

    // Return to available pool (LIFO: push_back, Acquire pops from back)
    available.push_back(std::move(entry));

    LOG_DEBUG("conn_pool", "Released connection (available=" +
              std::to_string(available.size()) +
              ", in_use=" + std::to_string(in_use.size()) + ")");

    // Notify waiters
    available_cv.notify_one();
}

std::unique_ptr<ConnectionPool::PoolEntry> ConnectionPool::CreateConnection() {
    try {
        auto conn = std::make_unique<duckdb::Connection>(*db_instance);
        total_created++;
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
    while (running) {
        // Wait up to 30 seconds, wake immediately on shutdown
        {
            std::unique_lock<std::mutex> lock(maintenance_mutex);
            maintenance_cv.wait_for(lock, std::chrono::seconds(30),
                                    [this] { return !running.load(); });
        }

        if (!running) break;

        // Phase 1: under lock, move idle entries out and determine how many
        // new connections are needed. Do NOT destroy or create connections
        // while holding the lock — duckdb::Connection ctor/dtor may be slow.
        std::vector<std::unique_ptr<PoolEntry>> to_destroy;
        size_t need_create = 0;
        size_t removed = 0;
        {
            std::lock_guard<std::mutex> lock(mutex);

            auto now = Clock::now();
            size_t remove_count = 0;
            for (size_t i = 0; i < available.size() &&
                               available.size() - remove_count > config.min_connections; i++) {
                auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
                    now - available[i]->last_used);
                if (idle_time >= config.idle_timeout) {
                    remove_count++;
                } else {
                    break;
                }
            }

            if (remove_count > 0) {
                to_destroy.reserve(remove_count);
                for (size_t i = 0; i < remove_count; i++) {
                    to_destroy.push_back(std::move(available[i]));
                }
                available.erase(available.begin(), available.begin() + remove_count);
                total_destroyed += remove_count;
                removed = remove_count;
            }

            size_t total = available.size() + in_use.size();
            if (total < config.min_connections) {
                need_create = config.min_connections - total;
            }
        }

        // Phase 2: destroy idle connections outside the lock
        to_destroy.clear();

        if (removed > 0) {
            LOG_DEBUG("conn_pool", "Removed " + std::to_string(removed) +
                     " idle connections");
        }

        // Phase 3: create new connections outside the lock, then batch-insert
        if (need_create > 0) {
            std::vector<std::unique_ptr<PoolEntry>> new_entries;
            new_entries.reserve(need_create);
            for (size_t i = 0; i < need_create; i++) {
                auto entry = CreateConnection();
                if (entry) {
                    new_entries.push_back(std::move(entry));
                } else {
                    break;
                }
            }
            if (!new_entries.empty()) {
                std::lock_guard<std::mutex> lock(mutex);
                for (auto& e : new_entries) {
                    available.push_back(std::move(e));
                }
            }
        }
    }
}

void ConnectionPool::EnsureMinConnections() {
    std::lock_guard<std::mutex> lock(mutex);

    while (available.size() < config.min_connections) {
        auto entry = CreateConnection();
        if (entry) {
            available.push_back(std::move(entry));
        } else {
            LOG_WARN("conn_pool", "Failed to create minimum connections, only " +
                    std::to_string(available.size()) + " created");
            break;
        }
    }
}

ConnectionPool::Stats ConnectionPool::GetStats() const {
    std::lock_guard<std::mutex> lock(mutex);

    Stats stats;
    stats.total_created = total_created.load();
    stats.total_destroyed = total_destroyed.load();
    stats.current_size = available.size() + in_use.size();
    stats.available = available.size();
    stats.in_use = in_use.size();
    stats.acquire_count = acquire_count.load();
    stats.acquire_timeout_count = acquire_timeout_count.load();
    stats.validation_failure_count = validation_failure_count.load();

    return stats;
}

void ConnectionPool::SetMinConnections(size_t min_conn) {
    config.min_connections = min_conn;
    EnsureMinConnections();
}

void ConnectionPool::SetMaxConnections(size_t max_conn) {
    config.max_connections = max_conn;
}

} // namespace duckdb_server
