//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/tcp_server.hpp
//
// TCP server implementation
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "network/io_context_pool.hpp"
#include "network/tcp_connection.hpp"
#include <asio.hpp>
#include <set>

namespace duckdb_server {

class ServerConfig;
class SessionManager;
class ExecutorPool;

class TcpServer {
public:
    TcpServer(const ServerConfig& config,
              std::shared_ptr<SessionManager> session_manager,
              std::shared_ptr<ExecutorPool> executor_pool);
    ~TcpServer();
    
    // Non-copyable
    TcpServer(const TcpServer&) = delete;
    TcpServer& operator=(const TcpServer&) = delete;
    
    // Start the server
    void Start();
    
    // Stop the server
    void Stop();
    
    // Check if running
    bool IsRunning() const { return running_; }
    
    // Get session manager
    std::shared_ptr<SessionManager> GetSessionManager() { return session_manager_; }
    
    // Get executor pool
    std::shared_ptr<ExecutorPool> GetExecutorPool() { return executor_pool_; }
    
    // Connection management
    void AddConnection(TcpConnectionPtr conn);
    void RemoveConnection(TcpConnectionPtr conn);
    size_t GetConnectionCount() const;
    
    // Get config
    const ServerConfig& GetConfig() const { return config_; }
    
    // Statistics
    uint64_t GetTotalConnections() const { return total_connections_; }
    uint64_t GetTotalBytesReceived() const { return total_bytes_received_; }
    uint64_t GetTotalBytesSent() const { return total_bytes_sent_; }
    
    void AddBytesReceived(uint64_t bytes) { total_bytes_received_ += bytes; }
    void AddBytesSent(uint64_t bytes) { total_bytes_sent_ += bytes; }

private:
    // Start accepting connections
    void DoAccept();

private:
    // Configuration
    const ServerConfig& config_;
    
    // IO context pool
    IoContextPool io_pool_;
    
    // Acceptor IO context (separate from worker pool)
    asio::io_context acceptor_io_context_;
    asio::ip::tcp::acceptor acceptor_;
    
    // Session manager
    std::shared_ptr<SessionManager> session_manager_;
    
    // Executor pool
    std::shared_ptr<ExecutorPool> executor_pool_;
    
    // Active connections
    std::set<TcpConnectionPtr> connections_;
    mutable std::mutex connections_mutex_;
    
    // Running flag
    std::atomic<bool> running_;
    
    // Acceptor thread
    std::thread acceptor_thread_;
    
    // Statistics
    std::atomic<uint64_t> total_connections_;
    std::atomic<uint64_t> total_bytes_received_;
    std::atomic<uint64_t> total_bytes_sent_;
};

} // namespace duckdb_server
