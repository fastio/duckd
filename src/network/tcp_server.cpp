//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/tcp_server.cpp
//
// TCP server implementation
//===----------------------------------------------------------------------===//

#include "network/tcp_server.hpp"
#include "config/server_config.hpp"
#include "protocol/protocol_handler.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "executor/query_executor.hpp"
#include "serialization/arrow_serializer.hpp"
#include "utils/logger.hpp"

namespace duckdb_server {

TcpServer::TcpServer(const ServerConfig& config,
                     std::shared_ptr<SessionManager> session_manager,
                     std::shared_ptr<ExecutorPool> executor_pool)
    : config_(config)
    , io_pool_(config.GetIoThreadCount())
    , acceptor_(acceptor_io_context_)
    , session_manager_(session_manager)
    , executor_pool_(executor_pool)
    , running_(false)
    , total_connections_(0)
    , total_bytes_received_(0)
    , total_bytes_sent_(0) {
}

TcpServer::~TcpServer() {
    Stop();
}

void TcpServer::Start() {
    if (running_) {
        return;
    }
    
    // Set up acceptor
    asio::ip::tcp::endpoint endpoint(
        asio::ip::make_address(config_.host),
        config_.port
    );
    
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor_.bind(endpoint);
    acceptor_.listen(asio::socket_base::max_listen_connections);
    
    running_ = true;
    
    // Start IO pool
    io_pool_.Start();
    
    // Start accepting
    DoAccept();
    
    // Run acceptor in its own thread
    acceptor_thread_ = std::thread([this]() {
        acceptor_io_context_.run();
    });
    
    LOG_INFO("server", "DuckDB Server started on " + config_.host + ":" + 
             std::to_string(config_.port));
}

void TcpServer::Stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    // Stop accepting
    asio::error_code ec;
    acceptor_.close(ec);
    acceptor_io_context_.stop();
    
    if (acceptor_thread_.joinable()) {
        acceptor_thread_.join();
    }
    
    // Close all connections
    {
        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (auto& conn : connections_) {
            conn->Close();
        }
        connections_.clear();
    }
    
    // Stop IO pool
    io_pool_.Stop();
    
    LOG_INFO("server", "DuckDB Server stopped");
}

void TcpServer::AddConnection(TcpConnectionPtr conn) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.insert(conn);
    total_connections_++;
}

void TcpServer::RemoveConnection(TcpConnectionPtr conn) {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    connections_.erase(conn);
}

size_t TcpServer::GetConnectionCount() const {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    return connections_.size();
}

void TcpServer::DoAccept() {
    if (!running_) {
        return;
    }
    
    // Get next io_context for this connection
    asio::io_context& io_context = io_pool_.GetNextIoContext();
    
    // Create query executor and arrow serializer
    auto arrow_serializer = std::make_shared<ArrowSerializer>();
    auto query_executor = std::make_shared<QueryExecutor>(arrow_serializer);
    
    // Create protocol handler
    auto handler = std::make_shared<ProtocolHandler>(
        session_manager_,
        executor_pool_,
        query_executor
    );
    
    // Create connection
    auto conn = std::make_shared<TcpConnection>(io_context, *this, handler);
    
    acceptor_.async_accept(conn->GetSocket(),
        [this, conn](const asio::error_code& ec) {
            if (ec) {
                if (running_) {
                    LOG_WARN("server", "Accept error: " + ec.message());
                    DoAccept();
                }
                return;
            }
            
            // Check max connections
            if (GetConnectionCount() >= config_.max_connections) {
                LOG_WARN("server", "Max connections reached, rejecting new connection");
                conn->Close();
            } else {
                // Add and start connection
                AddConnection(conn);
                conn->Start();
            }
            
            // Accept next connection
            DoAccept();
        });
}

} // namespace duckdb_server
