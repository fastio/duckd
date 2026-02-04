//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// network/tcp_server.cpp
//
// TCP server implementation
//===----------------------------------------------------------------------===//

#include "network/tcp_server.hpp"
#include "network/pg_connection.hpp"
#include "config/server_config.hpp"
#include "protocol/protocol_handler.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "executor/query_executor.hpp"
#include "serialization/arrow_serializer.hpp"
#include "logging/logger.hpp"

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
    
    std::string protocol_name = (config_.protocol == ProtocolType::PostgreSQL) ? "PostgreSQL" : "Native";
    LOG_INFO("server", "DuckD Server started on " + config_.host + ":" +
             std::to_string(config_.port) + " (" + protocol_name + " protocol)");
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

    if (config_.protocol == ProtocolType::PostgreSQL) {
        // PostgreSQL protocol - create socket and accept
        auto socket = std::make_shared<asio::ip::tcp::socket>(io_context);

        acceptor_.async_accept(*socket,
            [this, socket](const asio::error_code& ec) {
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
                    socket->close();
                } else {
                    // Create session for this connection
                    auto session = session_manager_->CreateSession();
                    if (session) {
                        auto pg_conn = PgConnection::Create(std::move(*socket), this, session);
                        total_connections_++;
                        pg_conn->Start();
                    } else {
                        LOG_WARN("server", "Failed to create session");
                        socket->close();
                    }
                }

                // Accept next connection
                DoAccept();
            });
    } else {
        // Native protocol
        auto arrow_serializer = std::make_shared<ArrowSerializer>();
        auto query_executor = std::make_shared<QueryExecutor>(arrow_serializer);

        auto handler = std::make_shared<ProtocolHandler>(
            session_manager_,
            executor_pool_,
            query_executor
        );

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
                    AddConnection(conn);
                    conn->Start();
                }

                // Accept next connection
                DoAccept();
            });
    }
}

} // namespace duckdb_server
