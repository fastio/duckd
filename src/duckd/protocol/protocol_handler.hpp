//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// protocol/protocol_handler.hpp
//
// Protocol message handling
//===----------------------------------------------------------------------===//

#pragma once

#include "common.hpp"
#include "protocol/message.hpp"
#include "session/session.hpp"

namespace duckdb_server {

class TcpConnection;
class SessionManager;
class ExecutorPool;
class QueryExecutor;
class ArrowSerializer;

class ProtocolHandler : public std::enable_shared_from_this<ProtocolHandler> {
public:
    ProtocolHandler(std::shared_ptr<SessionManager> session_manager,
                   std::shared_ptr<ExecutorPool> executor_pool,
                   std::shared_ptr<QueryExecutor> query_executor);
    ~ProtocolHandler() = default;
    
    // Handle incoming message
    void HandleMessage(const Message& message, 
                      std::shared_ptr<TcpConnection> connection);
    
    // Send response
    void SendResponse(MessageType type,
                     const std::vector<uint8_t>& payload,
                     std::shared_ptr<TcpConnection> connection);

private:
    // Message handlers
    void HandleHello(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandlePing(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandleClose(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandleQuery(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandleQueryCancel(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandlePrepare(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandleExecute(const Message& message, std::shared_ptr<TcpConnection> connection);
    void HandleDeallocate(const Message& message, std::shared_ptr<TcpConnection> connection);
    
    // Get session for connection
    SessionPtr GetSessionForConnection(std::shared_ptr<TcpConnection> connection);
    
    // Send error response
    void SendError(ErrorCode code, 
                  const std::string& message,
                  std::shared_ptr<TcpConnection> connection);

private:
    std::shared_ptr<SessionManager> session_manager_;
    std::shared_ptr<ExecutorPool> executor_pool_;
    std::shared_ptr<QueryExecutor> query_executor_;
};

} // namespace duckdb_server
