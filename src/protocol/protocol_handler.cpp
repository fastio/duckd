//===----------------------------------------------------------------------===//
//                         DuckDB Server
//
// protocol/protocol_handler.cpp
//
// Protocol message handler implementation
//===----------------------------------------------------------------------===//

#include "protocol/protocol_handler.hpp"
#include "network/tcp_connection.hpp"
#include "session/session_manager.hpp"
#include "executor/executor_pool.hpp"
#include "executor/query_executor.hpp"
#include "logging/logger.hpp"

namespace duckdb_server {

ProtocolHandler::ProtocolHandler(std::shared_ptr<SessionManager> session_manager,
                                 std::shared_ptr<ExecutorPool> executor_pool,
                                 std::shared_ptr<QueryExecutor> query_executor)
    : session_manager_(session_manager)
    , executor_pool_(executor_pool)
    , query_executor_(query_executor) {
}

void ProtocolHandler::HandleMessage(const Message& message,
                                    std::shared_ptr<TcpConnection> connection) {
    switch (message.GetType()) {
        case MessageType::HELLO:
            HandleHello(message, connection);
            break;
        case MessageType::PING:
            HandlePing(message, connection);
            break;
        case MessageType::CLOSE:
            HandleClose(message, connection);
            break;
        case MessageType::QUERY:
            HandleQuery(message, connection);
            break;
        case MessageType::QUERY_CANCEL:
            HandleQueryCancel(message, connection);
            break;
        case MessageType::PREPARE:
            HandlePrepare(message, connection);
            break;
        case MessageType::EXECUTE:
            HandleExecute(message, connection);
            break;
        case MessageType::DEALLOCATE:
            HandleDeallocate(message, connection);
            break;
        default:
            LOG_WARN("protocol", "Unknown message type: " + 
                     std::to_string(static_cast<int>(message.GetType())));
            SendError(ErrorCode::PROTOCOL_ERROR, "Unknown message type", connection);
            break;
    }
}

void ProtocolHandler::SendResponse(MessageType type,
                                   const std::vector<uint8_t>& payload,
                                   std::shared_ptr<TcpConnection> connection) {
    Message response(type, payload);
    connection->Send(response);
}

void ProtocolHandler::HandleHello(const Message& message,
                                  std::shared_ptr<TcpConnection> connection) {
    try {
        // Parse hello payload
        HelloPayload hello = HelloPayload::Deserialize(message.GetPayload());
        
        LOG_DEBUG("protocol", "HELLO from " + hello.username + 
                  " (client_type=" + std::to_string(hello.client_type) + 
                  ", version=" + std::to_string(hello.client_version) + ")");
        
        // Check protocol version
        if (hello.protocol_version != PROTOCOL_VERSION) {
            SendError(ErrorCode::VERSION_MISMATCH, 
                     "Protocol version mismatch. Server version: " + 
                     std::to_string(PROTOCOL_VERSION), 
                     connection);
            return;
        }
        
        // Create session
        auto session = session_manager_->CreateSession();
        if (!session) {
            SendError(ErrorCode::MAX_CONNECTIONS, 
                     "Maximum sessions reached", 
                     connection);
            return;
        }
        
        // Store session info
        session->SetUsername(hello.username);
        session->SetClientInfo("type=" + std::to_string(hello.client_type) + 
                              ",version=" + std::to_string(hello.client_version));
        
        // Associate session with connection
        connection->SetSessionId(session->GetSessionId());
        
        // Build response
        HelloResponsePayload response;
        response.status = 0;  // Success
        response.session_id = session->GetSessionId();
        response.protocol_version = PROTOCOL_VERSION;
        response.server_capabilities = 0;  // TODO: Define capabilities
        response.server_info = "{\"name\":\"DuckDB Server\",\"version\":\"1.0.0\"}";
        
        SendResponse(MessageType::HELLO_RESPONSE, response.Serialize(), connection);
        
        LOG_INFO("protocol", "Session " + std::to_string(session->GetSessionId()) + 
                 " created for user " + hello.username);
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling HELLO: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

void ProtocolHandler::HandlePing(const Message& message,
                                 std::shared_ptr<TcpConnection> connection) {
    // Update session activity
    auto session = GetSessionForConnection(connection);
    if (session) {
        session->Touch();
    }
    
    // Send PONG
    Message pong(MessageType::PONG);
    connection->Send(pong);
}

void ProtocolHandler::HandleClose(const Message& message,
                                  std::shared_ptr<TcpConnection> connection) {
    uint64_t session_id = connection->GetSessionId();
    
    if (session_id != 0) {
        // Remove session
        session_manager_->RemoveSession(session_id);
        LOG_INFO("protocol", "Session " + std::to_string(session_id) + " closed");
    }
    
    // Close connection
    connection->Close();
}

void ProtocolHandler::HandleQuery(const Message& message,
                                  std::shared_ptr<TcpConnection> connection) {
    try {
        // Get session
        auto session = GetSessionForConnection(connection);
        if (!session) {
            SendError(ErrorCode::SESSION_NOT_FOUND, "Session not found", connection);
            return;
        }
        
        // Parse query payload
        QueryPayload query = QueryPayload::Deserialize(message.GetPayload());
        
        LOG_DEBUG("protocol", "QUERY (id=" + std::to_string(query.query_id) + 
                  ", session=" + std::to_string(session->GetSessionId()) + "): " + 
                  query.sql.substr(0, 100) + (query.sql.size() > 100 ? "..." : ""));
        
        // Update session state
        session->Touch();
        session->SetCurrentQueryId(query.query_id);
        session->ClearCancel();
        
        // Create query context
        QueryContext context;
        context.query_id = query.query_id;
        context.sql = query.sql;
        context.timeout_ms = query.timeout_ms;
        context.max_rows = query.max_rows;
        context.session = session;
        context.cancel_flag = nullptr;  // TODO: Add proper cancel flag
        
        // Capture connection for callback
        auto conn_ptr = connection;
        context.send_callback = [conn_ptr](const std::vector<uint8_t>& data) {
            if (conn_ptr && conn_ptr->IsConnected()) {
                conn_ptr->Send(data);
            }
        };
        
        // Submit to executor pool
        executor_pool_->Submit([this, context = std::move(context)]() mutable {
            query_executor_->Execute(std::move(context));
        });
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling QUERY: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

void ProtocolHandler::HandleQueryCancel(const Message& message,
                                        std::shared_ptr<TcpConnection> connection) {
    try {
        auto session = GetSessionForConnection(connection);
        if (!session) {
            SendError(ErrorCode::SESSION_NOT_FOUND, "Session not found", connection);
            return;
        }
        
        QueryCancelPayload cancel = QueryCancelPayload::Deserialize(message.GetPayload());
        
        LOG_DEBUG("protocol", "QUERY_CANCEL (id=" + std::to_string(cancel.query_id) + 
                  ", session=" + std::to_string(session->GetSessionId()) + ")");
        
        // Check if this is the current query
        if (session->GetCurrentQueryId() == cancel.query_id) {
            session->RequestCancel();
            // Also interrupt the DuckDB connection
            session->GetConnection().Interrupt();
        }
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling QUERY_CANCEL: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

void ProtocolHandler::HandlePrepare(const Message& message,
                                    std::shared_ptr<TcpConnection> connection) {
    try {
        auto session = GetSessionForConnection(connection);
        if (!session) {
            SendError(ErrorCode::SESSION_NOT_FOUND, "Session not found", connection);
            return;
        }
        
        PreparePayload prepare = PreparePayload::Deserialize(message.GetPayload());
        
        LOG_DEBUG("protocol", "PREPARE '" + prepare.statement_name + 
                  "' (session=" + std::to_string(session->GetSessionId()) + ")");
        
        session->Touch();
        
        // Prepare the statement
        auto stmt = session->GetConnection().Prepare(prepare.sql);
        
        if (stmt->HasError()) {
            SendError(ErrorCode::SYNTAX_ERROR, stmt->GetError(), connection);
            return;
        }
        
        // Store in session
        session->AddPreparedStatement(prepare.statement_name, std::move(stmt));
        
        // Send success response (could include parameter info)
        // For now, send empty PREPARE_RESPONSE
        Message response(MessageType::PREPARE_RESPONSE);
        connection->Send(response);
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling PREPARE: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

void ProtocolHandler::HandleExecute(const Message& message,
                                    std::shared_ptr<TcpConnection> connection) {
    try {
        auto session = GetSessionForConnection(connection);
        if (!session) {
            SendError(ErrorCode::SESSION_NOT_FOUND, "Session not found", connection);
            return;
        }
        
        ExecutePayload execute = ExecutePayload::Deserialize(message.GetPayload());
        
        LOG_DEBUG("protocol", "EXECUTE '" + execute.statement_name + 
                  "' (id=" + std::to_string(execute.query_id) + 
                  ", session=" + std::to_string(session->GetSessionId()) + ")");
        
        session->Touch();
        session->SetCurrentQueryId(execute.query_id);
        session->ClearCancel();
        
        // Get prepared statement
        auto* stmt = session->GetPreparedStatement(execute.statement_name);
        if (!stmt) {
            SendError(ErrorCode::INVALID_PARAMETER, 
                     "Prepared statement not found: " + execute.statement_name, 
                     connection);
            return;
        }
        
        // TODO: Convert parameters from Arrow format to DuckDB Values
        duckdb::vector<duckdb::Value> params;
        
        // Create query context
        QueryContext context;
        context.query_id = execute.query_id;
        context.sql = execute.statement_name;  // For logging
        context.timeout_ms = 0;
        context.max_rows = 0;
        context.session = session;
        context.cancel_flag = nullptr;
        
        auto conn_ptr = connection;
        context.send_callback = [conn_ptr](const std::vector<uint8_t>& data) {
            if (conn_ptr && conn_ptr->IsConnected()) {
                conn_ptr->Send(data);
            }
        };
        
        // Submit to executor pool
        executor_pool_->Submit([this, context = std::move(context), 
                                stmt_name = execute.statement_name,
                                params = std::move(params)]() mutable {
            query_executor_->ExecutePrepared(std::move(context), stmt_name, params);
        });
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling EXECUTE: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

void ProtocolHandler::HandleDeallocate(const Message& message,
                                       std::shared_ptr<TcpConnection> connection) {
    try {
        auto session = GetSessionForConnection(connection);
        if (!session) {
            SendError(ErrorCode::SESSION_NOT_FOUND, "Session not found", connection);
            return;
        }
        
        // Parse statement name (simple length-prefixed string)
        const auto& payload = message.GetPayload();
        if (payload.size() < 4) {
            SendError(ErrorCode::PROTOCOL_ERROR, "Invalid DEALLOCATE payload", connection);
            return;
        }
        
        uint32_t name_len = payload[0] | (payload[1] << 8) | 
                           (payload[2] << 16) | (payload[3] << 24);
        
        if (payload.size() < 4 + name_len) {
            SendError(ErrorCode::PROTOCOL_ERROR, "Invalid DEALLOCATE payload", connection);
            return;
        }
        
        std::string stmt_name(payload.begin() + 4, payload.begin() + 4 + name_len);
        
        LOG_DEBUG("protocol", "DEALLOCATE '" + stmt_name + 
                  "' (session=" + std::to_string(session->GetSessionId()) + ")");
        
        session->RemovePreparedStatement(stmt_name);
        
        // No response needed for DEALLOCATE
        
    } catch (const std::exception& e) {
        LOG_ERROR("protocol", "Error handling DEALLOCATE: " + std::string(e.what()));
        SendError(ErrorCode::PROTOCOL_ERROR, e.what(), connection);
    }
}

SessionPtr ProtocolHandler::GetSessionForConnection(std::shared_ptr<TcpConnection> connection) {
    uint64_t session_id = connection->GetSessionId();
    if (session_id == 0) {
        return nullptr;
    }
    return session_manager_->GetSession(session_id);
}

void ProtocolHandler::SendError(ErrorCode code,
                                const std::string& message,
                                std::shared_ptr<TcpConnection> connection) {
    QueryErrorPayload error(code, message);
    SendResponse(MessageType::QUERY_ERROR, error.Serialize(), connection);
}

} // namespace duckdb_server
