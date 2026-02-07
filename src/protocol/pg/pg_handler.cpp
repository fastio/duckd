//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_handler.cpp
//
// PostgreSQL protocol handler implementation
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_handler.hpp"
#include "session/session.hpp"
#include "executor/executor_pool.hpp"
#include "logging/logger.hpp"
#include <algorithm>
#include <cctype>
#include <cstring>
#include <random>
#include <unistd.h>

namespace duckdb_server {
namespace pg {

PgHandler::PgHandler(std::shared_ptr<Session> session, SendCallback send_callback,
                     std::shared_ptr<ExecutorPool> executor_pool, ResumeCallback resume_callback)
    : session_(std::move(session))
    , send_callback_(std::move(send_callback))
    , executor_pool_(std::move(executor_pool))
    , resume_callback_(std::move(resume_callback))
    , process_id_(getpid())
    , secret_key_(0) {
    // Generate random secret key
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist;
    secret_key_ = dist(gen);
}

PgHandler::~PgHandler() = default;

ProcessResult PgHandler::ProcessData(const uint8_t* data, size_t len) {
    // Compact buffer if offset has consumed more than half
    if (buffer_offset_ > 0 && buffer_offset_ >= buffer_.size() / 2) {
        buffer_.erase(buffer_.begin(), buffer_.begin() + buffer_offset_);
        buffer_offset_ = 0;
    }
    buffer_.insert(buffer_.end(), data, data + len);
    return ProcessBufferLoop();
}

ProcessResult PgHandler::ProcessPendingBuffer() {
    async_pending_ = false;
    return ProcessBufferLoop();
}

ProcessResult PgHandler::ProcessBufferLoop() {
    while (buffer_offset_ < buffer_.size()) {
        // If an async operation is pending, stop processing
        if (async_pending_) {
            return ProcessResult::AsyncPending;
        }

        const uint8_t* buf = buffer_.data() + buffer_offset_;
        size_t remaining = buffer_.size() - buffer_offset_;

        if (state_ == PgConnectionState::Initial) {
            // Startup message: length(4) + content
            if (remaining < 4) return ProcessResult::Continue;

            int32_t msg_len;
            std::memcpy(&msg_len, buf, 4);
            msg_len = NetworkToHost32(msg_len);

            if (msg_len < 4 || static_cast<size_t>(msg_len) > remaining) {
                return ProcessResult::Continue;  // Need more data
            }

            bool result = HandleStartup(buf, msg_len);
            buffer_offset_ += msg_len;
            if (!result) return ProcessResult::Close;
        }
        else if (state_ == PgConnectionState::Authenticating) {
            // Password message: type(1) + length(4) + content
            if (remaining < 5) return ProcessResult::Continue;

            int32_t msg_len;
            std::memcpy(&msg_len, buf + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > remaining) {
                return ProcessResult::Continue;  // Need more data
            }

            bool result = HandleAuthentication(buf + 5, msg_len - 4);
            buffer_offset_ += msg_len + 1;
            if (!result) return ProcessResult::Close;
        }
        else {
            // Regular message: type(1) + length(4) + content
            if (remaining < 5) return ProcessResult::Continue;

            char type = static_cast<char>(buf[0]);
            int32_t msg_len;
            std::memcpy(&msg_len, buf + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > remaining) {
                return ProcessResult::Continue;  // Need more data
            }

            bool result = HandleMessage(type, buf + 5, msg_len - 4);
            buffer_offset_ += msg_len + 1;
            if (!result) return ProcessResult::Close;

            // Check if HandleMessage triggered an async operation
            if (async_pending_) {
                return ProcessResult::AsyncPending;
            }
        }
    }

    // All data consumed, reset buffer
    buffer_.clear();
    buffer_offset_ = 0;

    return ProcessResult::Continue;
}

bool PgHandler::CanReleaseConnection() const {
    return !in_transaction_ && prepared_statements_.empty();
}

// Zero-allocation case-insensitive prefix match (skips leading whitespace)
static bool SqlStartsWithCI(const char* sql, size_t len, const char* keyword) {
    size_t i = 0;
    while (i < len && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r')) i++;
    size_t klen = strlen(keyword);
    if (len - i < klen) return false;
    return strncasecmp(sql + i, keyword, klen) == 0;
}

// Search for keyword within the first max_scan non-whitespace chars after position start
static bool SqlContainsCI(const char* sql, size_t len, size_t start, const char* keyword, size_t max_scan) {
    size_t klen = strlen(keyword);
    size_t end = std::min(len, start + max_scan);
    if (klen == 0 || end < klen) return false;
    for (size_t i = start; i + klen <= end; i++) {
        if (strncasecmp(sql + i, keyword, klen) == 0) return true;
    }
    return false;
}

void PgHandler::UpdateTransactionState(const std::string& sql) {
    const char* s = sql.data();
    size_t len = sql.size();

    if (SqlStartsWithCI(s, len, "BEGIN")) {
        in_transaction_ = true;
        transaction_failed_ = false;
    } else if (SqlStartsWithCI(s, len, "COMMIT") ||
               SqlStartsWithCI(s, len, "ROLLBACK")) {
        in_transaction_ = false;
        transaction_failed_ = false;
    }
}

void PgHandler::RevalidatePreparedStatements(duckdb::Connection& conn) {
    for (auto& [name, info] : prepared_statements_) {
        try {
            auto prepared = conn.Prepare(info.query);
            if (!prepared->HasError()) {
                info.statement = std::move(prepared);
                info.column_names.clear();
                info.column_types.clear();
                for (idx_t i = 0; i < info.statement->ColumnCount(); i++) {
                    info.column_names.push_back(info.statement->GetNames()[i]);
                    info.column_types.push_back(info.statement->GetTypes()[i]);
                }
            } else {
                LOG_WARN("pg", "Failed to revalidate prepared statement '" + name + "': " + prepared->GetError());
            }
        } catch (const std::exception& e) {
            LOG_WARN("pg", "Exception revalidating prepared statement '" + name + "': " + e.what());
        }
    }
    last_connection_ = &conn;
}

bool PgHandler::HandleStartup(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    StartupMessage msg;

    if (!reader.ReadStartupMessage(msg)) {
        SendError("FATAL", "08P01", "Invalid startup message");
        return false;
    }

    // Handle SSL request
    if (msg.protocol_version == SSL_REQUEST_CODE) {
        // Send 'N' to indicate SSL not supported
        std::vector<uint8_t> response = {'N'};
        send_callback_(response);
        return true;  // Wait for real startup message
    }

    // Handle cancel request
    if (msg.protocol_version == CANCEL_REQUEST_CODE) {
        // TODO: Implement query cancellation
        return false;  // Close connection
    }

    // Check protocol version
    if (msg.protocol_version != PROTOCOL_VERSION_3_0) {
        SendError("FATAL", "08004", "Unsupported protocol version");
        return false;
    }

    username_ = msg.GetUser();
    database_ = msg.GetDatabase();

    if (username_.empty()) {
        SendError("FATAL", "28000", "No username provided");
        return false;
    }

    LOG_INFO("pg", "Connection from user: " + username_ + ", database: " + database_);

    // For now, use trust authentication (no password)
    // TODO: Implement proper authentication
    state_ = PgConnectionState::Ready;
    SendStartupResponse();

    return true;
}

bool PgHandler::HandleAuthentication(const uint8_t* data, size_t len) {
    // Handle password message
    std::string password(reinterpret_cast<const char*>(data), len - 1);  // Remove null terminator

    // TODO: Verify password
    // For now, accept any password
    state_ = PgConnectionState::Ready;
    SendStartupResponse();

    return true;
}

void PgHandler::SendStartupResponse() {
    writer_.Clear();

    // Authentication OK
    writer_.WriteAuthenticationOk();

    // Send parameter status messages
    writer_.WriteParameterStatus("server_version", "15.0 (DuckDB)");
    writer_.WriteParameterStatus("server_encoding", "UTF8");
    writer_.WriteParameterStatus("client_encoding", "UTF8");
    writer_.WriteParameterStatus("DateStyle", "ISO, MDY");
    writer_.WriteParameterStatus("TimeZone", "UTC");
    writer_.WriteParameterStatus("integer_datetimes", "on");
    writer_.WriteParameterStatus("standard_conforming_strings", "on");

    // Backend key data
    writer_.WriteBackendKeyData(process_id_, secret_key_);

    // Ready for query
    writer_.WriteReadyForQuery(GetTransactionStatus());

    send_callback_(writer_.GetBuffer());
}

bool PgHandler::HandleMessage(char type, const uint8_t* data, size_t len) {
    switch (type) {
        case FrontendMessage::Query:
            HandleQuery(data, len);
            break;
        case FrontendMessage::Parse:
            HandleParse(data, len);
            break;
        case FrontendMessage::Bind:
            HandleBind(data, len);
            break;
        case FrontendMessage::Describe:
            HandleDescribe(data, len);
            break;
        case FrontendMessage::Execute:
            HandleExecute(data, len);
            break;
        case FrontendMessage::Close:
            HandleClose(data, len);
            break;
        case FrontendMessage::Sync:
            HandleSync(data, len);
            break;
        case FrontendMessage::Flush:
            HandleFlush(data, len);
            break;
        case FrontendMessage::Terminate:
            HandleTerminate(data, len);
            return false;
        default:
            LOG_WARN("pg", "Unknown message type: " + std::string(1, type));
            SendError("ERROR", "0A000", "Unsupported message type");
            SendReadyForQuery();
            break;
    }
    return state_ != PgConnectionState::Closed;
}

void PgHandler::HandleQuery(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    QueryMessage msg;
    reader.ReadQueryMessage(msg);

    LOG_DEBUG("pg", "Query: " + msg.query);

    // Handle empty query
    std::string sql = std::move(msg.query);
    // Trim whitespace
    sql.erase(0, sql.find_first_not_of(" \t\n\r"));
    sql.erase(sql.find_last_not_of(" \t\n\r") + 1);

    if (sql.empty()) {
        writer_.Clear();
        writer_.WriteEmptyQueryResponse();
        writer_.WriteReadyForQuery(GetTransactionStatus());
        send_callback_(writer_.GetBuffer());
        return;
    }

    // Dispatch to executor pool
    async_pending_ = true;
    auto session = session_;
    auto send_cb = send_callback_;
    auto resume_cb = resume_callback_;

    executor_pool_->Submit([this, sql = std::move(sql), session, send_cb, resume_cb]() {
        PgMessageWriter writer;

        try {
            auto result = session->GetConnection().Query(sql);

            if (result->HasError()) {
                writer.WriteErrorResponse("ERROR", "42000", result->GetError());
                writer.WriteReadyForQuery(GetTransactionStatus());
                send_cb(writer.GetBuffer());

                // Update transaction state on IO thread via resume
                UpdateTransactionState(sql);
                if (in_transaction_) {
                    transaction_failed_ = true;
                }

                // Try to release connection if possible
                if (CanReleaseConnection() && session->HasActiveConnection()) {
                    session->ReleaseConnection();
                }

                resume_cb();
                return;
            }

            // Update transaction state
            UpdateTransactionState(sql);

            // Write result
            uint64_t row_count = 0;
            WriteQueryResult(writer, *result, sql, sql, row_count);

            // ReadyForQuery
            writer.WriteReadyForQuery(GetTransactionStatus());
            send_cb(writer.GetBuffer());

            // Try to release connection if possible
            if (CanReleaseConnection() && session->HasActiveConnection()) {
                session->ReleaseConnection();
            }

        } catch (const std::exception& e) {
            if (in_transaction_) {
                transaction_failed_ = true;
            }
            writer.WriteErrorResponse("ERROR", "XX000", e.what());
            writer.WriteReadyForQuery(GetTransactionStatus());
            send_cb(writer.GetBuffer());

            if (CanReleaseConnection() && session->HasActiveConnection()) {
                session->ReleaseConnection();
            }
        }

        resume_cb();
    });
}

void PgHandler::WriteQueryResult(PgMessageWriter& writer, duckdb::QueryResult& result,
                                  const std::string& command, const std::string& sql,
                                  uint64_t& rows_affected) {
    // Get column info
    std::vector<std::string> names;
    std::vector<duckdb::LogicalType> types;

    idx_t col_count = result.ColumnCount();
    for (idx_t i = 0; i < col_count; i++) {
        names.push_back(result.ColumnName(i));
        types.push_back(result.types[i]);
    }

    // Send RowDescription if there are columns
    if (!names.empty()) {
        writer.WriteRowDescription(names, types);
    }

    // Send data rows using column-vector direct path
    rows_affected = 0;
    while (true) {
        auto chunk = result.Fetch();
        if (!chunk || chunk->size() == 0) break;

        auto unified = chunk->ToUnifiedFormat();
        for (idx_t row = 0; row < chunk->size(); row++) {
            writer.WriteDataRowDirect(unified, types, row, col_count, chunk.get());
            rows_affected++;
        }
    }

    // Send CommandComplete
    std::string tag = GetCommandTag(sql, rows_affected);
    writer.WriteCommandComplete(tag);
}

void PgHandler::HandleParse(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    ParseMessage msg;

    if (!reader.ReadParseMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Parse message");
        return;
    }

    LOG_DEBUG("pg", "Parse: " + msg.statement_name + " = " + msg.query);

    // Dispatch Prepare to executor pool
    async_pending_ = true;
    auto session = session_;
    auto send_cb = send_callback_;
    auto resume_cb = resume_callback_;
    std::string stmt_name = msg.statement_name;
    std::string query = msg.query;

    executor_pool_->Submit([this, stmt_name, query, session, send_cb, resume_cb]() {
        try {
            auto& conn = session->GetConnection();
            auto prepared = conn.Prepare(query);

            if (prepared->HasError()) {
                PgMessageWriter writer;
                writer.WriteErrorResponse("ERROR", "42000", prepared->GetError());
                send_cb(writer.GetBuffer());
                resume_cb();
                return;
            }

            PreparedStatementInfo info;
            info.query = query;
            info.statement = std::move(prepared);

            // Get column info from statement
            for (idx_t i = 0; i < info.statement->ColumnCount(); i++) {
                info.column_names.push_back(info.statement->GetNames()[i]);
                info.column_types.push_back(info.statement->GetTypes()[i]);
            }

            prepared_statements_[stmt_name] = std::move(info);
            last_connection_ = &conn;

            PgMessageWriter writer;
            writer.WriteParseComplete();
            send_cb(writer.GetBuffer());

        } catch (const std::exception& e) {
            PgMessageWriter writer;
            writer.WriteErrorResponse("ERROR", "42000", e.what());
            send_cb(writer.GetBuffer());
        }

        resume_cb();
    });
}

void PgHandler::HandleBind(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    BindMessage msg;

    if (!reader.ReadBindMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Bind message");
        return;
    }

    LOG_DEBUG("pg", "Bind: portal=" + msg.portal_name + ", statement=" + msg.statement_name);

    auto it = prepared_statements_.find(msg.statement_name);
    if (it == prepared_statements_.end()) {
        SendError("ERROR", "26000", "Prepared statement not found: " + msg.statement_name);
        return;
    }

    PortalInfo portal;
    portal.statement_name = msg.statement_name;
    portal.result_formats = msg.result_formats;

    // Convert parameters to DuckDB values
    for (size_t i = 0; i < msg.param_values.size(); i++) {
        if (!msg.param_values[i].has_value()) {
            portal.parameters.push_back(duckdb::Value());  // NULL
        } else {
            // For now, treat all parameters as strings (text format)
            const auto& bytes = msg.param_values[i].value();
            std::string str(bytes.begin(), bytes.end());
            portal.parameters.push_back(duckdb::Value(str));
        }
    }

    portals_[msg.portal_name] = std::move(portal);

    writer_.Clear();
    writer_.WriteBindComplete();
    send_callback_(writer_.GetBuffer());
}

void PgHandler::HandleDescribe(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    DescribeMessage msg;

    if (!reader.ReadDescribeMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Describe message");
        return;
    }

    writer_.Clear();

    if (msg.describe_type == 'S') {
        // Describe statement
        auto it = prepared_statements_.find(msg.name);
        if (it == prepared_statements_.end()) {
            SendError("ERROR", "26000", "Prepared statement not found: " + msg.name);
            return;
        }

        // Send ParameterDescription
        std::vector<int32_t> param_oids;
        for (const auto& type : it->second.param_types) {
            param_oids.push_back(DuckDBTypeToOid(type));
        }
        writer_.WriteParameterDescription(param_oids);

        // Send RowDescription or NoData
        if (it->second.column_names.empty()) {
            writer_.WriteNoData();
        } else {
            writer_.WriteRowDescription(it->second.column_names, it->second.column_types);
        }
    } else {
        // Describe portal
        auto it = portals_.find(msg.name);
        if (it == portals_.end()) {
            SendError("ERROR", "34000", "Portal not found: " + msg.name);
            return;
        }

        auto stmt_it = prepared_statements_.find(it->second.statement_name);
        if (stmt_it == prepared_statements_.end()) {
            writer_.WriteNoData();
        } else if (stmt_it->second.column_names.empty()) {
            writer_.WriteNoData();
        } else {
            writer_.WriteRowDescription(stmt_it->second.column_names, stmt_it->second.column_types);
        }
    }

    send_callback_(writer_.GetBuffer());
}

void PgHandler::HandleExecute(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    ExecuteMessage msg;

    if (!reader.ReadExecuteMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Execute message");
        return;
    }

    LOG_DEBUG("pg", "Execute: portal=" + msg.portal_name);

    auto portal_it = portals_.find(msg.portal_name);
    if (portal_it == portals_.end()) {
        SendError("ERROR", "34000", "Portal not found: " + msg.portal_name);
        return;
    }

    auto& portal = portal_it->second;
    auto stmt_it = prepared_statements_.find(portal.statement_name);
    if (stmt_it == prepared_statements_.end()) {
        SendError("ERROR", "26000", "Prepared statement not found");
        return;
    }

    // Capture what we need for the async lambda
    auto params = portal.parameters;
    int32_t max_rows = msg.max_rows;
    std::string query = stmt_it->second.query;
    auto statement = stmt_it->second.statement;

    // Dispatch Execute to executor pool
    async_pending_ = true;
    auto session = session_;
    auto send_cb = send_callback_;
    auto resume_cb = resume_callback_;

    executor_pool_->Submit([this, params = std::move(params), max_rows, query,
                            statement, session, send_cb, resume_cb]() {
        try {
            // Check if connection changed and revalidate if needed
            auto& conn = session->GetConnection();
            if (last_connection_ != nullptr && last_connection_ != &conn) {
                RevalidatePreparedStatements(conn);
            }
            last_connection_ = &conn;

            // Find the (potentially revalidated) statement
            duckdb::shared_ptr<duckdb::PreparedStatement> exec_stmt;

            // Search by query text since name may vary
            for (auto& [name, info] : prepared_statements_) {
                if (info.query == query) {
                    exec_stmt = info.statement;
                    break;
                }
            }

            if (!exec_stmt) {
                exec_stmt = statement;  // Fall back to captured statement
            }

            // Execute the prepared statement with bound parameters
            std::unique_ptr<duckdb::QueryResult> result;
            if (params.empty()) {
                result = exec_stmt->Execute();
            } else {
                duckdb::vector<duckdb::Value> duckdb_params(params.begin(), params.end());
                auto pending = exec_stmt->PendingQuery(duckdb_params, true);
                result = pending->Execute();
            }

            if (result->HasError()) {
                PgMessageWriter writer;
                writer.WriteErrorResponse("ERROR", "42000", result->GetError());
                send_cb(writer.GetBuffer());
                resume_cb();
                return;
            }

            PgMessageWriter writer;

            // Collect types for direct formatting
            std::vector<duckdb::LogicalType> types;
            idx_t col_count = result->ColumnCount();
            types.reserve(col_count);
            for (idx_t i = 0; i < col_count; i++) {
                types.push_back(result->types[i]);
            }

            // Send data rows using column-vector direct path
            uint64_t row_count = 0;
            while (true) {
                auto chunk = result->Fetch();
                if (!chunk || chunk->size() == 0) break;

                auto unified = chunk->ToUnifiedFormat();
                for (idx_t row = 0; row < chunk->size(); row++) {
                    if (max_rows > 0 && row_count >= static_cast<uint64_t>(max_rows)) {
                        break;
                    }
                    writer.WriteDataRowDirect(unified, types, row, col_count, chunk.get());
                    row_count++;
                }
                if (max_rows > 0 && row_count >= static_cast<uint64_t>(max_rows)) {
                    break;
                }
            }

            std::string tag = GetCommandTag(query, row_count);
            writer.WriteCommandComplete(tag);

            send_cb(writer.GetBuffer());

        } catch (const std::exception& e) {
            PgMessageWriter writer;
            writer.WriteErrorResponse("ERROR", "XX000", e.what());
            send_cb(writer.GetBuffer());
        }

        resume_cb();
    });
}

void PgHandler::HandleClose(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    CloseMessage msg;

    if (!reader.ReadCloseMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Close message");
        return;
    }

    if (msg.close_type == 'S') {
        prepared_statements_.erase(msg.name);
    } else {
        portals_.erase(msg.name);
    }

    writer_.Clear();
    writer_.WriteCloseComplete();
    send_callback_(writer_.GetBuffer());

    // Try to release connection after deallocating a prepared statement
    if (msg.close_type == 'S' && CanReleaseConnection() && session_->HasActiveConnection()) {
        session_->ReleaseConnection();
    }
}

void PgHandler::HandleSync(const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    SendReadyForQuery();

    // Try to release connection after sync if possible
    if (CanReleaseConnection() && session_->HasActiveConnection()) {
        session_->ReleaseConnection();
    }
}

void PgHandler::HandleFlush(const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    // Flush is a no-op for us since we send immediately
}

void PgHandler::HandleTerminate(const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    LOG_INFO("pg", "Client disconnected");
    state_ = PgConnectionState::Closed;
}

void PgHandler::SendError(const std::string& severity, const std::string& code,
                          const std::string& message, const std::string& detail) {
    writer_.Clear();
    writer_.WriteErrorResponse(severity, code, message, detail);
    send_callback_(writer_.GetBuffer());

    if (severity == "FATAL") {
        state_ = PgConnectionState::Failed;
    } else if (in_transaction_) {
        transaction_failed_ = true;
    }
}

void PgHandler::SendReadyForQuery() {
    writer_.Clear();
    writer_.WriteReadyForQuery(GetTransactionStatus());
    send_callback_(writer_.GetBuffer());
}

char PgHandler::GetTransactionStatus() const {
    if (!in_transaction_) {
        return TransactionStatus::Idle;
    } else if (transaction_failed_) {
        return TransactionStatus::Failed;
    } else {
        return TransactionStatus::InTransaction;
    }
}

std::string PgHandler::GetCommandTag(const std::string& sql, uint64_t rows_affected) {
    const char* s = sql.data();
    size_t len = sql.size();

    if (SqlStartsWithCI(s, len, "SELECT") || SqlStartsWithCI(s, len, "WITH")) {
        return "SELECT " + std::to_string(rows_affected);
    } else if (SqlStartsWithCI(s, len, "INSERT")) {
        return "INSERT 0 " + std::to_string(rows_affected);
    } else if (SqlStartsWithCI(s, len, "UPDATE")) {
        return "UPDATE " + std::to_string(rows_affected);
    } else if (SqlStartsWithCI(s, len, "DELETE")) {
        return "DELETE " + std::to_string(rows_affected);
    } else if (SqlStartsWithCI(s, len, "CREATE")) {
        if (SqlContainsCI(s, len, 6, "TABLE", 25)) {
            return "CREATE TABLE";
        } else if (SqlContainsCI(s, len, 6, "INDEX", 25)) {
            return "CREATE INDEX";
        } else if (SqlContainsCI(s, len, 6, "VIEW", 25)) {
            return "CREATE VIEW";
        } else if (SqlContainsCI(s, len, 6, "SCHEMA", 25)) {
            return "CREATE SCHEMA";
        }
        return "CREATE";
    } else if (SqlStartsWithCI(s, len, "DROP")) {
        if (SqlContainsCI(s, len, 4, "TABLE", 20)) {
            return "DROP TABLE";
        }
        return "DROP";
    } else if (SqlStartsWithCI(s, len, "ALTER")) {
        return "ALTER TABLE";
    } else if (SqlStartsWithCI(s, len, "BEGIN")) {
        return "BEGIN";
    } else if (SqlStartsWithCI(s, len, "COMMIT")) {
        return "COMMIT";
    } else if (SqlStartsWithCI(s, len, "ROLLBACK")) {
        return "ROLLBACK";
    } else if (SqlStartsWithCI(s, len, "SET")) {
        return "SET";
    } else if (SqlStartsWithCI(s, len, "SHOW")) {
        return "SHOW";
    } else if (SqlStartsWithCI(s, len, "COPY")) {
        return "COPY " + std::to_string(rows_affected);
    }

    return "OK";
}

} // namespace pg
} // namespace duckdb_server
