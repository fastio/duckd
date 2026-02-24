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

namespace duckdb_server {
namespace pg {

PgHandler::PgHandler(std::shared_ptr<Session> session_p, SendCallback send_callback_p,
                     std::shared_ptr<ExecutorPool> executor_pool_p, ResumeCallback resume_callback_p,
                     CancelCallback cancel_callback_p)
    : session(std::move(session_p))
    , send_callback(std::move(send_callback_p))
    , executor_pool(std::move(executor_pool_p))
    , resume_callback(std::move(resume_callback_p))
    , cancel_callback(std::move(cancel_callback_p))
    , process_id(static_cast<int32_t>(session->GetSessionId()))
    , secret_key(0) {
    // Generate random secret key
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist;
    secret_key = dist(gen);
}

PgHandler::~PgHandler() = default;

ProcessResult PgHandler::ProcessData(const uint8_t* data, size_t len) {
    // Compact buffer if offset exceeds threshold
    if (buffer_offset >= 4096) {
        size_t remaining = buffer.size() - buffer_offset;
        if (remaining > 0) {
            std::memmove(buffer.data(), buffer.data() + buffer_offset, remaining);
        }
        buffer.resize(remaining);
        buffer_offset = 0;
    }
    buffer.insert(buffer.end(), data, data + len);
    return ProcessBufferLoop();
}

ProcessResult PgHandler::ProcessPendingBuffer() {
    async_pending = false;
    return ProcessBufferLoop();
}

ProcessResult PgHandler::ProcessBufferLoop() {
    while (buffer_offset < buffer.size()) {
        // If an async operation is pending, stop processing
        if (async_pending) {
            return ProcessResult::ASYNC_PENDING;
        }

        const uint8_t* buf = buffer.data() + buffer_offset;
        size_t remaining = buffer.size() - buffer_offset;

        if (state == PgConnectionState::INITIAL) {
            // Startup message: length(4) + content
            if (remaining < 4) return ProcessResult::CONTINUE;

            int32_t msg_len;
            std::memcpy(&msg_len, buf, 4);
            msg_len = NetworkToHost32(msg_len);

            if (msg_len < 4 || static_cast<size_t>(msg_len) > remaining) {
                return ProcessResult::CONTINUE;  // Need more data
            }

            bool result = HandleStartup(buf, msg_len);
            buffer_offset += msg_len;
            if (!result) return ProcessResult::CLOSE;
        }
        else if (state == PgConnectionState::AUTHENTICATING) {
            // Password message: type(1) + length(4) + content
            if (remaining < 5) return ProcessResult::CONTINUE;

            int32_t msg_len;
            std::memcpy(&msg_len, buf + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > remaining) {
                return ProcessResult::CONTINUE;  // Need more data
            }

            bool result = HandleAuthentication(buf + 5, msg_len - 4);
            buffer_offset += msg_len + 1;
            if (!result) return ProcessResult::CLOSE;
        }
        else {
            // Regular message: type(1) + length(4) + content
            if (remaining < 5) return ProcessResult::CONTINUE;

            char type = static_cast<char>(buf[0]);
            int32_t msg_len;
            std::memcpy(&msg_len, buf + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > remaining) {
                return ProcessResult::CONTINUE;  // Need more data
            }

            bool result = HandleMessage(type, buf + 5, msg_len - 4);
            buffer_offset += msg_len + 1;
            if (!result) return ProcessResult::CLOSE;

            // Check if HandleMessage triggered an async operation
            if (async_pending) {
                return ProcessResult::ASYNC_PENDING;
            }
        }
    }

    // All data consumed, reset buffer
    buffer.clear();
    buffer_offset = 0;

    return ProcessResult::CONTINUE;
}

bool PgHandler::CanReleaseConnection() const {
    return !in_transaction && prepared_statements.empty();
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

void PgHandler::RevalidatePreparedStatements(duckdb::Connection& conn) {
    for (auto& [name, info] : prepared_statements) {
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
    last_connection = &conn;
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
        send_callback(std::move(response));
        return true;  // Wait for real startup message
    }

    // Handle cancel request
    if (msg.protocol_version == CANCEL_REQUEST_CODE) {
        if (cancel_callback) {
            cancel_callback(msg.cancel_pid, msg.cancel_secret_key);
        }
        return false;  // Cancel connections always close after sending
    }

    // Check protocol version
    if (msg.protocol_version != PROTOCOL_VERSION_3_0) {
        SendError("FATAL", "08004", "Unsupported protocol version");
        return false;
    }

    username = msg.GetUser();
    database = msg.GetDatabase();

    if (username.empty()) {
        SendError("FATAL", "28000", "No username provided");
        return false;
    }

    LOG_INFO("pg", "Connection from user: " + username + ", database: " + database);

    // For now, use trust authentication (no password)
    // TODO: Implement proper authentication
    state = PgConnectionState::READY;
    SendStartupResponse();

    return true;
}

bool PgHandler::HandleAuthentication(const uint8_t* data, size_t len) {
    // Handle password message
    std::string password(reinterpret_cast<const char*>(data), len - 1);  // Remove null terminator

    // TODO: Verify password
    // For now, accept any password
    state = PgConnectionState::READY;
    SendStartupResponse();

    return true;
}

void PgHandler::SendStartupResponse() {
    writer.Clear();

    // Authentication OK
    writer.WriteAuthenticationOk();

    // Send parameter status messages
    writer.WriteParameterStatus("server_version", "15.0 (DuckDB)");
    writer.WriteParameterStatus("server_encoding", "UTF8");
    writer.WriteParameterStatus("client_encoding", "UTF8");
    writer.WriteParameterStatus("DateStyle", "ISO, MDY");
    writer.WriteParameterStatus("TimeZone", "UTC");
    writer.WriteParameterStatus("integer_datetimes", "on");
    writer.WriteParameterStatus("standard_conforming_strings", "on");

    // Backend key data
    writer.WriteBackendKeyData(process_id, secret_key);
    session->SetBackendKeyData(process_id, secret_key);

    // Ready for query
    writer.WriteReadyForQuery(GetTransactionStatus());

    send_callback(writer.TakeBuffer());
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
    return state != PgConnectionState::CLOSED;
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
        writer.Clear();
        writer.WriteEmptyQueryResponse();
        writer.WriteReadyForQuery(GetTransactionStatus());
        send_callback(writer.TakeBuffer());
        return;
    }

    // Dispatch to executor pool
    async_pending = true;
    auto sess = session;
    auto send_cb = send_callback;
    auto resume_cb = resume_callback;

    executor_pool->Submit([this, sql = std::move(sql), sess, send_cb, resume_cb]() {
        PgMessageWriter writer;
        bool has_error = false;
        bool is_exception = false;
        sess->MarkQueryStart();

        try {
            auto result = sess->GetConnection().Query(sql);

            if (result->HasError()) {
                writer.WriteErrorResponse("ERROR", "42000", result->GetError());
                has_error = true;
            } else {
                // Write result rows
                uint64_t row_count = 0;
                WriteQueryResult(writer, *result, sql, sql, row_count, send_cb);
            }
        } catch (const std::exception& e) {
            writer.WriteErrorResponse("ERROR", "XX000", e.what());
            has_error = true;
            is_exception = true;
        }

        // Determine transaction state changes from SQL
        bool is_begin = SqlStartsWithCI(sql.data(), sql.size(), "BEGIN");
        bool is_end = SqlStartsWithCI(sql.data(), sql.size(), "COMMIT") ||
                      SqlStartsWithCI(sql.data(), sql.size(), "ROLLBACK");

        // Move buffer out before posting to IO thread
        auto response_buf = writer.TakeBuffer();
        sess->MarkQueryEnd();

        resume_cb([this, is_begin, is_end, has_error, is_exception,
                   buf = std::move(response_buf), send_cb, sess]() mutable {
            // Update transaction state on IO thread
            if (!is_exception) {
                if (is_begin) {
                    in_transaction = true;
                    transaction_failed = false;
                } else if (is_end) {
                    in_transaction = false;
                    transaction_failed = false;
                }
            }
            if (has_error && in_transaction) {
                transaction_failed = true;
            }

            // Append ReadyForQuery with correct transaction status
            this->writer.Clear();
            this->writer.WriteReadyForQuery(GetTransactionStatus());
            const auto& rfq = this->writer.GetBuffer();
            buf.insert(buf.end(), rfq.begin(), rfq.end());

            send_cb(std::move(buf));

            if (CanReleaseConnection() && sess->HasActiveConnection()) {
                sess->ReleaseConnection();
            }
        });
    });
}

void PgHandler::WriteQueryResult(PgMessageWriter& writer, duckdb::QueryResult& result,
                                  const std::string& command, const std::string& sql,
                                  uint64_t& rows_affected, const SendCallback& send_cb) {
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

    // Send data rows using column-vector direct path, flush per chunk
    rows_affected = 0;
    while (true) {
        auto chunk = result.Fetch();
        if (!chunk || chunk->size() == 0) break;

        auto unified = chunk->ToUnifiedFormat();
        for (idx_t row = 0; row < chunk->size(); row++) {
            writer.WriteDataRowDirect(unified, types, row, col_count, chunk.get());
            rows_affected++;
        }

        // Flush accumulated data after each chunk
        send_cb(writer.TakeBuffer());
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
    async_pending = true;
    auto sess = session;
    auto send_cb = send_callback;
    auto resume_cb = resume_callback;
    std::string stmt_name = msg.statement_name;
    std::string query = msg.query;

    executor_pool->Submit([this, stmt_name, query, sess, send_cb, resume_cb]() {
        PgMessageWriter writer;

        try {
            auto& conn = sess->GetConnection();
            auto prepared = conn.Prepare(query);

            if (prepared->HasError()) {
                writer.WriteErrorResponse("ERROR", "42000", prepared->GetError());
                resume_cb([this, send_cb, buf = writer.TakeBuffer()]() mutable {
                    if (in_transaction) {
                        transaction_failed = true;
                    }
                    send_cb(std::move(buf));
                });
                return;
            }

            PreparedStatementInfo info;
            info.query = query;
            info.statement = std::move(prepared);

            // Get parameter types from prepared statement
            auto expected_types = info.statement->GetExpectedParameterTypes();
            // Build ordered param_types vector from $1, $2, ...
            for (idx_t i = 1; i <= expected_types.size(); i++) {
                auto key = "$" + std::to_string(i);
                auto it = expected_types.find(key);
                if (it != expected_types.end()) {
                    info.param_types.push_back(it->second);
                } else {
                    info.param_types.push_back(duckdb::LogicalType::VARCHAR);
                }
            }

            // Get column info from statement
            for (idx_t i = 0; i < info.statement->ColumnCount(); i++) {
                info.column_names.push_back(info.statement->GetNames()[i]);
                info.column_types.push_back(info.statement->GetTypes()[i]);
            }

            duckdb::Connection* conn_ptr = &conn;
            writer.WriteParseComplete();

            resume_cb([this, stmt_name, info = std::move(info), conn_ptr, send_cb,
                       buf = writer.TakeBuffer()]() mutable {
                prepared_statements[stmt_name] = std::move(info);
                last_connection = conn_ptr;
                send_cb(std::move(buf));
            });

        } catch (const std::exception& e) {
            writer.WriteErrorResponse("ERROR", "42000", e.what());
            resume_cb([this, send_cb, buf = writer.TakeBuffer()]() mutable {
                if (in_transaction) {
                    transaction_failed = true;
                }
                send_cb(std::move(buf));
            });
        }
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

    auto it = prepared_statements.find(msg.statement_name);
    if (it == prepared_statements.end()) {
        SendError("ERROR", "26000", "Prepared statement not found: " + msg.statement_name);
        return;
    }

    PortalInfo portal;
    portal.statement_name = msg.statement_name;
    portal.result_formats = msg.result_formats;

    // Convert parameters to DuckDB values using expected types
    const auto& param_types = it->second.param_types;
    for (size_t i = 0; i < msg.param_values.size(); i++) {
        if (!msg.param_values[i].has_value()) {
            portal.parameters.push_back(duckdb::Value());  // NULL
        } else {
            const auto& bytes = msg.param_values[i].value();
            std::string str(bytes.begin(), bytes.end());

            // Cast to expected type if available
            if (i < param_types.size()) {
                try {
                    portal.parameters.push_back(duckdb::Value(str).DefaultCastAs(param_types[i]));
                } catch (...) {
                    portal.parameters.push_back(duckdb::Value(str));
                }
            } else {
                portal.parameters.push_back(duckdb::Value(str));
            }
        }
    }

    portals[msg.portal_name] = std::move(portal);

    writer.Clear();
    writer.WriteBindComplete();
    send_callback(writer.TakeBuffer());
}

void PgHandler::HandleDescribe(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    DescribeMessage msg;

    if (!reader.ReadDescribeMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Describe message");
        return;
    }

    writer.Clear();

    if (msg.describe_type == 'S') {
        // Describe statement
        auto it = prepared_statements.find(msg.name);
        if (it == prepared_statements.end()) {
            SendError("ERROR", "26000", "Prepared statement not found: " + msg.name);
            return;
        }

        // Send ParameterDescription
        std::vector<int32_t> param_oids;
        for (const auto& type : it->second.param_types) {
            param_oids.push_back(DuckDBTypeToOid(type));
        }
        writer.WriteParameterDescription(param_oids);

        // Send RowDescription or NoData
        if (it->second.column_names.empty()) {
            writer.WriteNoData();
        } else {
            writer.WriteRowDescription(it->second.column_names, it->second.column_types);
        }
    } else {
        // Describe portal
        auto it = portals.find(msg.name);
        if (it == portals.end()) {
            SendError("ERROR", "34000", "Portal not found: " + msg.name);
            return;
        }

        auto stmt_it = prepared_statements.find(it->second.statement_name);
        if (stmt_it == prepared_statements.end()) {
            writer.WriteNoData();
        } else if (stmt_it->second.column_names.empty()) {
            writer.WriteNoData();
        } else {
            writer.WriteRowDescription(stmt_it->second.column_names, stmt_it->second.column_types);
        }
    }

    send_callback(writer.TakeBuffer());
}

void PgHandler::HandleExecute(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    ExecuteMessage msg;

    if (!reader.ReadExecuteMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Execute message");
        return;
    }

    LOG_DEBUG("pg", "Execute: portal=" + msg.portal_name);

    auto portal_it = portals.find(msg.portal_name);
    if (portal_it == portals.end()) {
        SendError("ERROR", "34000", "Portal not found: " + msg.portal_name);
        return;
    }

    auto& portal = portal_it->second;
    auto stmt_it = prepared_statements.find(portal.statement_name);
    if (stmt_it == prepared_statements.end()) {
        SendError("ERROR", "26000", "Prepared statement not found");
        return;
    }

    // Capture what we need for the async lambda
    auto params = portal.parameters;
    int32_t max_rows = msg.max_rows;
    std::string query = stmt_it->second.query;
    auto statement = stmt_it->second.statement;

    // Dispatch Execute to executor pool
    async_pending = true;
    auto sess = session;
    auto send_cb = send_callback;
    auto resume_cb = resume_callback;

    executor_pool->Submit([this, params = std::move(params), max_rows, query, stmt_name = portal.statement_name,
                            statement, sess, send_cb, resume_cb]() {
        PgMessageWriter writer;
        duckdb::Connection* conn_ptr = nullptr;
        bool needs_revalidation = false;
        sess->MarkQueryStart();

        try {
            auto& conn = sess->GetConnection();
            conn_ptr = &conn;
            needs_revalidation = (last_connection != nullptr && last_connection != &conn);

            auto exec_stmt = statement;

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
                writer.WriteErrorResponse("ERROR", "42000", result->GetError());
                sess->MarkQueryEnd();
                resume_cb([this, conn_ptr, needs_revalidation, send_cb,
                           buf = writer.TakeBuffer()]() mutable {
                    if (needs_revalidation) {
                        RevalidatePreparedStatements(*conn_ptr);
                    }
                    last_connection = conn_ptr;
                    if (in_transaction) {
                        transaction_failed = true;
                    }
                    send_cb(std::move(buf));
                });
                return;
            }

            // Collect types for direct formatting
            std::vector<duckdb::LogicalType> types;
            idx_t col_count = result->ColumnCount();
            types.reserve(col_count);
            for (idx_t i = 0; i < col_count; i++) {
                types.push_back(result->types[i]);
            }

            // Send data rows using column-vector direct path, flush per chunk
            uint64_t row_count = 0;
            bool suspended = false;
            while (true) {
                auto chunk = result->Fetch();
                if (!chunk || chunk->size() == 0) break;

                auto unified = chunk->ToUnifiedFormat();
                for (idx_t row = 0; row < chunk->size(); row++) {
                    if (max_rows > 0 && row_count >= static_cast<uint64_t>(max_rows)) {
                        suspended = true;
                        break;
                    }
                    writer.WriteDataRowDirect(unified, types, row, col_count, chunk.get());
                    row_count++;
                }

                // Flush accumulated data after each chunk
                send_cb(writer.TakeBuffer());

                if (suspended) {
                    break;
                }
            }

            if (suspended) {
                writer.WritePortalSuspended();
            } else {
                std::string tag = GetCommandTag(query, row_count);
                writer.WriteCommandComplete(tag);
            }

            sess->MarkQueryEnd();
            resume_cb([this, conn_ptr, needs_revalidation, send_cb,
                       buf = writer.TakeBuffer()]() mutable {
                if (needs_revalidation) {
                    RevalidatePreparedStatements(*conn_ptr);
                }
                last_connection = conn_ptr;
                send_cb(std::move(buf));
            });

        } catch (const std::exception& e) {
            writer.WriteErrorResponse("ERROR", "XX000", e.what());
            sess->MarkQueryEnd();
            resume_cb([this, conn_ptr, needs_revalidation, send_cb,
                       buf = writer.TakeBuffer()]() mutable {
                if (needs_revalidation && conn_ptr) {
                    RevalidatePreparedStatements(*conn_ptr);
                }
                if (conn_ptr) {
                    last_connection = conn_ptr;
                }
                if (in_transaction) {
                    transaction_failed = true;
                }
                send_cb(std::move(buf));
            });
        }
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
        prepared_statements.erase(msg.name);
    } else {
        portals.erase(msg.name);
    }

    writer.Clear();
    writer.WriteCloseComplete();
    send_callback(writer.TakeBuffer());

    // Try to release connection after deallocating a prepared statement
    if (msg.close_type == 'S' && CanReleaseConnection() && session->HasActiveConnection()) {
        session->ReleaseConnection();
    }
}

void PgHandler::HandleSync(const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    SendReadyForQuery();

    // Try to release connection after sync if possible
    if (CanReleaseConnection() && session->HasActiveConnection()) {
        session->ReleaseConnection();
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
    state = PgConnectionState::CLOSED;
}

void PgHandler::SendError(const std::string& severity, const std::string& code,
                          const std::string& message, const std::string& detail) {
    writer.Clear();
    writer.WriteErrorResponse(severity, code, message, detail);
    send_callback(writer.TakeBuffer());

    if (severity == "FATAL") {
        state = PgConnectionState::FAILED;
    } else if (in_transaction) {
        transaction_failed = true;
    }
}

void PgHandler::SendReadyForQuery() {
    writer.Clear();
    writer.WriteReadyForQuery(GetTransactionStatus());
    send_callback(writer.TakeBuffer());
}

char PgHandler::GetTransactionStatus() const {
    if (!in_transaction) {
        return TransactionStatus::Idle;
    } else if (transaction_failed) {
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
