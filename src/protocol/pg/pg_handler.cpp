//===----------------------------------------------------------------------===//
//                         DuckD Server
//
// protocol/pg/pg_handler.cpp
//
// PostgreSQL protocol handler implementation
//===----------------------------------------------------------------------===//

#include "protocol/pg/pg_handler.hpp"
#include "session/session.hpp"
#include "utils/logger.hpp"
#include <algorithm>
#include <cctype>
#include <random>
#include <unistd.h>

namespace duckdb_server {
namespace pg {

PgHandler::PgHandler(std::shared_ptr<Session> session, SendCallback send_callback)
    : session_(std::move(session))
    , send_callback_(std::move(send_callback))
    , process_id_(getpid())
    , secret_key_(0) {
    // Generate random secret key
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dist;
    secret_key_ = dist(gen);
}

PgHandler::~PgHandler() = default;

bool PgHandler::ProcessData(const uint8_t* data, size_t len) {
    // Append to buffer
    buffer_.insert(buffer_.end(), data, data + len);

    while (!buffer_.empty()) {
        if (state_ == PgConnectionState::Initial) {
            // Startup message: length(4) + content
            if (buffer_.size() < 4) return true;

            int32_t msg_len;
            std::memcpy(&msg_len, buffer_.data(), 4);
            msg_len = NetworkToHost32(msg_len);

            if (msg_len < 4 || static_cast<size_t>(msg_len) > buffer_.size()) {
                return true;  // Need more data
            }

            bool result = HandleStartup(buffer_.data(), msg_len);
            buffer_.erase(buffer_.begin(), buffer_.begin() + msg_len);
            if (!result) return false;
        }
        else if (state_ == PgConnectionState::Authenticating) {
            // Password message: type(1) + length(4) + content
            if (buffer_.size() < 5) return true;

            char type = static_cast<char>(buffer_[0]);
            int32_t msg_len;
            std::memcpy(&msg_len, buffer_.data() + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > buffer_.size()) {
                return true;  // Need more data
            }

            bool result = HandleAuthentication(buffer_.data() + 5, msg_len - 4);
            buffer_.erase(buffer_.begin(), buffer_.begin() + msg_len + 1);
            if (!result) return false;
        }
        else {
            // Regular message: type(1) + length(4) + content
            if (buffer_.size() < 5) return true;

            char type = static_cast<char>(buffer_[0]);
            int32_t msg_len;
            std::memcpy(&msg_len, buffer_.data() + 1, 4);
            msg_len = NetworkToHost32(msg_len);

            if (static_cast<size_t>(msg_len + 1) > buffer_.size()) {
                return true;  // Need more data
            }

            bool result = HandleMessage(type, buffer_.data() + 5, msg_len - 4);
            buffer_.erase(buffer_.begin(), buffer_.begin() + msg_len + 1);
            if (!result) return false;
        }
    }

    return true;
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
    std::string sql = msg.query;
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

    ExecuteQuery(sql);
}

void PgHandler::ExecuteQuery(const std::string& sql) {
    writer_.Clear();

    try {
        auto result = session_->GetConnection().Query(sql);

        if (result->HasError()) {
            SendError("ERROR", "42000", result->GetError());
            SendReadyForQuery();
            return;
        }

        // Check for transaction state changes
        std::string upper_sql = sql;
        std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

        if (upper_sql.find("BEGIN") != std::string::npos) {
            in_transaction_ = true;
            transaction_failed_ = false;
        } else if (upper_sql.find("COMMIT") != std::string::npos ||
                   upper_sql.find("ROLLBACK") != std::string::npos) {
            in_transaction_ = false;
            transaction_failed_ = false;
        }

        SendQueryResult(*result, sql);
        SendReadyForQuery();

    } catch (const std::exception& e) {
        if (in_transaction_) {
            transaction_failed_ = true;
        }
        SendError("ERROR", "XX000", e.what());
        SendReadyForQuery();
    }
}

void PgHandler::SendQueryResult(duckdb::QueryResult& result, const std::string& command) {
    // Get column info
    std::vector<std::string> names;
    std::vector<duckdb::LogicalType> types;

    for (size_t i = 0; i < result.ColumnCount(); i++) {
        names.push_back(result.ColumnName(i));
        types.push_back(result.types[i]);
    }

    // Send RowDescription if there are columns
    if (!names.empty()) {
        writer_.WriteRowDescription(names, types);
    }

    // Send data rows
    uint64_t row_count = 0;
    while (true) {
        auto chunk = result.Fetch();
        if (!chunk || chunk->size() == 0) break;

        for (size_t row = 0; row < chunk->size(); row++) {
            std::vector<duckdb::Value> row_values;
            for (size_t col = 0; col < chunk->ColumnCount(); col++) {
                row_values.push_back(chunk->GetValue(col, row));
            }
            writer_.WriteDataRow(row_values);
            row_count++;
        }
    }

    // Send CommandComplete
    std::string tag = GetCommandTag(command, row_count);
    writer_.WriteCommandComplete(tag);

    send_callback_(writer_.GetBuffer());
}

void PgHandler::HandleParse(const uint8_t* data, size_t len) {
    PgMessageReader reader(data, len);
    ParseMessage msg;

    if (!reader.ReadParseMessage(msg)) {
        SendError("ERROR", "08P01", "Invalid Parse message");
        return;
    }

    LOG_DEBUG("pg", "Parse: " + msg.statement_name + " = " + msg.query);

    try {
        auto prepared = session_->GetConnection().Prepare(msg.query);

        if (prepared->HasError()) {
            SendError("ERROR", "42000", prepared->GetError());
            return;
        }

        PreparedStatementInfo info;
        info.query = msg.query;
        info.statement = std::move(prepared);

        // Get column info from statement
        for (idx_t i = 0; i < info.statement->ColumnCount(); i++) {
            info.column_names.push_back(info.statement->GetNames()[i]);
            info.column_types.push_back(info.statement->GetTypes()[i]);
        }

        prepared_statements_[msg.statement_name] = std::move(info);

        writer_.Clear();
        writer_.WriteParseComplete();
        send_callback_(writer_.GetBuffer());

    } catch (const std::exception& e) {
        SendError("ERROR", "42000", e.what());
    }
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

    try {
        // Execute the prepared statement with bound parameters
        std::unique_ptr<duckdb::QueryResult> result;
        if (portal.parameters.empty()) {
            result = stmt_it->second.statement->Execute();
        } else {
            // Use PendingQuery with vector<Value>& for parameter binding
            duckdb::vector<duckdb::Value> params(portal.parameters.begin(), portal.parameters.end());
            auto pending = stmt_it->second.statement->PendingQuery(params, true);
            result = pending->Execute();
        }

        if (result->HasError()) {
            SendError("ERROR", "42000", result->GetError());
            return;
        }

        writer_.Clear();

        // Get column info for result
        std::vector<std::string> names;
        std::vector<duckdb::LogicalType> types;
        for (size_t i = 0; i < result->ColumnCount(); i++) {
            names.push_back(result->ColumnName(i));
            types.push_back(result->types[i]);
        }

        // Send data rows
        uint64_t row_count = 0;
        while (true) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) break;

            for (size_t row = 0; row < chunk->size(); row++) {
                if (msg.max_rows > 0 && row_count >= static_cast<uint64_t>(msg.max_rows)) {
                    break;
                }
                std::vector<duckdb::Value> row_values;
                for (size_t col = 0; col < chunk->ColumnCount(); col++) {
                    row_values.push_back(chunk->GetValue(col, row));
                }
                writer_.WriteDataRow(row_values);
                row_count++;
            }
            if (msg.max_rows > 0 && row_count >= static_cast<uint64_t>(msg.max_rows)) {
                break;
            }
        }

        std::string tag = GetCommandTag(stmt_it->second.query, row_count);
        writer_.WriteCommandComplete(tag);

        send_callback_(writer_.GetBuffer());

    } catch (const std::exception& e) {
        SendError("ERROR", "XX000", e.what());
    }
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
}

void PgHandler::HandleSync(const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    SendReadyForQuery();
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
    std::string upper_sql = sql;
    std::transform(upper_sql.begin(), upper_sql.end(), upper_sql.begin(), ::toupper);

    // Remove leading whitespace
    size_t start = upper_sql.find_first_not_of(" \t\n\r");
    if (start != std::string::npos) {
        upper_sql = upper_sql.substr(start);
    }

    if (upper_sql.rfind("SELECT", 0) == 0 || upper_sql.rfind("WITH", 0) == 0) {
        return "SELECT " + std::to_string(rows_affected);
    } else if (upper_sql.rfind("INSERT", 0) == 0) {
        return "INSERT 0 " + std::to_string(rows_affected);
    } else if (upper_sql.rfind("UPDATE", 0) == 0) {
        return "UPDATE " + std::to_string(rows_affected);
    } else if (upper_sql.rfind("DELETE", 0) == 0) {
        return "DELETE " + std::to_string(rows_affected);
    } else if (upper_sql.rfind("CREATE", 0) == 0) {
        if (upper_sql.find("TABLE") != std::string::npos) {
            return "CREATE TABLE";
        } else if (upper_sql.find("INDEX") != std::string::npos) {
            return "CREATE INDEX";
        } else if (upper_sql.find("VIEW") != std::string::npos) {
            return "CREATE VIEW";
        } else if (upper_sql.find("SCHEMA") != std::string::npos) {
            return "CREATE SCHEMA";
        }
        return "CREATE";
    } else if (upper_sql.rfind("DROP", 0) == 0) {
        if (upper_sql.find("TABLE") != std::string::npos) {
            return "DROP TABLE";
        }
        return "DROP";
    } else if (upper_sql.rfind("ALTER", 0) == 0) {
        return "ALTER TABLE";
    } else if (upper_sql.rfind("BEGIN", 0) == 0) {
        return "BEGIN";
    } else if (upper_sql.rfind("COMMIT", 0) == 0) {
        return "COMMIT";
    } else if (upper_sql.rfind("ROLLBACK", 0) == 0) {
        return "ROLLBACK";
    } else if (upper_sql.rfind("SET", 0) == 0) {
        return "SET";
    } else if (upper_sql.rfind("SHOW", 0) == 0) {
        return "SHOW";
    } else if (upper_sql.rfind("COPY", 0) == 0) {
        return "COPY " + std::to_string(rows_affected);
    }

    return "OK";
}

} // namespace pg
} // namespace duckdb_server
