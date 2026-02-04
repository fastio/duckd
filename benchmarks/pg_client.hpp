//===----------------------------------------------------------------------===//
//                         DuckD Server - Benchmarks
//
// benchmarks/pg_client.hpp
//
// Simple PostgreSQL client using ASIO (no libpq dependency)
// Implements PostgreSQL wire protocol v3.0
//===----------------------------------------------------------------------===//

#pragma once

#include <asio.hpp>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace duckd::bench {

//===----------------------------------------------------------------------===//
// Network Byte Order Utilities
//===----------------------------------------------------------------------===//

inline int32_t HostToNetwork32(int32_t value) {
    uint32_t v = static_cast<uint32_t>(value);
    return static_cast<int32_t>(
        ((v & 0xFF000000) >> 24) |
        ((v & 0x00FF0000) >> 8) |
        ((v & 0x0000FF00) << 8) |
        ((v & 0x000000FF) << 24)
    );
}

inline int32_t NetworkToHost32(int32_t value) {
    return HostToNetwork32(value);  // Same operation
}

inline int16_t HostToNetwork16(int16_t value) {
    uint16_t v = static_cast<uint16_t>(value);
    return static_cast<int16_t>(((v & 0xFF00) >> 8) | ((v & 0x00FF) << 8));
}

inline int16_t NetworkToHost16(int16_t value) {
    return HostToNetwork16(value);
}

//===----------------------------------------------------------------------===//
// PostgreSQL Protocol Constants
//===----------------------------------------------------------------------===//

constexpr int32_t PROTOCOL_VERSION_3_0 = 196608;  // 3 << 16

// Backend message types
namespace BackendMsg {
    constexpr char Authentication = 'R';
    constexpr char ParameterStatus = 'S';
    constexpr char BackendKeyData = 'K';
    constexpr char ReadyForQuery = 'Z';
    constexpr char RowDescription = 'T';
    constexpr char DataRow = 'D';
    constexpr char CommandComplete = 'C';
    constexpr char ErrorResponse = 'E';
    constexpr char EmptyQueryResponse = 'I';
    constexpr char ParseComplete = '1';
    constexpr char BindComplete = '2';
    constexpr char CloseComplete = '3';
    constexpr char NoData = 'n';
}

// Authentication types
constexpr int32_t AUTH_OK = 0;
constexpr int32_t AUTH_CLEARTEXT = 3;

//===----------------------------------------------------------------------===//
// Message Buffer
//===----------------------------------------------------------------------===//

class MessageBuffer {
public:
    MessageBuffer() { buffer_.reserve(4096); }

    void Clear() { buffer_.clear(); }

    // Write startup message (no type byte)
    void WriteStartupMessage(const std::string& user, const std::string& database) {
        Clear();
        // Reserve space for length
        size_t len_pos = buffer_.size();
        WriteInt32(0);  // Placeholder for length

        WriteInt32(PROTOCOL_VERSION_3_0);
        WriteString("user");
        WriteString(user);
        WriteString("database");
        WriteString(database);
        WriteByte(0);  // Null terminator

        // Update length (includes length field itself)
        int32_t len = HostToNetwork32(static_cast<int32_t>(buffer_.size()));
        std::memcpy(&buffer_[len_pos], &len, 4);
    }

    // Write regular message (with type byte)
    void StartMessage(char type) {
        Clear();
        WriteByte(type);
        len_pos_ = buffer_.size();
        WriteInt32(0);  // Placeholder for length
    }

    void EndMessage() {
        // Length includes itself but not type byte
        int32_t len = HostToNetwork32(static_cast<int32_t>(buffer_.size() - len_pos_));
        std::memcpy(&buffer_[len_pos_], &len, 4);
    }

    void WriteByte(char b) { buffer_.push_back(b); }

    void WriteInt16(int16_t value) {
        int16_t net = HostToNetwork16(value);
        const char* p = reinterpret_cast<const char*>(&net);
        buffer_.insert(buffer_.end(), p, p + 2);
    }

    void WriteInt32(int32_t value) {
        int32_t net = HostToNetwork32(value);
        const char* p = reinterpret_cast<const char*>(&net);
        buffer_.insert(buffer_.end(), p, p + 4);
    }

    void WriteString(const std::string& s) {
        buffer_.insert(buffer_.end(), s.begin(), s.end());
        buffer_.push_back(0);  // Null terminator
    }

    void WriteBytes(const char* data, size_t len) {
        buffer_.insert(buffer_.end(), data, data + len);
    }

    const std::vector<char>& Data() const { return buffer_; }
    size_t Size() const { return buffer_.size(); }

private:
    std::vector<char> buffer_;
    size_t len_pos_ = 0;
};

//===----------------------------------------------------------------------===//
// Query Result
//===----------------------------------------------------------------------===//

struct QueryResult {
    bool success = false;
    std::string error_message;
    std::string command_tag;
    int rows_affected = 0;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> rows;

    bool Ok() const { return success; }
    int NumRows() const { return static_cast<int>(rows.size()); }
    int NumCols() const { return static_cast<int>(column_names.size()); }
};

//===----------------------------------------------------------------------===//
// PostgreSQL Connection
//===----------------------------------------------------------------------===//

class PgConnection {
public:
    PgConnection() : socket_(io_context_) {}

    ~PgConnection() { Disconnect(); }

    // Non-copyable
    PgConnection(const PgConnection&) = delete;
    PgConnection& operator=(const PgConnection&) = delete;

    // Movable
    PgConnection(PgConnection&& other) noexcept
        : io_context_(), socket_(io_context_), connected_(other.connected_) {
        // Note: ASIO sockets can't be moved across io_contexts easily
        // For simplicity, we don't support moving connected sockets
        other.connected_ = false;
    }

    bool Connect(const std::string& host, int port,
                 const std::string& user, const std::string& database) {
        try {
            Disconnect();

            asio::ip::tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(host, std::to_string(port));

            asio::connect(socket_, endpoints);
            socket_.set_option(asio::ip::tcp::no_delay(true));

            // Send startup message
            MessageBuffer buf;
            buf.WriteStartupMessage(user, database);
            asio::write(socket_, asio::buffer(buf.Data()));

            // Handle authentication
            if (!HandleAuthentication()) {
                Disconnect();
                return false;
            }

            connected_ = true;
            return true;
        } catch (const std::exception& e) {
            error_message_ = e.what();
            return false;
        }
    }

    void Disconnect() {
        if (socket_.is_open()) {
            try {
                // Send Terminate message
                MessageBuffer buf;
                buf.StartMessage('X');
                buf.EndMessage();
                asio::write(socket_, asio::buffer(buf.Data()));
            } catch (...) {}

            asio::error_code ec;
            socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
            socket_.close(ec);
        }
        connected_ = false;
    }

    bool IsConnected() const { return connected_ && socket_.is_open(); }

    std::string ErrorMessage() const { return error_message_; }

    // Simple query protocol
    QueryResult Execute(const std::string& query) {
        QueryResult result;

        if (!IsConnected()) {
            result.error_message = "Not connected";
            return result;
        }

        try {
            // Send Query message
            MessageBuffer buf;
            buf.StartMessage('Q');
            buf.WriteString(query);
            buf.EndMessage();

            asio::write(socket_, asio::buffer(buf.Data()));

            // Read responses until ReadyForQuery
            while (true) {
                char type;
                int32_t len;

                asio::read(socket_, asio::buffer(&type, 1));
                asio::read(socket_, asio::buffer(&len, 4));
                len = NetworkToHost32(len) - 4;

                std::vector<char> body(len);
                if (len > 0) {
                    asio::read(socket_, asio::buffer(body));
                }

                if (!HandleMessage(type, body, result)) {
                    break;
                }

                if (type == BackendMsg::ReadyForQuery) {
                    result.success = result.error_message.empty();
                    break;
                }
            }
        } catch (const std::exception& e) {
            result.error_message = e.what();
            connected_ = false;
        }

        return result;
    }

    // Prepare statement (extended query protocol)
    QueryResult Prepare(const std::string& stmt_name, const std::string& query) {
        QueryResult result;

        if (!IsConnected()) {
            result.error_message = "Not connected";
            return result;
        }

        try {
            // Parse message
            MessageBuffer buf;
            buf.StartMessage('P');
            buf.WriteString(stmt_name);
            buf.WriteString(query);
            buf.WriteInt16(0);  // No parameter types specified
            buf.EndMessage();

            // Sync message
            MessageBuffer sync;
            sync.StartMessage('S');
            sync.EndMessage();

            // Send both
            std::vector<asio::const_buffer> buffers;
            buffers.push_back(asio::buffer(buf.Data()));
            buffers.push_back(asio::buffer(sync.Data()));
            asio::write(socket_, buffers);

            // Read until ReadyForQuery
            while (true) {
                char type;
                int32_t len;

                asio::read(socket_, asio::buffer(&type, 1));
                asio::read(socket_, asio::buffer(&len, 4));
                len = NetworkToHost32(len) - 4;

                std::vector<char> body(len);
                if (len > 0) {
                    asio::read(socket_, asio::buffer(body));
                }

                HandleMessage(type, body, result);

                if (type == BackendMsg::ReadyForQuery) {
                    result.success = result.error_message.empty();
                    break;
                }
            }
        } catch (const std::exception& e) {
            result.error_message = e.what();
            connected_ = false;
        }

        return result;
    }

    // Execute prepared statement
    QueryResult ExecutePrepared(const std::string& stmt_name,
                                const std::vector<std::string>& params) {
        QueryResult result;

        if (!IsConnected()) {
            result.error_message = "Not connected";
            return result;
        }

        try {
            // Bind message
            MessageBuffer bind;
            bind.StartMessage('B');
            bind.WriteString("");        // Portal name (unnamed)
            bind.WriteString(stmt_name); // Statement name
            bind.WriteInt16(0);          // No parameter format codes (use text)
            bind.WriteInt16(static_cast<int16_t>(params.size()));
            for (const auto& p : params) {
                bind.WriteInt32(static_cast<int32_t>(p.size()));
                bind.WriteBytes(p.data(), p.size());
            }
            bind.WriteInt16(0);  // No result format codes (use text)
            bind.EndMessage();

            // Execute message
            MessageBuffer exec;
            exec.StartMessage('E');
            exec.WriteString("");  // Portal name
            exec.WriteInt32(0);    // No row limit
            exec.EndMessage();

            // Sync message
            MessageBuffer sync;
            sync.StartMessage('S');
            sync.EndMessage();

            // Send all
            std::vector<asio::const_buffer> buffers;
            buffers.push_back(asio::buffer(bind.Data()));
            buffers.push_back(asio::buffer(exec.Data()));
            buffers.push_back(asio::buffer(sync.Data()));
            asio::write(socket_, buffers);

            // Read until ReadyForQuery
            while (true) {
                char type;
                int32_t len;

                asio::read(socket_, asio::buffer(&type, 1));
                asio::read(socket_, asio::buffer(&len, 4));
                len = NetworkToHost32(len) - 4;

                std::vector<char> body(len);
                if (len > 0) {
                    asio::read(socket_, asio::buffer(body));
                }

                HandleMessage(type, body, result);

                if (type == BackendMsg::ReadyForQuery) {
                    result.success = result.error_message.empty();
                    break;
                }
            }
        } catch (const std::exception& e) {
            result.error_message = e.what();
            connected_ = false;
        }

        return result;
    }

    // Deallocate prepared statement
    QueryResult Deallocate(const std::string& stmt_name) {
        return Execute("DEALLOCATE " + stmt_name);
    }

    // Transaction control
    QueryResult Begin() { return Execute("BEGIN"); }
    QueryResult Commit() { return Execute("COMMIT"); }
    QueryResult Rollback() { return Execute("ROLLBACK"); }

private:
    bool HandleAuthentication() {
        while (true) {
            char type;
            int32_t len;

            asio::read(socket_, asio::buffer(&type, 1));
            asio::read(socket_, asio::buffer(&len, 4));
            len = NetworkToHost32(len) - 4;

            std::vector<char> body(len);
            if (len > 0) {
                asio::read(socket_, asio::buffer(body));
            }

            if (type == BackendMsg::Authentication) {
                if (len >= 4) {
                    int32_t auth_type;
                    std::memcpy(&auth_type, body.data(), 4);
                    auth_type = NetworkToHost32(auth_type);

                    if (auth_type == AUTH_OK) {
                        // Authentication successful, continue to ReadyForQuery
                    } else if (auth_type == AUTH_CLEARTEXT) {
                        // Send empty password (for now)
                        MessageBuffer buf;
                        buf.StartMessage('p');
                        buf.WriteString("");
                        buf.EndMessage();
                        asio::write(socket_, asio::buffer(buf.Data()));
                    } else {
                        error_message_ = "Unsupported authentication type: " +
                                         std::to_string(auth_type);
                        return false;
                    }
                }
            } else if (type == BackendMsg::ErrorResponse) {
                error_message_ = ParseErrorMessage(body);
                return false;
            } else if (type == BackendMsg::ReadyForQuery) {
                return true;
            }
            // Ignore ParameterStatus, BackendKeyData, etc.
        }
    }

    bool HandleMessage(char type, const std::vector<char>& body, QueryResult& result) {
        switch (type) {
            case BackendMsg::RowDescription:
                ParseRowDescription(body, result);
                break;
            case BackendMsg::DataRow:
                ParseDataRow(body, result);
                break;
            case BackendMsg::CommandComplete:
                if (!body.empty()) {
                    result.command_tag = std::string(body.data());
                }
                break;
            case BackendMsg::ErrorResponse:
                result.error_message = ParseErrorMessage(body);
                break;
            case BackendMsg::EmptyQueryResponse:
            case BackendMsg::ParseComplete:
            case BackendMsg::BindComplete:
            case BackendMsg::CloseComplete:
            case BackendMsg::NoData:
                // OK responses, ignore
                break;
            case BackendMsg::ReadyForQuery:
                return true;
            default:
                // Unknown message, ignore
                break;
        }
        return true;
    }

    void ParseRowDescription(const std::vector<char>& body, QueryResult& result) {
        if (body.size() < 2) return;

        size_t pos = 0;
        int16_t num_fields;
        std::memcpy(&num_fields, &body[pos], 2);
        num_fields = NetworkToHost16(num_fields);
        pos += 2;

        result.column_names.clear();
        for (int i = 0; i < num_fields && pos < body.size(); ++i) {
            // Read null-terminated string
            std::string name;
            while (pos < body.size() && body[pos] != 0) {
                name += body[pos++];
            }
            pos++;  // Skip null terminator
            result.column_names.push_back(name);

            // Skip table OID (4), column attr (2), type OID (4),
            // type size (2), type modifier (4), format (2) = 18 bytes
            pos += 18;
        }
    }

    void ParseDataRow(const std::vector<char>& body, QueryResult& result) {
        if (body.size() < 2) return;

        size_t pos = 0;
        int16_t num_cols;
        std::memcpy(&num_cols, &body[pos], 2);
        num_cols = NetworkToHost16(num_cols);
        pos += 2;

        std::vector<std::string> row;
        for (int i = 0; i < num_cols && pos < body.size(); ++i) {
            int32_t col_len;
            std::memcpy(&col_len, &body[pos], 4);
            col_len = NetworkToHost32(col_len);
            pos += 4;

            if (col_len == -1) {
                row.push_back("");  // NULL
            } else if (col_len > 0 && pos + col_len <= body.size()) {
                row.push_back(std::string(&body[pos], col_len));
                pos += col_len;
            } else {
                row.push_back("");
            }
        }
        result.rows.push_back(std::move(row));
    }

    std::string ParseErrorMessage(const std::vector<char>& body) {
        std::string message;
        size_t pos = 0;

        while (pos < body.size() && body[pos] != 0) {
            char field_type = body[pos++];
            std::string field_value;
            while (pos < body.size() && body[pos] != 0) {
                field_value += body[pos++];
            }
            pos++;  // Skip null terminator

            if (field_type == 'M') {  // Message field
                message = field_value;
            }
        }

        return message.empty() ? "Unknown error" : message;
    }

    asio::io_context io_context_;
    asio::ip::tcp::socket socket_;
    bool connected_ = false;
    std::string error_message_;
};

//===----------------------------------------------------------------------===//
// Connection Pool (Simple)
//===----------------------------------------------------------------------===//

class PgConnectionPool {
public:
    PgConnectionPool(const std::string& host, int port,
                     const std::string& user, const std::string& database,
                     size_t pool_size)
        : host_(host), port_(port), user_(user), database_(database) {
        connections_.reserve(pool_size);
        for (size_t i = 0; i < pool_size; ++i) {
            auto conn = std::make_unique<PgConnection>();
            if (!conn->Connect(host, port, user, database)) {
                throw std::runtime_error("Failed to create connection: " +
                                         conn->ErrorMessage());
            }
            connections_.push_back(std::move(conn));
        }
    }

    PgConnection& Get(size_t index) {
        return *connections_[index % connections_.size()];
    }

    size_t Size() const { return connections_.size(); }

    bool AllConnected() const {
        for (const auto& conn : connections_) {
            if (!conn->IsConnected()) return false;
        }
        return true;
    }

private:
    std::string host_;
    int port_;
    std::string user_;
    std::string database_;
    std::vector<std::unique_ptr<PgConnection>> connections_;
};

}  // namespace duckd::bench
