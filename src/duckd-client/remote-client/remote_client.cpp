//===----------------------------------------------------------------------===//
//                         DuckDB Remote Extension
//
// remote_client.cpp
//
// Network client implementation using ASIO
//===----------------------------------------------------------------------===//

#include "remote_client.hpp"
#include "duckdb/common/exception.hpp"

#ifdef _WIN32
#define _WIN32_WINNT 0x0601
#endif

#include <asio.hpp>
#include <cstring>

namespace duckdb {

using asio::ip::tcp;
using namespace remote_protocol;

//===----------------------------------------------------------------------===//
// Socket Implementation (PIMPL)
//===----------------------------------------------------------------------===//

class RemoteClient::SocketImpl {
public:
    asio::io_context io_context;
    tcp::socket socket;

    SocketImpl() : socket(io_context) {}
};

//===----------------------------------------------------------------------===//
// RemoteClient Implementation
//===----------------------------------------------------------------------===//

RemoteClient::RemoteClient() : socket_impl_(make_uniq<SocketImpl>()) {}

RemoteClient::~RemoteClient() {
    if (connected_) {
        try {
            Disconnect();
        } catch (...) {
            // Ignore disconnect errors in destructor
        }
    }
}

void RemoteClient::Connect(const string &host, uint16_t port, const string &username) {
    if (connected_) {
        throw IOException("Already connected");
    }

    host_ = host;
    port_ = port;

    try {
        // Resolve and connect
        tcp::resolver resolver(socket_impl_->io_context);
        auto endpoints = resolver.resolve(host, std::to_string(port));

        asio::connect(socket_impl_->socket, endpoints);

        // Send HELLO
        auto hello_payload = BuildHelloPayload(username);
        SendMessage(MessageType::HELLO, hello_payload);

        // Receive HELLO_RESPONSE
        MessageHeader header;
        vector<uint8_t> payload;

        if (!ReceiveMessage(header, payload)) {
            throw IOException("Failed to receive HELLO_RESPONSE");
        }

        if (static_cast<MessageType>(header.type) == MessageType::QUERY_ERROR) {
            RemoteQueryResult result;
            ParseError(payload, result);
            throw IOException("Connection rejected: " + result.error_message);
        }

        if (static_cast<MessageType>(header.type) != MessageType::HELLO_RESPONSE) {
            throw IOException("Unexpected response to HELLO");
        }

        ParseHelloResponse(payload);
        connected_ = true;

    } catch (const asio::system_error &e) {
        throw IOException("Connection failed: " + string(e.what()));
    }
}

void RemoteClient::Disconnect() {
    if (!connected_) {
        return;
    }

    try {
        // Send CLOSE message
        SendMessage(MessageType::CLOSE, {});
    } catch (...) {
        // Ignore errors during close
    }

    try {
        socket_impl_->socket.shutdown(tcp::socket::shutdown_both);
        socket_impl_->socket.close();
    } catch (...) {
        // Ignore socket errors
    }

    connected_ = false;
    session_id_ = 0;
}

bool RemoteClient::IsConnected() const {
    return connected_;
}

unique_ptr<RemoteQueryResult> RemoteClient::Query(const string &sql, uint32_t timeout_ms) {
    if (!connected_) {
        throw IOException("Not connected");
    }

    auto result = make_uniq<RemoteQueryResult>();
    uint64_t query_id = ++query_id_counter_;

    try {
        // Send QUERY
        auto query_payload = BuildQueryPayload(query_id, sql, timeout_ms);
        SendMessage(MessageType::QUERY, query_payload);

        // Receive results
        while (true) {
            MessageHeader header;
            vector<uint8_t> payload;

            if (!ReceiveMessage(header, payload)) {
                result->has_error = true;
                result->error_message = "Connection closed";
                break;
            }

            auto msg_type = static_cast<MessageType>(header.type);

            switch (msg_type) {
            case MessageType::QUERY_ERROR:
                ParseError(payload, *result);
                return result;

            case MessageType::RESULT_HEADER:
                ParseSchema(payload, *result);
                break;

            case MessageType::RESULT_BATCH:
                ParseBatch(payload, *result);
                break;

            case MessageType::RESULT_END:
                ParseResultEnd(payload, *result);
                return result;

            default:
                result->has_error = true;
                result->error_message = "Unexpected message type: " + std::to_string(header.type);
                return result;
            }
        }
    } catch (const std::exception &e) {
        result->has_error = true;
        result->error_message = e.what();
    }

    return result;
}

//===----------------------------------------------------------------------===//
// Network Operations
//===----------------------------------------------------------------------===//

void RemoteClient::SendMessage(MessageType type, const vector<uint8_t> &payload) {
    lock_guard<mutex> lock(send_mutex_);

    MessageHeader header;
    header.magic = PROTOCOL_MAGIC;
    header.version = PROTOCOL_VERSION;
    header.type = static_cast<uint8_t>(type);
    header.flags = 0;
    header.reserved = 0;
    header.length = static_cast<uint32_t>(payload.size());

    // Send header
    asio::write(socket_impl_->socket, asio::buffer(&header, sizeof(header)));

    // Send payload
    if (!payload.empty()) {
        asio::write(socket_impl_->socket, asio::buffer(payload.data(), payload.size()));
    }
}

bool RemoteClient::ReceiveMessage(MessageHeader &header, vector<uint8_t> &payload) {
    lock_guard<mutex> lock(recv_mutex_);

    try {
        // Read header
        ReceiveExact(reinterpret_cast<uint8_t*>(&header), sizeof(header));

        if (!header.IsValid()) {
            throw IOException("Invalid message header");
        }

        // Read payload
        payload.clear();
        if (header.length > 0) {
            payload.resize(header.length);
            ReceiveExact(payload.data(), header.length);
        }

        return true;
    } catch (const asio::system_error &e) {
        if (e.code() == asio::error::eof) {
            return false;
        }
        throw IOException("Receive error: " + string(e.what()));
    }
}

void RemoteClient::ReceiveExact(uint8_t *buffer, size_t size) {
    size_t received = 0;
    while (received < size) {
        size_t n = socket_impl_->socket.read_some(asio::buffer(buffer + received, size - received));
        if (n == 0) {
            throw IOException("Connection closed");
        }
        received += n;
    }
}

//===----------------------------------------------------------------------===//
// Payload Builders
//===----------------------------------------------------------------------===//

vector<uint8_t> RemoteClient::BuildHelloPayload(const string &username) {
    vector<uint8_t> payload;

    // protocol_version (1 byte)
    payload.push_back(PROTOCOL_VERSION);

    // client_type (1 byte)
    payload.push_back(static_cast<uint8_t>(ClientType::CPP));

    // client_version (2 bytes)
    uint16_t client_version = 1;
    payload.push_back(client_version & 0xFF);
    payload.push_back((client_version >> 8) & 0xFF);

    // capabilities (4 bytes)
    uint32_t capabilities = 0;
    for (int i = 0; i < 4; i++) {
        payload.push_back((capabilities >> (i * 8)) & 0xFF);
    }

    // username length + data
    uint32_t username_len = static_cast<uint32_t>(username.size());
    for (int i = 0; i < 4; i++) {
        payload.push_back((username_len >> (i * 8)) & 0xFF);
    }
    payload.insert(payload.end(), username.begin(), username.end());

    // auth_data length (0)
    for (int i = 0; i < 4; i++) {
        payload.push_back(0);
    }

    return payload;
}

vector<uint8_t> RemoteClient::BuildQueryPayload(uint64_t query_id, const string &sql, uint32_t timeout_ms) {
    vector<uint8_t> payload;

    // query_id (8 bytes)
    for (int i = 0; i < 8; i++) {
        payload.push_back((query_id >> (i * 8)) & 0xFF);
    }

    // flags (4 bytes)
    for (int i = 0; i < 4; i++) {
        payload.push_back(0);
    }

    // timeout_ms (4 bytes)
    for (int i = 0; i < 4; i++) {
        payload.push_back((timeout_ms >> (i * 8)) & 0xFF);
    }

    // max_rows (4 bytes) - 0 means no limit
    for (int i = 0; i < 4; i++) {
        payload.push_back(0);
    }

    // sql length + data
    uint32_t sql_len = static_cast<uint32_t>(sql.size());
    for (int i = 0; i < 4; i++) {
        payload.push_back((sql_len >> (i * 8)) & 0xFF);
    }
    payload.insert(payload.end(), sql.begin(), sql.end());

    return payload;
}

//===----------------------------------------------------------------------===//
// Response Parsers
//===----------------------------------------------------------------------===//

void RemoteClient::ParseHelloResponse(const vector<uint8_t> &payload) {
    if (payload.size() < 14) {
        throw IOException("Invalid HELLO_RESPONSE payload");
    }

    size_t offset = 0;

    // status (1 byte)
    uint8_t status = payload[offset++];
    if (status != 0) {
        throw IOException("Connection rejected with status: " + std::to_string(status));
    }

    // session_id (8 bytes)
    session_id_ = 0;
    for (int i = 0; i < 8; i++) {
        session_id_ |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }
}

void RemoteClient::ParseSchema(const vector<uint8_t> &payload, RemoteQueryResult &result) {
    if (payload.size() < 8) {
        throw IOException("Invalid RESULT_HEADER payload");
    }

    size_t offset = 0;

    // n_columns (8 bytes)
    uint64_t n_columns = 0;
    for (int i = 0; i < 8; i++) {
        n_columns |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }

    result.columns.clear();
    result.columns.reserve(n_columns);

    for (uint64_t i = 0; i < n_columns; i++) {
        RemoteColumnInfo col;

        // name length (4 bytes)
        uint32_t name_len = 0;
        for (int j = 0; j < 4; j++) {
            name_len |= static_cast<uint32_t>(payload[offset++]) << (j * 8);
        }
        col.name = string(reinterpret_cast<const char*>(&payload[offset]), name_len);
        offset += name_len;

        // format/type length (4 bytes)
        uint32_t format_len = 0;
        for (int j = 0; j < 4; j++) {
            format_len |= static_cast<uint32_t>(payload[offset++]) << (j * 8);
        }
        col.type_str = string(reinterpret_cast<const char*>(&payload[offset]), format_len);
        offset += format_len;

        // Parse Arrow format to DuckDB type
        col.type = ParseArrowTypeFormat(col.type_str);

        // nullable (1 byte)
        col.nullable = (payload[offset++] != 0);

        result.columns.push_back(std::move(col));
    }
}

void RemoteClient::ParseBatch(const vector<uint8_t> &payload, RemoteQueryResult &result) {
    if (payload.size() < 32) {
        return;  // Empty batch
    }

    size_t offset = 0;

    // Read array metadata
    uint64_t length = 0;
    for (int i = 0; i < 8; i++) {
        length |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }

    // Skip null_count (8 bytes) - not used at struct level
    offset += 8;

    // Skip n_buffers (8 bytes) - not used at struct level
    offset += 8;

    uint64_t n_children = 0;
    for (int i = 0; i < 8; i++) {
        n_children |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }

    if (length == 0) {
        return;
    }

    // Parse each column
    vector<vector<Value>> columns_data(n_children);

    for (uint64_t col_idx = 0; col_idx < n_children; col_idx++) {
        if (offset + 24 > payload.size()) {
            break;
        }

        uint64_t child_length = 0;
        for (int i = 0; i < 8; i++) {
            child_length |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
        }

        uint64_t child_null_count = 0;
        for (int i = 0; i < 8; i++) {
            child_null_count |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
        }

        uint64_t child_n_buffers = 0;
        for (int i = 0; i < 8; i++) {
            child_n_buffers |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
        }

        // Get column type
        LogicalType col_type = col_idx < result.columns.size()
                                   ? result.columns[col_idx].type
                                   : LogicalType::VARCHAR;
        string type_str = col_idx < result.columns.size()
                              ? result.columns[col_idx].type_str
                              : "u";

        // Read buffers
        vector<vector<uint8_t>> buffers;
        for (uint64_t buf_idx = 0; buf_idx < child_n_buffers; buf_idx++) {
            if (offset + 8 > payload.size()) {
                break;
            }

            uint64_t buf_size = 0;
            for (int i = 0; i < 8; i++) {
                buf_size |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
            }

            if (buf_size > 0 && offset + buf_size <= payload.size()) {
                buffers.emplace_back(payload.begin() + offset, payload.begin() + offset + buf_size);
                offset += buf_size;
            } else {
                buffers.emplace_back();
            }
        }

        // Decode column values
        columns_data[col_idx] = DecodeColumn(buffers, type_str, child_length, child_null_count);
    }

    // Transpose columns to rows
    for (uint64_t row_idx = 0; row_idx < length; row_idx++) {
        vector<Value> row;
        row.reserve(n_children);

        for (uint64_t col_idx = 0; col_idx < n_children; col_idx++) {
            if (row_idx < columns_data[col_idx].size()) {
                row.push_back(columns_data[col_idx][row_idx]);
            } else {
                row.push_back(Value());  // NULL
            }
        }

        result.rows.push_back(std::move(row));
    }
}

vector<Value> RemoteClient::DecodeColumn(const vector<vector<uint8_t>> &buffers,
                                          const string &type_str,
                                          uint64_t length,
                                          uint64_t null_count) {
    vector<Value> values;
    values.reserve(length);

    if (buffers.empty()) {
        for (uint64_t i = 0; i < length; i++) {
            values.push_back(Value());
        }
        return values;
    }

    // Arrow buffer layout:
    // - buffers[0]: validity bitmap (may be empty if no nulls)
    // - buffers[1]: data buffer for fixed-size types
    // - For strings: buffers[1] = offsets, buffers[2] = data

    // Validity buffer is always at index 0 (if non-empty and null_count > 0)
    const vector<uint8_t> *validity_buf = nullptr;
    if (buffers.size() > 0 && !buffers[0].empty() && null_count > 0) {
        validity_buf = &buffers[0];
    }

    // Data buffer is at index 1 for most types (validity is at 0 even if empty/nullptr)
    const vector<uint8_t> *data_buf = nullptr;
    if (buffers.size() > 1 && !buffers[1].empty()) {
        data_buf = &buffers[1];
    } else if (buffers.size() > 0 && !buffers[0].empty() && null_count == 0) {
        // Fallback: if no validity buffer was sent, data might be at index 0
        // This handles cases where server optimization skips the validity buffer entirely
        data_buf = &buffers[0];
    }

    if (!data_buf && type_str != "u" && type_str != "U" && type_str != "z" && type_str != "Z") {
        for (uint64_t i = 0; i < length; i++) {
            values.push_back(Value());
        }
        return values;
    }

    char type_char = type_str.empty() ? 'u' : type_str[0];

    for (uint64_t i = 0; i < length; i++) {
        // Check validity
        bool is_null = false;
        if (validity_buf && !validity_buf->empty()) {
            size_t byte_idx = i / 8;
            size_t bit_idx = i % 8;
            if (byte_idx < validity_buf->size()) {
                is_null = (((*validity_buf)[byte_idx] >> bit_idx) & 1) == 0;
            }
        }

        if (is_null) {
            values.push_back(Value());
            continue;
        }

        try {
            switch (type_char) {
            case 'l': {  // INT64
                if (data_buf && i * 8 + 8 <= data_buf->size()) {
                    int64_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 8], 8);
                    values.push_back(Value::BIGINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'L': {  // UINT64
                if (data_buf && i * 8 + 8 <= data_buf->size()) {
                    uint64_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 8], 8);
                    values.push_back(Value::UBIGINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'i': {  // INT32
                if (data_buf && i * 4 + 4 <= data_buf->size()) {
                    int32_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 4], 4);
                    values.push_back(Value::INTEGER(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'I': {  // UINT32
                if (data_buf && i * 4 + 4 <= data_buf->size()) {
                    uint32_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 4], 4);
                    values.push_back(Value::UINTEGER(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 's': {  // INT16
                if (data_buf && i * 2 + 2 <= data_buf->size()) {
                    int16_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 2], 2);
                    values.push_back(Value::SMALLINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'S': {  // UINT16
                if (data_buf && i * 2 + 2 <= data_buf->size()) {
                    uint16_t val = 0;
                    memcpy(&val, &(*data_buf)[i * 2], 2);
                    values.push_back(Value::USMALLINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'c': {  // INT8
                if (data_buf && i < data_buf->size()) {
                    int8_t val = static_cast<int8_t>((*data_buf)[i]);
                    values.push_back(Value::TINYINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'C': {  // UINT8
                if (data_buf && i < data_buf->size()) {
                    uint8_t val = (*data_buf)[i];
                    values.push_back(Value::UTINYINT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'g': {  // DOUBLE
                if (data_buf && i * 8 + 8 <= data_buf->size()) {
                    double val = 0;
                    memcpy(&val, &(*data_buf)[i * 8], 8);
                    values.push_back(Value::DOUBLE(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'f': {  // FLOAT
                if (data_buf && i * 4 + 4 <= data_buf->size()) {
                    float val = 0;
                    memcpy(&val, &(*data_buf)[i * 4], 4);
                    values.push_back(Value::FLOAT(val));
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'b': {  // BOOL
                if (data_buf) {
                    size_t byte_idx = i / 8;
                    size_t bit_idx = i % 8;
                    if (byte_idx < data_buf->size()) {
                        bool val = (((*data_buf)[byte_idx] >> bit_idx) & 1) != 0;
                        values.push_back(Value::BOOLEAN(val));
                    } else {
                        values.push_back(Value());
                    }
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'u':
            case 'U': {  // String (UTF-8)
                // Arrow string format: buffer[0]=validity, buffer[1]=offsets, buffer[2]=data
                // Even when null_count=0, the layout is the same (validity might be empty)
                if (buffers.size() >= 3 && !buffers[1].empty() && !buffers[2].empty()) {
                    const auto &offsets_buf = buffers[1];
                    const auto &str_data_buf = buffers[2];

                    if (i * 4 + 8 <= offsets_buf.size()) {
                        uint32_t start = 0, end = 0;
                        memcpy(&start, &offsets_buf[i * 4], 4);
                        memcpy(&end, &offsets_buf[(i + 1) * 4], 4);

                        if (end >= start && end <= str_data_buf.size()) {
                            string str(reinterpret_cast<const char*>(&str_data_buf[start]), end - start);
                            values.push_back(Value(str));
                        } else {
                            values.push_back(Value());
                        }
                    } else {
                        values.push_back(Value());
                    }
                } else if (buffers.size() >= 2 && !buffers[0].empty() && !buffers[1].empty()) {
                    // Fallback: offsets at 0, data at 1 (if server omits empty validity)
                    const auto &offsets_buf = buffers[0];
                    const auto &str_data_buf = buffers[1];

                    if (i * 4 + 8 <= offsets_buf.size()) {
                        uint32_t start = 0, end = 0;
                        memcpy(&start, &offsets_buf[i * 4], 4);
                        memcpy(&end, &offsets_buf[(i + 1) * 4], 4);

                        if (end >= start && end <= str_data_buf.size()) {
                            string str(reinterpret_cast<const char*>(&str_data_buf[start]), end - start);
                            values.push_back(Value(str));
                        } else {
                            values.push_back(Value());
                        }
                    } else {
                        values.push_back(Value());
                    }
                } else {
                    values.push_back(Value());
                }
                break;
            }
            case 'z':
            case 'Z': {  // Binary (BLOB)
                // Arrow binary format: buffer[0]=validity, buffer[1]=offsets, buffer[2]=data
                if (buffers.size() >= 3 && !buffers[1].empty() && !buffers[2].empty()) {
                    const auto &offsets_buf = buffers[1];
                    const auto &blob_data_buf = buffers[2];

                    if (i * 4 + 8 <= offsets_buf.size()) {
                        uint32_t start = 0, end = 0;
                        memcpy(&start, &offsets_buf[i * 4], 4);
                        memcpy(&end, &offsets_buf[(i + 1) * 4], 4);

                        if (end >= start && end <= blob_data_buf.size()) {
                            string blob_data(reinterpret_cast<const char*>(&blob_data_buf[start]), end - start);
                            values.push_back(Value::BLOB(blob_data));
                        } else {
                            values.push_back(Value());
                        }
                    } else {
                        values.push_back(Value());
                    }
                } else {
                    values.push_back(Value());
                }
                break;
            }
            default:
                values.push_back(Value());
                break;
            }
        } catch (...) {
            values.push_back(Value());
        }
    }

    return values;
}

void RemoteClient::ParseResultEnd(const vector<uint8_t> &payload, RemoteQueryResult &result) {
    if (payload.size() < 24) {
        return;
    }

    size_t offset = 0;

    // total_rows (8 bytes)
    result.total_rows = 0;
    for (int i = 0; i < 8; i++) {
        result.total_rows |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }

    // execution_time_us (8 bytes)
    result.execution_time_us = 0;
    for (int i = 0; i < 8; i++) {
        result.execution_time_us |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }

    // bytes_scanned (8 bytes)
    result.bytes_scanned = 0;
    for (int i = 0; i < 8; i++) {
        result.bytes_scanned |= static_cast<uint64_t>(payload[offset++]) << (i * 8);
    }
}

void RemoteClient::ParseError(const vector<uint8_t> &payload, RemoteQueryResult &result) {
    result.has_error = true;

    if (payload.size() < 9) {
        result.error_message = "Unknown error";
        return;
    }

    size_t offset = 0;

    // error_code (4 bytes) - skip
    offset += 4;

    // sql_state (5 bytes) - skip
    offset += 5;

    // message length (4 bytes)
    if (offset + 4 > payload.size()) {
        result.error_message = "Unknown error";
        return;
    }

    uint32_t msg_len = 0;
    for (int i = 0; i < 4; i++) {
        msg_len |= static_cast<uint32_t>(payload[offset++]) << (i * 8);
    }

    if (offset + msg_len <= payload.size()) {
        result.error_message = string(reinterpret_cast<const char*>(&payload[offset]), msg_len);
    } else {
        result.error_message = "Unknown error";
    }
}

//===----------------------------------------------------------------------===//
// Type Conversion
//===----------------------------------------------------------------------===//

LogicalType RemoteClient::ParseArrowTypeFormat(const string &format) {
    if (format.empty()) {
        return LogicalType::VARCHAR;
    }

    char type_char = format[0];
    switch (type_char) {
    case 'b':
        return LogicalType::BOOLEAN;
    case 'c':
        return LogicalType::TINYINT;
    case 'C':
        return LogicalType::UTINYINT;
    case 's':
        return LogicalType::SMALLINT;
    case 'S':
        return LogicalType::USMALLINT;
    case 'i':
        return LogicalType::INTEGER;
    case 'I':
        return LogicalType::UINTEGER;
    case 'l':
        return LogicalType::BIGINT;
    case 'L':
        return LogicalType::UBIGINT;
    case 'e':
        return LogicalType::FLOAT;  // half float -> float
    case 'f':
        return LogicalType::FLOAT;
    case 'g':
        return LogicalType::DOUBLE;
    case 'u':
    case 'U':
        return LogicalType::VARCHAR;
    case 'z':
    case 'Z':
        return LogicalType::BLOB;
    case 't':  // time
        if (format.size() > 1) {
            switch (format[1]) {
            case 'd':  // date32
            case 'D':  // date64
                return LogicalType::DATE;
            case 't':  // time32
            case 'T':  // time64
                return LogicalType::TIME;
            case 's':  // timestamp
                return LogicalType::TIMESTAMP;
            }
        }
        return LogicalType::VARCHAR;
    default:
        return LogicalType::VARCHAR;
    }
}

} // namespace duckdb
